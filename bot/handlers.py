import os
import re
import asyncio
import logging
import time
from aiogram import types, F, Router
from aiogram.filters import Command, CommandObject
from aiogram.types import FSInputFile, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder

import messages
from config import bot, r, SUPER_ADMIN_IDS
from database import (
    register_user, get_user, get_user_by_username, set_user_password,
    check_auth, validate_password, update_user_phone, set_role,
    get_all_by_role, get_all_users, get_users_by_admin, is_super_admin, get_db_stats, ban_user, delete_user
)
from utils import generate_html_report
from search_service import service as search_service
from notifier import run_manual_check

router = Router()
logger = logging.getLogger("bot.handlers")

# --- Помічники ---
async def notify_super_admins(text: str):
    for admin_id in SUPER_ADMIN_IDS:
        try:
            await bot.send_message(admin_id, f"🔔 <b>СИСТЕМНЕ ПОВІДОМЛЕННЯ</b>\n\n{text}", parse_mode="HTML")
        except Exception as e:
            logger.error(f"Failed to notify super admin {admin_id}: {e}")

def get_main_keyboard(user_id):
    builder = ReplyKeyboardBuilder()
    builder.row(types.KeyboardButton(text="🔍 Пошук"))
    builder.row(types.KeyboardButton(text="🌐 Режим пошуку"))
    
    # Кнопка для адмінів та супер-адмінів
    user = get_user(user_id)
    if is_super_admin(user_id) or (user and user.get('role') == 'admin'):
        builder.row(types.KeyboardButton(text="⚙️ Адмін-панель"))
        
    return builder.as_markup(resize_keyboard=True)

@router.message(F.text == "⚙️ Адмін-панель")
async def cmd_admin_panel(message: types.Message):
    await show_admin_panel(message, uid=message.from_user.id, edit=False)

async def show_admin_panel(message: types.Message, uid: int, edit: bool = False):
    user = get_user(uid)
    if not is_super_admin(uid) and (not user or user['role'] != 'admin'):
        return

    builder = InlineKeyboardBuilder()
    if is_super_admin(uid):
        builder.row(InlineKeyboardButton(text="👥 Усі користувачі", callback_data="admin_users_all"))
        builder.row(InlineKeyboardButton(text="📂 Мої учасники", callback_data="admin_users_my"))
        builder.row(InlineKeyboardButton(text="👮 Список адмінів", callback_data="admin_admins"))
    else:
        builder.row(InlineKeyboardButton(text="👥 Мої користувачі", callback_data="admin_users_my"))
        
    builder.row(InlineKeyboardButton(text="📊 Статистика бази", callback_data="admin_stats"))
    builder.row(InlineKeyboardButton(text="📝 Як реєструвати?", callback_data="admin_help"))
    
    text = "🛠 <b>Панель керування SKYWORK</b>\n\nВиберіть потрібний розділ:"
    if edit:
        await message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="HTML")
    else:
        await message.answer(text, reply_markup=builder.as_markup(), parse_mode="HTML")

# --- Обробка авторизації та входу ---

@router.message(Command("start"))
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    user = get_user(user_id)
    
    # Реєструємо, якщо новий
    if not user:
        register_user(user_id, message.from_user.username, message.from_user.first_name)
    
    if check_auth(user_id):
        await message.answer(messages.START_MESSAGE, reply_markup=get_main_keyboard(user_id))
    else:
        # ПОВНЕ ІГНОРУВАННЯ для неавторизованих (згідно з ТЗ)
        return

@router.message(F.contact)
async def handle_contact(message: types.Message):
    user_id = message.from_user.id
    if not await r.get(f"user_temp_auth:{user_id}"): return
    
    phone = message.contact.phone_number
    update_user_phone(user_id, phone)
    await r.delete(f"user_temp_auth:{user_id}")
    
    await message.answer("✅ <b>Верифікація успішна!</b>", parse_mode="HTML", reply_markup=get_main_keyboard(user_id))
    await notify_super_admins(f"👤 <b>Новий вхід!</b>\nЮзер: @{message.from_user.username}\nТелефон: {phone}")

# --- Коллбеки Адмін-панелі ---
@router.callback_query(F.data == "admin_users_all")
async def cb_admin_users_all(callback: types.CallbackQuery):
    await cmd_users(callback.message, from_user_id=callback.from_user.id, mode="all", edit=True)
    await callback.answer()

@router.callback_query(F.data == "admin_users_my")
async def cb_admin_users_my(callback: types.CallbackQuery):
    await cmd_users(callback.message, from_user_id=callback.from_user.id, mode="my", edit=True)
    await callback.answer()

@router.callback_query(F.data == "admin_admins")
async def cb_admin_admins(callback: types.CallbackQuery):
    await cmd_admins(callback.message, from_user_id=callback.from_user.id, edit=True)
    await callback.answer()

# --- Керування користувачами та адмінами через кнопки ---

@router.callback_query(F.data.startswith("manage_user:"))
async def cb_manage_user(callback: types.CallbackQuery):
    parts = callback.data.split(":")
    target_id = int(parts[1])
    mode = parts[2]
    
    target = get_user(target_id)
    if not target:
        await callback.answer("Користувача не знайдено.")
        return
        
    text = (
        f"👤 <b>Керування користувачем</b>\n\n"
        f"<b>Ім'я:</b> {target['first_name'] or 'NoName'}\n"
        f"<b>Нік:</b> @{target['username'] or '—'}\n"
        f"<b>ID:</b> <code>{target['user_id']}</code>\n"
        f"<b>Телефон:</b> {target['phone'] or '—'}\n"
        f"<b>Статус:</b> {'✅ Авторизований' if target['is_authorized'] else '🔑 Очікує'}\n"
    )
    
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="🗑 Видалити користувача", callback_data=f"del_confirm:{target_id}:{mode}"))
    builder.row(InlineKeyboardButton(text="🔙 Назад до списку", callback_data=f"admin_users_{mode}"))
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="HTML")

@router.callback_query(F.data.startswith("del_confirm:"))
async def cb_del_confirm(callback: types.CallbackQuery):
    parts = callback.data.split(":")
    target_id = int(parts[1])
    mode = parts[2]
    
    if delete_user(target_id):
        await callback.answer("✅ Користувача видалено", show_alert=True)
        # Повертаємось до списку
        await cmd_users(callback.message, from_user_id=callback.from_user.id, mode=mode, edit=True)
    else:
        await callback.answer("❌ Помилка видалення", show_alert=True)

@router.callback_query(F.data.startswith("manage_admin:"))
async def cb_manage_admin(callback: types.CallbackQuery):
    target_id = int(callback.data.split(":")[1])
    target = get_user(target_id)
    
    if not target:
        await callback.answer("Адміна не знайдено.")
        return
        
    text = (
        f"👮 <b>Керування адміністратором</b>\n\n"
        f"<b>Нік:</b> @{target['username'] or '—'}\n"
        f"<b>ID:</b> <code>{target['user_id']}</code>\n"
        f"<b>Пароль:</b> <code>{target['raw_password'] or '—'}</code>\n"
    )
    
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="🚫 Зняти права адміна", callback_data=f"demote_confirm:{target_id}"))
    builder.row(InlineKeyboardButton(text="🔙 Назад до списку", callback_data="admin_admins"))
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="HTML")

@router.callback_query(F.data.startswith("demote_confirm:"))
async def cb_demote_confirm(callback: types.CallbackQuery):
    target_id = int(callback.data.split(":")[1])
    
    if set_role(target_id, 'user'):
        await callback.answer("✅ Права адміна знято", show_alert=True)
        await cmd_admins(callback.message, from_user_id=callback.from_user.id, edit=True)
    else:
        await callback.answer("❌ Помилка", show_alert=True)

@router.callback_query(F.data == "admin_stats")
async def cb_admin_stats(callback: types.CallbackQuery):
    # Виклик існуючої команди статистики
    await cmd_stats(callback.message, from_user_id=callback.from_user.id)
    await callback.answer()

@router.callback_query(F.data == "admin_help")
async def cb_admin_help(callback: types.CallbackQuery):
    text = (
        "📝 <b>Як реєструвати користувача:</b>\n\n"
        "1. Користувач повинен написати боту <code>/start</code>\n"
        "2. Ви пишете команду: <code>/reg @username пароль</code>\n"
        "3. Передаєте пароль користувачу.\n"
        "4. При вході він введе пароль і надішле свій номер телефону.\n\n"
        "<i>Пароль жорстко прив'язується до його ID.</i>"
    )
    await callback.message.answer(text, parse_mode="HTML")
    await callback.answer()

@router.message(Command("check"))
async def cmd_check(message: types.Message):
    if not is_super_admin(message.from_user.id): return
    
    sent_msg = await message.answer("🔄 <b>Перевіряю базу даних...</b>", parse_mode="HTML")
    report, has_changes = await run_manual_check(message.from_user.id)
    
    # Видаляємо статус-повідомлення
    try:
        await sent_msg.delete()
    except:
        pass
        
    if has_changes:
        # Надсилаємо звіт усім супер-адмінам, якщо є зміни
        from notifier import notify_super_admins as broadcast
        await broadcast(report)
    else:
        # Якщо змін немає - відповідаємо тільки тому, хто запитав
        await message.answer(report, parse_mode="HTML")

# --- Команди Адміністраторів ---

@router.message(Command("reg"))
async def cmd_reg(message: types.Message, command: CommandObject):
    user = get_user(message.from_user.id)
    if not is_super_admin(message.from_user.id) and (not user or user['role'] != 'admin'):
        return

    if not command.args or len(command.args.split()) < 2:
        await message.answer("❌ Використання: `/reg @nick password`", parse_mode="Markdown")
        return

    parts = command.args.split()
    username = parts[0]
    password = parts[1]

    success, msg = set_user_password(username, password, message.from_user.id)
    await message.answer(f"{'✅' if success else '❌'} {msg}")

@router.message(Command("users"))
async def cmd_users(message: types.Message, from_user_id: int = None, mode: str = "my", edit: bool = False):
    uid = from_user_id or message.from_user.id
    user = get_user(uid)
    if not is_super_admin(uid) and (not user or user['role'] != 'admin'):
        return
    
    if is_super_admin(uid) and mode == "all":
        users = get_all_users()
        title = "Усі користувачі системи"
    else:
        users = get_users_by_admin(uid)
        title = "Ваші користувачі"
        
    if not users:
        text = f"❌ {title} порожній."
        builder = InlineKeyboardBuilder()
        builder.row(InlineKeyboardButton(text="🔙 В адмін-панель", callback_data="back_to_admin"))
        if edit: await message.edit_text(text, reply_markup=builder.as_markup())
        else: await message.answer(text, reply_markup=builder.as_markup())
        return
    
    builder = InlineKeyboardBuilder()
    for u in users:
        # u: (user_id, username, phone, is_authorized, role, first_name)
        status = "✅" if u[3] else "🔑"
        name = u[5] or u[1] or str(u[0])
        builder.row(InlineKeyboardButton(text=f"{status} {name}", callback_data=f"manage_user:{u[0]}:{mode}"))
    
    builder.row(InlineKeyboardButton(text="🔙 В адмін-панель", callback_data="back_to_admin"))
    
    text = f"👥 <b>{title}</b>\nНатисніть на користувача для керування:"
    
    if edit:
        await message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="HTML")
    else:
        await message.answer(text, reply_markup=builder.as_markup(), parse_mode="HTML")

@router.callback_query(F.data == "back_to_admin")
async def cb_back_to_admin(callback: types.CallbackQuery):
    await show_admin_panel(callback.message, uid=callback.from_user.id, edit=True)
    await callback.answer()

@router.message(Command("stats"))
async def cmd_stats(message: types.Message, from_user_id: int = None, edit: bool = False):
    uid = from_user_id or message.from_user.id
    user = get_user(uid)
    if not is_super_admin(uid) and (not user or user['role'] != 'admin'):
        return
    
    stats = get_db_stats()
    if not stats:
        text = "❌ Помилка отримання статистики."
        if edit: await message.edit_text(text)
        else: await message.answer(text)
        return
        
    text = (
        "📊 <b>Статистика системи:</b>\n\n"
        f"👥 Всього користувачів: <code>{stats['total_users']}</code>\n"
        f"✅ Авторизовано: <code>{stats['authorized_users']}</code>\n"
        f"👮 Адміністраторів: <code>{stats['admins']}</code>\n"
        f"🚫 Забанено: <code>{stats['banned_users']}</code>\n"
    )
    
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="🔙 В адмін-панель", callback_data="back_to_admin"))
    
    if edit:
        await message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="HTML")
    else:
        await message.answer(text, reply_markup=builder.as_markup(), parse_mode="HTML")

@router.message(Command("admins"))
async def cmd_admins(message: types.Message, from_user_id: int = None, edit: bool = False):
    uid = from_user_id or message.from_user.id
    if not is_super_admin(uid): return
    
    admins = get_all_by_role('admin')
    if not admins:
        text = "❌ Адмінів не знайдено."
        builder = InlineKeyboardBuilder()
        builder.row(InlineKeyboardButton(text="🔙 В адмін-панель", callback_data="back_to_admin"))
        if edit: await message.edit_text(text, reply_markup=builder.as_markup())
        else: await message.answer(text, reply_markup=builder.as_markup())
        return
        
    builder = InlineKeyboardBuilder()
    for a in admins:
        # a: (user_id, username, raw_password, is_banned)
        status = "🔴" if a[3] else "🟢"
        name = a[1] or str(a[0])
        builder.row(InlineKeyboardButton(text=f"{status} @{name}", callback_data=f"manage_admin:{a[0]}"))
    
    builder.row(InlineKeyboardButton(text="🔙 В адмін-панель", callback_data="back_to_admin"))
    
    text = "👮 <b>Список адміністраторів</b>\nНатисніть для керування:"
    
    if edit:
        await message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="HTML")
    else:
        await message.answer(text, reply_markup=builder.as_markup(), parse_mode="HTML")

@router.message(Command("add_admin", "promote"))
async def cmd_add_admin(message: types.Message, command: CommandObject):
    if not is_super_admin(message.from_user.id): return
    if not command.args:
        await message.answer("❌ Використання: `/promote @username`")
        return
    
    user = get_user_by_username(command.args)
    if not user:
        await message.answer("❌ Користувач не знайдений в базі.")
        return
        
    if set_role(user['user_id'], 'admin'):
        await message.answer(f"✅ @{user['username']} призначений Адміном.\n\n💡 Користувачу потрібно прописати /start, щоб оновити меню.")
    else:
        await message.answer("❌ Помилка при зміні ролі в базі даних.")

@router.message(Command("demote"))
async def cmd_demote(message: types.Message, command: CommandObject):
    if not is_super_admin(message.from_user.id): return
    if not command.args:
        await message.answer("❌ Використання: `/demote @username`")
        return
    
    user = get_user_by_username(command.args)
    if not user:
        await message.answer("❌ Користувач не знайдений.")
        return
        
    set_role(user['user_id'], 'user')
    await message.answer(f"✅ @{user['username']} розжалуваний до звичайного Користувача.")

@router.message(Command("ban", "unban"))
async def cmd_ban_unban(message: types.Message, command: CommandObject):
    if not is_super_admin(message.from_user.id): return
    if not command.args: return
    
    is_ban = message.text.startswith("/ban")
    user = get_user_by_username(command.args)
    if not user:
        await message.answer("Користувач не знайдений.")
        return
        
    ban_user(user['user_id'], status=is_ban)
    await message.answer(f"✅ Користувач @{user['username']} {'забанений' if is_ban else 'розбанений'}.")

@router.message(Command("del"))
async def cmd_del(message: types.Message, command: CommandObject):
    admin_id = message.from_user.id
    user_info = get_user(admin_id)
    if not is_super_admin(admin_id) and (not user_info or user_info['role'] != 'admin'):
        return
        
    if not command.args:
        await message.answer("❌ Використання: `/del @username` або `/del ID`")
        return
        
    # Спроба знайти за нікнеймом
    target_user = get_user_by_username(command.args)
    if not target_user and command.args.isdigit():
        # Спроба знайти за ID
        target_user = get_user(int(command.args))
        
    if not target_user:
        await message.answer("❌ Користувача не знайдено.")
        return
        
    # Перевірка прав на видалення
    if not is_super_admin(admin_id) and target_user['created_by'] != admin_id:
        await message.answer("❌ Ви можете видаляти лише тих користувачів, яких зареєстрували самі.")
        return
        
    if delete_user(target_user['user_id']):
        await message.answer(f"✅ Користувача {command.args} видалено.")
    else:
        await message.answer("❌ Помилка при видаленні.")

# --- Основна логіка пошуку та авторизації ---

@router.message(F.text)
async def handle_all_text(message: types.Message):
    user_id = message.from_user.id
    text = message.text.strip()

    # 1. Спроба входу (якщо не авторизований)
    if not check_auth(user_id):
        success, status = validate_password(user_id, text)
        
        if success:
            # Тимчасово позначаємо в Redis що він ввів пароль і чекаємо телефон (на 10 хв)
            await r.set(f"user_temp_auth:{user_id}", "true", ex=600)
            
            # Запитуємо телефон для завершення реєстрації
            builder = ReplyKeyboardBuilder()
            builder.row(types.KeyboardButton(text="📱 Надіслати номер телефону", request_contact=True))
            
            await message.answer(
                "🔓 <b>Пароль вірний!</b>\nДля завершення входу, будь ласка, натисніть кнопку нижче, щоб поділитися контактом.",
                reply_markup=builder.as_markup(resize_keyboard=True),
                parse_mode="HTML"
            )
            return
        elif status.startswith("theft:"):
            parts = status.split(":")
            target_nick = parts[1]
            admin_id = parts[2]
            
            admin_info = "Система"
            if admin_id != 'system':
                admin = get_user(int(admin_id))
                admin_info = f"@{admin['username']}" if admin else f"ID:{admin_id}"
                
            await notify_super_admins(
                f"🚨 <b>НЕВІДОМИЙ ВХІД!</b>\n\n"
                f"👤 <b>Нова людина:</b> @{message.from_user.username} (ID: {user_id})\n"
                f"🎯 <b>Власник паролю:</b> @{target_nick}\n"
                f"👮 <b>Адмін:</b> {admin_info}"
            )
            await message.answer("❌ <b>Помилка доступу!</b>\nЦей пароль не належить вашому акаунту.", parse_mode="HTML")
            return
        else:
            logger.info(f"Wrong password attempt by {user_id}: {text}")
            # Сповіщення про будь-який невірний пароль (адміну)
            await notify_super_admins(
                f"⚠️ <b>НЕВІРНИЙ ПАРОЛЬ!</b>\n\n"
                f"👤 <b>Користувач:</b> @{message.from_user.username} (ID: {user_id})\n"
                f"🔑 <b>Ввів:</b> <code>{text}</code>"
            )
            # Повністю ігноруємо користувача (не відправляємо "Невірний пароль")
            return

    # 2. Якщо вже авторизований - обробляємо команди або пошук
    if text == "🔍 Пошук":
        await message.answer("Просто введіть текст для пошуку (ПІБ, телефон, email тощо) після  введіть ':' та фільтр для запиту як що треба.")
        return
    
    if text == "🌐 Режим пошуку":
        current_mode = await r.get(f"user_mode:{user_id}") or "ua"
        new_mode = "ru" if current_mode == "ua" else "ua"
        await r.set(f"user_mode:{user_id}", new_mode)
        await message.answer(f"✅ <b>Режим змінено на:</b> {'🌍 (RU)' if new_mode == 'ru' else ' (UA)'}", parse_mode="HTML")
        return

    # Якщо це не команда, то це ПОШУК
    if text.startswith("/"): return

    # Логіка пошуку
    current_mode = await r.get(f"user_mode:{user_id}") or "ua"
    table_name = "global_search_ua" if current_mode == "ua" else "global_search_ru"
    
    # ПЕРЕВІРКА НА ФІЛЬТР (формат Запит : Фільтр)
    if ":" in text:
        parts = text.split(":", 1)
        main_query = parts[0].strip()
        filter_val = parts[1].strip()
        
        if not main_query or not filter_val:
            await message.answer("❌ <b>Помилка!</b>\nВведіть і запит, і фільтр через двокрапку.", parse_mode="HTML")
            return
            
        msg = await message.answer(f"🔍 <b>Запит:</b> <code>{main_query}</code>\n🎯 <b>Фільтр:</b> <code>{filter_val}</code>\n⏳ <i>Обробка...</i>", parse_mode="HTML")
        
        try:
            results_matrix = await search_across_tables(main_query, table=table_name)
            filter_lower = filter_val.lower()
            
            filtered_matrix = []
            count = 0
            for table_data in results_matrix:
                if not table_data: continue
                source = table_data[0]
                rows = table_data[1:]
                matching_rows = []
                for row in rows:
                    row_str = " ".join(str(v) for v in row.values()).lower()
                    if filter_lower in row_str:
                        matching_rows.append(row)
                        count += 1
                if matching_rows:
                    filtered_matrix.append([source] + matching_rows)
            
            if count == 0:
                await msg.edit_text(f"❌ <b>Нічого не знайдено.</b>\nЗа запитом <code>{main_query}</code> з фільтром <code>{filter_val}</code> результатів немає.", parse_mode="HTML")
                return

            now = time.strftime("%Y-%m-%d %H:%M:%S")
            analyzed_html = generate_html_report(f"{main_query} + {filter_val}", filtered_matrix, now, analyzed=True)
            
            filename = f"report_{user_id}_{int(time.time())}.html"
            with open(filename, "w", encoding="utf-8") as f: f.write(analyzed_html)
            
            await msg.delete()
            await bot.send_document(
                message.chat.id, FSInputFile(filename), 
                caption=f"✅ <b>Звіт готовий!</b>\n🔍 Запит: <code>{main_query}</code>\n🎯 Фільтр: <code>{filter_val}</code>\n📊 Знайдено: <b>{count}</b>", 
                parse_mode="HTML"
            )
            if os.path.exists(filename): os.remove(filename)
            return
        except Exception as e:
            logger.error(f"Filtered search error: {e}")
            await message.answer("❌ Помилка пошуку.")
            return

    # ЗВИЧАЙНИЙ ПОШУК
    msg = await message.answer(f"🔍 Шукаю: <code>{text}</code>...", parse_mode="HTML")
    
    results_matrix = await search_across_tables(text, table=table_name)
    local_results_count = sum(len(table_data) - 1 for table_data in results_matrix if len(table_data) > 1)
    
    if local_results_count > 0:
        import json
        # Зберігаємо результати (про всяк випадок)
        await r.set(f"last_results:{user_id}", json.dumps(results_matrix, default=str), ex=3600)
        
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        analyzed_html = generate_html_report(text, results_matrix, now, analyzed=True)
        report_id = f"{user_id}_{int(time.time())}"
        filename = f"report_{report_id}.html"
        with open(filename, "w", encoding="utf-8") as f: f.write(analyzed_html)
        
        await msg.edit_text(f"✅ Знайдено: <b>{local_results_count}</b>", parse_mode="HTML")
        try:
            await bot.send_document(message.chat.id, FSInputFile(filename), caption=f"📊 <b>Звіт: {text}</b>", parse_mode="HTML")
        except Exception as e:
            logger.error(f"Failed to send report to {message.chat.id}: {e}")
        
        if os.path.exists(filename): os.remove(filename)
    else:
        await msg.edit_text(messages.SEARCH_NOT_FOUND, parse_mode="HTML")


async def search_across_tables(query, table):
    from search_service import search_across_tables as sat
    return await sat(query, table=table)
