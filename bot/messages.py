# -*- coding: utf-8 -*-

START_MESSAGE = (
    "<b>🔹 Skywork — система пошуку</b>\n\n"
    "🔍 <b>Як користуватися?</b>\n"
    "Просто надішліть будь-який з нижченаведених типів даних:\n\n"
    "👤 <b>ПІБ</b> — <code>Іванов Іван Іванович</code>\n"
    "📞 <b>Телефон</b> — <code>+380XXXXXXXXX</code> або <code>79XXXXXXXXX</code>\n"
    "📬 <b>Email</b> — <code>example@gmail.com</code>\n"
    "🏦 <b>ІНН/РНОКПП</b> — 10 або 12 цифр\n"
    "📄 <b>СНІЛС</b> — формат <code>XXX-XXX-XXX XX</code>\n"
    "🇺🇦 <b>Паспорт</b> — <code>1234 567890</code>\n"
    "👤 <b>Нік</b> — <code>@username</code>\n\n"
    "🌐 <b>Режим пошуку</b>\n"
    "Кнопка <b>🌐 Режим пошуку</b> перемикає між UA (Україна) і RU (Росія) базами.\n\n"
    "🎯 <b>Фільтрація результатів</b>\n"
    "Додайте <code>:</code> і слово для фільтрації:\n"
    "<code>Іванов Іван : Київ</code>\n\n"
    "✅ Бот <b>автоматично</b> визначає тип запиту і повідомляє вас, за яким полем відбувається пошук."
)



SEARCH_START = "🔍 Починаю пошук за запитом: `{query}`..."
SEARCH_SUCCESS = "✅ Завдання прийнято!\n\n📊 Знайдено {count} записів.\n📄 Генерую звіт..."
SEARCH_NOT_FOUND = "✅ Завдання прийнято!\n\n❌ Нічого не знайдено."

CABINET_MESSAGE = (
    "👤 **Особистий кабінет**\n\n"
    "🆔 **Ваш ID:** `{user_id}`\n"
    "📊 **Статус підписки:** {status}\n"
    "⚙️ **Режим пошуку:** {mode}\n"
    "⏳ **Діє до:** `{expiry}`\n\n"
    "📅 **Дата реєстрації:** `{reg_date}`"
)

PAY_OPTIONS_MESSAGE = "💳 **Оберіть термін підписки:**\n\nВартість: $10 за 1 місяць."

REPORT_CAPTION = "📄 Відсканований звіт: `{query}`"

ADMIN_ONLY_FILES = ""
ADMIN_SQL_WAIT = "⏳ Обробка SQL..."
ADMIN_SQL_SUCCESS = "✅ SQL виконано успішно!\n\nРезультат: `{result}`"
ADMIN_SQL_ERROR = "❌ Помилка при виконанні SQL:\n\n`{error}`"
ADMIN_SQL_FORBIDDEN = "⚠️ Команда відхилена! Використання `DROP` суворо заборонено."
ADMIN_FILE_TYPE_ERROR = "❌ Будь ласка, надішліть файл з розширенням `.sql`"

# HTML_REPORT_TEMPLATE moved to report_template.html
