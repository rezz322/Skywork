# -*- coding: utf-8 -*-

START_MESSAGE = (
    "Шукаю за наступними даними :\n"
    "• ПІБ, дата народження або адреса\n"
    "• Номер телефону або Ел. пошта\n"
    "• Нікнейм або Telegram ID\n"
    "• ІПН, СНІЛС або паспорт\n"
    "• Транспорт (номер або VIN)"
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
