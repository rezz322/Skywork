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

REPORT_CAPTION = "📄 Відсканований звіт: `{query}`"

ADMIN_ONLY_FILES = ""
ADMIN_SQL_WAIT = "⏳ Обробка SQL..."
ADMIN_SQL_SUCCESS = "✅ SQL виконано успішно!\n\nРезультат: `{result}`"
ADMIN_SQL_ERROR = "❌ Помилка при виконанні SQL:\n\n`{error}`"
ADMIN_SQL_FORBIDDEN = "⚠️ Команда відхилена! Використання `DROP` суворо заборонено."
ADMIN_FILE_TYPE_ERROR = "❌ Будь ласка, надішліть файл з розширенням `.sql`"

HTML_REPORT_TEMPLATE = """
<!DOCTYPE html>
<html lang="uk">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>skywork Звіт: {{ query }}</title>
    <style>
        @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;700&family=Inter:wght@400;600;800&display=swap');
        :root {
            --bg-body: #0a0a0a; --bg-card: #141414;
            --accent-color: #00e5ff; --accent-dim: rgba(0, 229, 255, 0.1);
            --text-primary: #ffffff; --text-secondary: #a0a0a0;
            --border-color: #222; --success-color: #00ff9d;
            --font-mono: 'JetBrains Mono', monospace; --font-sans: 'Inter', sans-serif;
        }
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { background-color: var(--bg-body); color: var(--text-primary); font-family: var(--font-sans); font-size: 13px; line-height: 1.4; padding: 30px; }
        .header { border-bottom: 2px solid var(--accent-color); padding-bottom: 15px; margin-bottom: 30px; display: flex; justify-content: space-between; align-items: flex-end; }
        .brand { font-family: var(--font-mono); font-size: 24px; font-weight: 800; color: var(--accent-color); letter-spacing: 2px; }
        .info-value { color: var(--accent-color); font-family: var(--font-mono); font-size: 13px; }
        .summary-card { background: rgba(255,255,255,0.02); border: 1px solid var(--border-color); border-radius: 6px; padding: 15px; margin-bottom: 30px; }
        .summary-data { font-size: 18px; font-weight: 700; color: var(--accent-color); }
        .source-title { font-family: var(--font-mono); font-size: 16px; margin: 30px 0 15px; padding-left: 10px; border-left: 3px solid var(--accent-color); color: var(--text-primary); }
        
        .results-grid { 
            display: grid; 
            grid-template-columns: repeat(2, 1fr); 
            gap: 15px; 
        }
        
        .result-card { background-color: var(--bg-card); border: 1px solid var(--border-color); border-radius: 6px; overflow: hidden; }
        .card-header { background: rgba(255, 255, 255, 0.03); padding: 8px 15px; font-family: var(--font-mono); font-size: 10px; color: #555; border-bottom: 1px solid var(--border-color); }
        .card-body { padding: 12px 15px; }
        .data-row { display: flex; padding: 4px 0; border-bottom: 1px solid rgba(255, 255, 255, 0.02); }
        .data-label { flex: 0 0 35%; color: var(--text-secondary); font-size: 11px; }
        .data-value { flex: 1; font-family: var(--font-mono); color: #eee; font-size: 12px; word-break: break-all; }

        @media (max-width: 800px) { .results-grid { grid-template-columns: 1fr; } }
    </style>
</head>
<body>
    <header class="header">
        <div class="brand">skywork</div>
        <div class="info-value">{{ current_time }}</div>
    </header>

    <div class="summary-card">
        <div class="summary-data">{{ query }}</div>
        <div style="margin-top:8px; font-size: 11px; color: var(--text-secondary);">
            ЗНАЙДЕНО: <span style="color: var(--success-color)">{{ total_records }}</span>
            {% if total_records >= 1000 %}
                <span style="color: #ff9800; margin-left: 15px;">[!] LIMIT 1,000 REACHED</span>
            {% endif %}
        </div>
    </div>

    {% for table_data in results %}
        {% if table_data|length > 1 %}
            <section class="source-section">
                <h2 class="source-title">{{ table_data[0] | upper }}</h2>
                <div class="results-grid">
                    {% for row in table_data[1:] %}
                        <div class="result-card">
                            <div class="card-header">#{{ loop.index }}</div>
                            <div class="card-body">
                                {% for key, value in row.items() %}
                                    <div class="data-row">
                                        <div class="data-label">{{ key }}</div>
                                        <div class="data-value">{{ value }}</div>
                                    </div>
                                {% endfor %}
                            </div>
                        </div>
                    {% endfor %}
                </div>
            </section>
        {% endif %}
    {% endfor %}
</body>
</html>
"""
