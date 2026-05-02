import re
from jinja2 import Template
import messages

def normalize_fio(s):
    if not s: return ""
    s = str(s).lower().strip()
    replacements = {'і': 'и', 'ї': 'и', 'є': 'е', 'ґ': 'г'}
    for k, v in replacements.items():
        s = s.replace(k, v)
    return s

def clean_id(s):
    if not s: return ""
    return re.sub(r'[^\d]', '', str(s))

def is_garbage_date(s):
    if not s: return True
    # Если в дате есть буквы - это мусор (адрес или имя)
    if any(c.isalpha() for c in str(s)):
        return True
    # Если дата слишком короткая или слишком длинная для реальной даты
    s_clean = clean_id(s)
    if len(s_clean) < 4: return True
    return False

def generate_html_report(query, results, current_time_str, analyzed=True):
    import os
    try:
        template_path = os.path.join(os.path.dirname(__file__), 'report_template.html')
        with open(template_path, 'r', encoding='utf-8') as f:
            template_content = f.read()
        template = Template(template_content)
    except Exception:
        return "<html><body>Error loading template</body></html>"

    if analyzed:
        persons = get_merged_persons(results)
        total_records = sum(p['count'] for p in persons)
        return template.render(query=query, persons=persons, total_records=total_records, current_time=current_time_str, is_analyzed=True)
    else:
        # Просто плоский список всех записей из всех таблиц
        all_rows = []
        for table_data in results:
            if len(table_data) > 1:
                for row in table_data[1:]:
                    all_rows.append(dict(row))
        
        return template.render(query=query, results=all_rows, total_records=len(all_rows), current_time=current_time_str, is_analyzed=False)

def get_merged_persons(results_matrix):
    persons = []
    
    for table_data in results_matrix:
        if len(table_data) <= 1: continue
        
        source_name = str(table_data[0]).strip().lower()
        ignore_fields = ["birth_date", "address"] if "відомості_про_фізичних_осіб" in source_name else []
        
        for row in table_data[1:]:
            current_row = dict(row)
            for f in ignore_fields:
                current_row[f] = ""

            fio_raw = str(current_row.get('fio', '')).strip()
            fio_norm = normalize_fio(fio_raw)
            inn = clean_id(current_row.get('inn', ''))
            phone = clean_id(current_row.get('phone', '') or current_row.get('mobile', ''))
            
            bday_raw = str(current_row.get('birth_date', '')).strip()
            # Проверка на мусор в дате рождения
            bday = bday_raw if not is_garbage_date(bday_raw) else ""
            
            if not fio_norm and not inn and not phone: continue
                
            found = False
            for p in persons:
                # Строгая проверка по ИНН: если оба ИНН есть и они разные - это РАЗНЫЕ люди
                if inn and p['inn'] and inn != p['inn']:
                    continue
                
                match_by_inn = (inn and p['inn'] == inn)
                match_by_phone = (phone and p['phone'] == phone)
                match_by_fio = (fio_norm and p['fio_norm'] == fio_norm)
                
                if match_by_inn:
                    found = True
                elif match_by_phone:
                    # Если ИНН нет у одного из них, можем объединить по телефону
                    found = True
                elif match_by_fio:
                    # Если ИНН нет у одного из них, можем объединить по ФИО + ДР
                    if not bday or not p['birth_date'] or bday == p['birth_date']:
                        found = True
                
                if found:
                    p['count'] += 1
                    for k, v in current_row.items():
                        new_val = str(v).strip()
                        if not new_val or new_val.lower() in ["", "none", "null", "0"]: continue
                        
                        # Фильтр телефонов: минимум 10 цифр
                        if k.lower() in ["phone", "mobile", "telephone", "номер"]:
                            new_val = clean_id(new_val)
                            if len(new_val) < 10: continue
                        
                        # Фильтр дат в дополнительных полях
                        if k.lower() == "birth_date" and is_garbage_date(new_val): continue

                        if k in p['merged_data']:
                            if isinstance(p['merged_data'][k], list):
                                if new_val not in p['merged_data'][k] and len(p['merged_data'][k]) < 10:
                                    p['merged_data'][k].append(new_val)
                            else:
                                if new_val != p['merged_data'][k]:
                                    p['merged_data'][k] = [p['merged_data'][k], new_val]
                        else:
                            p['merged_data'][k] = [new_val]
                    
                    if not p['inn'] and inn: p['inn'] = inn
                    if not p['phone'] and phone: p['phone'] = phone
                    if not p['birth_date'] and bday: p['birth_date'] = bday
                    break
            
            if not found:
                merged_init = {}
                for k, v in current_row.items():
                    val = str(v).strip()
                    if not val or val.lower() in ["", "none", "null", "0"]: continue
                    
                    if k.lower() in ["phone", "mobile", "telephone", "номер"]:
                        val = clean_id(val)
                        if len(val) < 10: continue
                    
                    if k.lower() == "birth_date" and is_garbage_date(val): continue
                    
                    merged_init[k] = [val]

                persons.append({
                    'fio_display': fio_raw,
                    'fio_norm': fio_norm,
                    'birth_date': bday,
                    'inn': inn,
                    'phone': phone,
                    'count': 1,
                    'merged_data': merged_init
                })
    return persons
