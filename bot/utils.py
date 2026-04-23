import re
from jinja2 import Template
import messages

def generate_html_report(query, results, current_time_str):
    cleaned_results = []
    has_letters = re.compile(r'[a-zA-Zа-яА-ЯёЁ]')

    for table_data in results:
        if len(table_data) <= 1:
            continue
            
        source_name = str(table_data[0]).lower()
        is_restricted_source = "відомості про фізичних осіб" in source_name

        new_table_data = [table_data[0]] 
        for row in table_data[1:]:
            new_row = {}
            
            raw_val = row.get('raw_data', '')
            if raw_val and str(raw_val).strip() not in ["", "{}", "[]", "None", "null"]:
                try:
                    import json
                    data = json.loads(str(raw_val))
                    if isinstance(data, dict):
                        for rk, rv in data.items():
                            val = rv
                            if isinstance(rv, str) and (rv.strip().startswith('{') or rv.strip().startswith('[')):
                                try: val = json.loads(rv)
                                except: pass
                            
                            display_key = rk
                            if rk == 'SQL_COL_13': display_key = "Доп. інформація"
                            elif rk.startswith('SQL_COL_'): display_key = f"Инфо ({rk.split('_')[-1]})"
                            
                            if isinstance(val, dict):
                                for ik, iv in val.items():
                                    if iv: new_row[f"{display_key}: {ik}"] = iv
                            else:
                                if val: new_row[display_key] = val
                    else:
                        new_row["Доп. інформація"] = raw_val
                except:
                    new_row["Доп. інформація"] = raw_val

            for k, v in row.items():
                if not v or k == 'raw_data': continue
                val_str = str(v).strip()
                
                if val_str.lower() in ["", "none", "null", "nan", "undefined"]:
                    continue
                
                if is_restricted_source and k.lower() in ['address', 'birth_date', 'адрес', 'дата народження']:
                    continue

                if k == 'birth_date' and has_letters.search(val_str):
                    continue
                
                if k.lower() in ['phone', 'mobile', 'telephone', 'телефон', 'номер']:
                    if ',' in val_str or ';' in val_str:
                        parts = re.split(r'[,;]', val_str)
                        val_str = "<br>".join(p.strip() for p in parts if p.strip())
                
                if k in ['inn', 'snils', 'phone', 'tg_id']:
                    parts_to_check = val_str.split("<br>")
                    valid_parts = []
                    for pt in parts_to_check:
                        clean_pt = re.sub(r'[\s\-\.\(\)\+]', '', pt)
                        if clean_pt.isdigit(): valid_parts.append(pt)
                    if not valid_parts: continue
                    val_str = "<br>".join(valid_parts)
                
                new_row[k] = val_str
            
            if new_row:
                new_table_data.append(new_row)
        
        if len(new_table_data) > 1:
            cleaned_results.append(new_table_data)

    total_records = sum(len(table_data) - 1 for table_data in cleaned_results)
    template = Template(messages.HTML_REPORT_TEMPLATE)
    return template.render(query=query, results=cleaned_results, total_records=total_records, current_time=current_time_str)
