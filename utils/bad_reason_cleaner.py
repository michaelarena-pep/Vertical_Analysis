import csv
csv.field_size_limit(10**7)

INPUT_CSV = 'data/net_new_web_info.csv'
COLUMN = 'Website Information'
THRESHOLD = 7500

def count_long_fields():
    count = 0
    total = 0
    over_urls = []
    rows = []
    with open(INPUT_CSV, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        for row in reader:
            val = row.get(COLUMN, '')
            # Fix typo in Website Information
            if val:
                fixed_val = val.replace('NUMNBER', 'NUMBER')
                if fixed_val != val:
                    row[COLUMN] = fixed_val
                    val = fixed_val
            if val and len(val) > THRESHOLD:
                count += 1
                over_urls.append(row.get('Website URL', ''))
                row[COLUMN] = 'N/A'
            rows.append(row)
            total += 1
    print(f"Rows with '{COLUMN}' > {THRESHOLD} chars: {count} / {total}")
    if over_urls:
        print('URLs with long Website Information:')
        for url in over_urls:
            print(url)
    # Write back the modified rows
    with open(INPUT_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

if __name__ == '__main__':
    count_long_fields()
