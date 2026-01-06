import csv
import pathlib
import re

INPUT_CSV = 'data/net_new_web_info.csv'
OUTPUT_CSV = 'data/net_new_web_info_parsed.csv'
INFO_COL = 'Website Information'

# List of categories to extract
CATEGORIES = [
    'COMPANY_NAME',
    'PRODUCTS',
    'BUSINESS_MODEL',
    'WEBSITE_FINDINGS',
    'TARGET_CUSTOMERS',
    'DISTRIBUTION FINDINGS',
    'NUMBER OF TRUCKS',
    'PAYMENT PROCESSING',
    'ERP',
    'PRODUCT BRANDS',
    'NUMBER OF SKUS',
    'PHONE NUMBERS & EMAILS',
    'ADDRESS',
    'ADDITIONAL FINDINGS'
]

# Build regex for each category
# Use ^[A-Z _]+: to match next label at start of line (multiline)
CATEGORY_REGEX = {
    cat: re.compile(rf'{cat}:\s*(.*?)(?=^([A-Z _]+):|$)', re.DOTALL | re.MULTILINE)
    for cat in CATEGORIES
}

def parse_info(info):
    if not info or info.strip() == 'N/A':
        return {cat: 'N/A' for cat in CATEGORIES}
    result = {}
    for cat, regex in CATEGORY_REGEX.items():
        match = regex.search(info)
        if match:
            val = match.group(1).strip()
            result[cat] = val
        else:
            result[cat] = 'N/A'
    return result

def main():
    input_path = pathlib.Path(INPUT_CSV)
    output_path = pathlib.Path(OUTPUT_CSV)
    with open(input_path, newline='', encoding='utf-8') as inf:
        reader = csv.DictReader(inf)
        fieldnames = reader.fieldnames or []
        # Remove 'Vertical' and 'Website Information' from output
        exclude = {'Vertical', INFO_COL}
        base_fieldnames = [f for f in fieldnames if f not in exclude]
        new_fieldnames = base_fieldnames + CATEGORIES
        rows = []
        for row in reader:
            info = row.get(INFO_COL, '')
            parsed = parse_info(info)
            # Build new row without excluded columns
            new_row = {f: row.get(f, '') for f in base_fieldnames}
            for cat in CATEGORIES:
                new_row[cat] = parsed[cat]
            rows.append(new_row)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w', newline='', encoding='utf-8') as outf:
        writer = csv.DictWriter(outf, fieldnames=new_fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f'Parsed CSV written to {OUTPUT_CSV}')

if __name__ == '__main__':
    main()
