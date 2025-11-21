"""
Script to clean the 'Website URL' column in a CSV by adding 'https://' if missing or if url is not homepage.
Reads from 'data/Unknown Vert.csv' and writes to 'data/cleaned_url_unknown_vert.csv'.
"""
import csv
import pathlib
from urllib.parse import urlparse

INPUT_CSV = 'data/Unknown Vert.csv'
OUTPUT_CSV = 'data/cleaned_url_unknown_vert.csv'
URL_COL = 'Website URL'
KEEP_COLS = ['Record ID', 'Company name', 'Website URL']

def clean_url(url):
    url = (url or '').strip()
    if not url:
        return url
    # Add https:// if missing
    if not url.startswith(('http://', 'https://')):
        url = f'https://{url}'
    # Parse and reconstruct only scheme + netloc
    parsed = urlparse(url)
    if parsed.scheme and parsed.netloc:
        return f'{parsed.scheme}://{parsed.netloc}'
    return url


def main():
    blocklist = ["google", "outlook", "yahoo"]
    with open(INPUT_CSV, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        missing = [col for col in KEEP_COLS if col not in fieldnames]
        if missing:
            print(f"Missing columns in input: {missing}")
            return
        rows = []
        for row in reader:
            cleaned_row = {col: row.get(col, '') for col in KEEP_COLS}
            cleaned_url = clean_url(cleaned_row.get(URL_COL, ''))
            # Blocklist check
            if any(bad in cleaned_url.lower() for bad in blocklist):
                continue
            cleaned_row[URL_COL] = cleaned_url
            rows.append(cleaned_row)

    outp = pathlib.Path(OUTPUT_CSV)
    outp.parent.mkdir(parents=True, exist_ok=True)
    with open(outp, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=KEEP_COLS)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote cleaned CSV to {OUTPUT_CSV}")

if __name__ == '__main__':
    main()
