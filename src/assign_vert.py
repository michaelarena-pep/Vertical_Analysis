
import os
import csv
import pathlib
import tempfile
from dotenv import load_dotenv
from openai import OpenAI

def main():

    load_dotenv()
    OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')

    INPUT_CSV = 'data/Website_comp_info.csv'
    PROMPT_PATH = 'prompts/Vertical.txt'
    INFO_COL = 'Website Information'
    VERTICAL_COL = 'Vertical'
    COMPANY_COL = 'Company name'

    client = OpenAI()

    # Load prompt template
    with open(PROMPT_PATH, 'r', encoding='utf-8') as f:
        prompt_template = f.read()


    with open(INPUT_CSV, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        if VERTICAL_COL not in fieldnames:
            fieldnames = fieldnames + [VERTICAL_COL]
        rows = list(reader)

    def save_progress(rows, csv_path, fieldnames):
        """Write full CSV to a temp file, then atomically replace the original."""
        csv_path = pathlib.Path(csv_path)
        tmp_fd, tmp_path = tempfile.mkstemp(prefix='tmp_', suffix='.csv',
                                            dir=str(csv_path.parent))
        os.close(tmp_fd)
        with open(tmp_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        os.replace(tmp_path, csv_path)

    for idx, row in enumerate(rows):
        website_info = row.get(INFO_COL, '')
        company_name = row.get(COMPANY_COL, '')

        vertical_raw = row.get(VERTICAL_COL)
        # Skip if Vertical already has any text
        if vertical_raw and vertical_raw.strip():
            print(f"[{idx+1}/{len(rows)}] {company_name}: SKIPPED (Vertical already set)")
            continue

        # Skip rows without website info
        if not website_info or not website_info.strip():
            print(f"[{idx+1}/{len(rows)}] {company_name}: SKIPPED (no website info)")
            continue

        # Clean strings for prompt
        website_info = website_info.strip()
        company_name = company_name.strip()

        prompt = prompt_template
        prompt = (
            prompt.replace('{INFO}', website_info)
            if '{INFO}' in prompt else f"{prompt}\n\nWebsite Information: {website_info}"
        )
        prompt = (
            prompt.replace('{COMPANY}', company_name)
            if '{COMPANY}' in prompt else f"{prompt}\n\nCompany name: {company_name}"
        )
        try:
            result = client.responses.create(
                model="gpt-5",
                input=prompt,
                reasoning={"effort": "high"}
            )
            output = result.output_text.strip()
            row[VERTICAL_COL] = output
            print(f"[{idx+1}/{len(rows)}] {company_name}: {output}")
        except Exception as e:
            row[VERTICAL_COL] = f"ERROR: {e}"
            print(f"[{idx+1}/{len(rows)}] {company_name}: ERROR: {e}")

        # Save after each row
        save_progress(rows, INPUT_CSV, fieldnames)

if __name__ == '__main__':
    main()