import os
import csv
import pathlib
import tempfile
import asyncio
from dotenv import load_dotenv
from openai import AsyncOpenAI

load_dotenv()

INPUT_CSV = 'data/Website_comp_info_company_type_v2.csv'
PROMPT_PATH = 'prompts/Vertical.txt'
INFO_COL = 'Website Information'
VERTICAL_COL = 'Vertical'
COMPANY_COL = 'Company name'
CONCURRENCY = 10
REQUEST_TIMEOUT = 45

async def classify_vertical(client, model, prompt):
    try:
        coro = client.responses.create(model=model, input=prompt, reasoning={"effort": "high"})
        result = await asyncio.wait_for(coro, timeout=REQUEST_TIMEOUT)
        return (getattr(result, "output_text", None) or str(result)).strip()
    except asyncio.TimeoutError:
        return "ERROR: timeout"
    except Exception as e:
        return f"ERROR: {e}"

async def main_async():
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("ERROR: OPENAI_API_KEY not set in environment. Load your .env or set the env var.")
        return

    client = AsyncOpenAI()
    model = "gpt-5"

    with open(PROMPT_PATH, 'r', encoding='utf-8') as f:
        prompt_template = f.read()

    with open(INPUT_CSV, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        if VERTICAL_COL not in fieldnames:
            fieldnames = fieldnames + [VERTICAL_COL]
        rows = list(reader)

    # Prepare rows to process
    rows_to_process = []
    for row in rows:
        company_name = row.get(COMPANY_COL, '').strip()
        company_type = row.get('Company Type', '').strip()
        vertical_raw = row.get(VERTICAL_COL)

        if vertical_raw and vertical_raw.strip():
            continue
        if company_type not in ('Distributor', 'Both'):
            continue
        rows_to_process.append(row)

    print(f"{len(rows_to_process)} rows will be processed (not skipped).")

    SEMAPHORE = asyncio.Semaphore(CONCURRENCY)

    async def process_row(row, idx):
        business_model = row.get('BUSINESS_MODEL', '').strip()
        products = row.get('PRODUCTS', '').strip()
        website_findings = row.get('WEBSITE_FINDINGS', '').strip()
        target_customers = row.get('TARGET_CUSTOMERS', '').strip()
        distribution_findings = row.get('DISTRIBUTION FINDINGS', '').strip()
        additional_info = row.get('ADDITIONAL FINDINGS', '').strip()

        prompt = prompt_template
        prompt = prompt.replace('{company name}', row.get(COMPANY_COL, '').strip())
        prompt = prompt.replace('{BUSINESS_MODEL}', business_model)
        prompt = prompt.replace('{PRODUCTS}', products)
        prompt = prompt.replace('{WEBSITE_FINDINGS}', website_findings)
        prompt = prompt.replace('{TARGET_CUSTOMERS}', target_customers)
        prompt = prompt.replace('{DISTRIBUTION FINDINGS}', distribution_findings)
        prompt = prompt.replace('{ADDITIONAL FINDINGS}', additional_info)

        output = await classify_vertical(client, model, prompt)
        row[VERTICAL_COL] = output
        print(f"[{idx+1}/{len(rows_to_process)}] {row.get(COMPANY_COL,'')}: {output[:120]}")
        # Save progress after each result
        # Merge this row into the full rows list and save
        cname = row.get(COMPANY_COL, '').strip()
        for r in rows:
            if r.get(COMPANY_COL, '').strip() == cname:
                r[VERTICAL_COL] = output
                break
        tmp_fd, tmp_path = tempfile.mkstemp(prefix='tmp_', suffix='.csv', dir=str(pathlib.Path(INPUT_CSV).parent))
        os.close(tmp_fd)
        with open(tmp_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        os.replace(tmp_path, INPUT_CSV)
        return row

    async def sem_task(row, idx):
        async with SEMAPHORE:
            return await process_row(row, idx)

    tasks = [asyncio.create_task(sem_task(row, i)) for i, row in enumerate(rows_to_process)]
    gathered = await asyncio.gather(*tasks, return_exceptions=True)

    # Merge results back into original row list
    processed_map = {r.get(COMPANY_COL, '').strip(): r for r in gathered if isinstance(r, dict)}
    final_rows = []
    for row in rows:
        cname = row.get(COMPANY_COL, '').strip()
        if cname in processed_map:
            final_rows.append(processed_map[cname])
        else:
            final_rows.append(row)

    tmp_fd, tmp_path = tempfile.mkstemp(prefix='tmp_', suffix='.csv', dir=str(pathlib.Path(INPUT_CSV).parent))
    os.close(tmp_fd)
    with open(tmp_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(final_rows)
    os.replace(tmp_path, INPUT_CSV)

    print("Completed processing with concurrency.")

def main():
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        print("Detected running event loop — scheduling processing as a background task.")
        return asyncio.create_task(main_async())
    else:
        return asyncio.run(main_async())

if __name__ == '__main__':
    main()