import os
import csv
import pathlib
import tempfile
import asyncio
from dotenv import load_dotenv
from openai import AsyncOpenAI

load_dotenv()

# ---------- CONFIG ----------
INPUT_CSV = 'data/net_new_web_info_parsed.csv'
PROMPT_PATH = 'prompts/company_type.txt'
OUTPUT_CSV = 'data/net_new_web_info_company_type_parsed.csv'
COMPANY_TYPE_COL = 'Company Type'
CONCURRENCY = 10
REQUEST_TIMEOUT = 60
# ----------------------------

# Columns to map to prompt variables (CSV header casing matters)
COL_MAP = {
    'company_name': 'Company Name',   # allow N/A
    'ADDITIONAL_FINDINGS': 'ADDITIONAL FINDINGS',
    'BUSINESS_MODEL': 'BUSINESS_MODEL',
    'PRODUCTS': 'PRODUCTS',
    'WEBSITE_FINDINGS': 'WEBSITE_FINDINGS',
    'TARGET_CUSTOMERS': 'TARGET_CUSTOMERS',
    'DISTRIBUTION_FINDINGS': 'DISTRIBUTION FINDINGS',
    'PRODUCT_BRANDS': 'PRODUCT BRANDS',
}


async def classify_company(client, model, prompt):
    """Call the API with a timeout and return result or error string."""
    try:
        coro = client.responses.create(
            model=model,
            input=prompt,
            reasoning={"effort": "high"}
        )
        result = await asyncio.wait_for(coro, timeout=REQUEST_TIMEOUT)
        return (getattr(result, "output_text", None) or str(result)).strip()
    except asyncio.TimeoutError:
        return "ERROR: timeout"
    except Exception as e:
        return f"ERROR: {e}"


async def main_async():
    # ---- Env check ----
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("ERROR: OPENAI_API_KEY not set.")
        return

    client = AsyncOpenAI()
    model = "gpt-5"

    # ---- Load prompt ----
    try:
        with open(PROMPT_PATH, 'r', encoding='utf-8') as f:
            prompt_template = f.read()
    except FileNotFoundError:
        print(f"ERROR: Prompt file not found at {PROMPT_PATH}")
        return

    # ---- Load existing output (resume support) using URL as key ----
    existing_types = {}
    output_path = pathlib.Path(OUTPUT_CSV)
    if output_path.exists():
        with open(output_path, newline='', encoding='utf-8') as outf:
            reader = csv.DictReader(outf)
            for r in reader:
                url = r.get('URL', '').strip()
                val = r.get(COMPANY_TYPE_COL, '').strip()
                if url and val:
                    existing_types[url] = val

    # ---- Read input CSV ----
    try:
        with open(INPUT_CSV, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            fieldnames = reader.fieldnames or []
            if COMPANY_TYPE_COL not in fieldnames:
                fieldnames = fieldnames + [COMPANY_TYPE_COL]
    except FileNotFoundError:
        print(f"ERROR: Input CSV not found at {INPUT_CSV}")
        return

    print(f"Read {len(rows)} rows. Existing classified: {len(existing_types)}")

    # ---- Decide what to process ----
    rows_to_process = []
    for row in rows:
        url = row.get('URL', '').strip()
        company_name = row.get('Company Name', '').strip()
        business_model = row.get('BUSINESS_MODEL', '').strip()

        # Resume: already classified
        if url in existing_types:
            row[COMPANY_TYPE_COL] = existing_types[url]
            continue

        # Skip only if business model is truly missing
        if business_model in ('', 'Not specified', 'N/A'):
            row[COMPANY_TYPE_COL] = ''
            continue

        # Company Name may be N/A — that is OK
        rows_to_process.append(row)

    print(f"{len(rows_to_process)} rows will be processed.")

    if not rows_to_process:
        with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        print("No rows to process. Output written.")
        return

    SEMAPHORE = asyncio.Semaphore(CONCURRENCY)

    async def process_row(row, idx):
        prompt = prompt_template
        for var, col in COL_MAP.items():
            value = row.get(col, "")
            if value.strip().upper() == "N/A":
                value = ""
            prompt = prompt.replace(f'{{{var}}}', value.strip())

        output = await classify_company(client, model, prompt)
        row[COMPANY_TYPE_COL] = output

        display_name = row.get('Company Name', '').strip()
        if not display_name or display_name.upper() == 'N/A':
            display_name = row.get('URL', '')

        print(f"[{idx+1}/{len(rows_to_process)}] {display_name}: {output[:120]}")
        return row

    async def sem_task(row, idx):
        async with SEMAPHORE:
            return await process_row(row, idx)

    tasks = [asyncio.create_task(sem_task(row, i)) for i, row in enumerate(rows_to_process)]
    gathered = await asyncio.gather(*tasks, return_exceptions=True)

    results = []
    for i, item in enumerate(gathered):
        if isinstance(item, Exception):
            r = rows_to_process[i]
            r[COMPANY_TYPE_COL] = f"ERROR: {item}"
            results.append(r)
            print(f"Task {i} error: {item}")
        else:
            results.append(item)

    # ---- Merge results back using URL ----
    processed_map = {r.get('URL', '').strip(): r for r in results}
    final_rows = []
    for row in rows:
        url = row.get('URL', '').strip()
        final_rows.append(processed_map.get(url, row))

    # ---- Atomic write ----
    tmp_fd, tmp_path = tempfile.mkstemp(
        prefix='tmp_', suffix='.csv',
        dir=str(pathlib.Path(OUTPUT_CSV).parent)
    )
    os.close(tmp_fd)

    with open(tmp_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(final_rows)

    os.replace(tmp_path, OUTPUT_CSV)
    print("Completed processing.")


def main():
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        print("Detected running event loop — scheduling task.")
        return asyncio.create_task(main_async())
    else:
        return asyncio.run(main_async())


if __name__ == "__main__":
    main()
