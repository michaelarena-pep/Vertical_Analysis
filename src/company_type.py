import os
import csv
import pathlib
import tempfile
import asyncio
from dotenv import load_dotenv
from openai import AsyncOpenAI

load_dotenv()

# ---------- CONFIG ----------
INPUT_CSV = 'data/Website_comp_info_parsed_v2.csv'
PROMPT_PATH = 'prompts/company_type.txt'
OUTPUT_CSV = 'data/Website_comp_info_company_type_v2.csv'
COMPANY_TYPE_COL = 'Company Type'
CONCURRENCY = 10                # semaphore limit
REQUEST_TIMEOUT = 60            # seconds per API call
# ----------------------------

# Columns to map to prompt variables
COL_MAP = {
    'company_name': 'Company name',
    'ADDITIONAL_FINDINGS': 'ADDITIONAL FINDINGS',
    'BUSINESS_MODEL': 'BUSINESS_MODEL',
    'PRODUCTS': 'PRODUCTS',
    'WEBSITE_FINDINGS': 'WEBSITE_FINDINGS',
    'TARGET_CUSTOMERS': 'TARGET_CUSTOMERS',
    'DISTRIBUTION_FINDINGS': 'DISTRIBUTION FINDINGS',
    'PRODUCT BRANDS': 'PRODUCT BRANDS',
}


async def classify_company(client, model, prompt):
    """Call the API with a timeout and return result or error string."""
    try:
        # guard each call with a timeout so a single stuck call won't hang everything
        coro = client.responses.create(model=model, input=prompt, reasoning={"effort": "high"})
        result = await asyncio.wait_for(coro, timeout=REQUEST_TIMEOUT)
        # result.output_text is what earlier examples used; adapt if your SDK differs
        return (getattr(result, "output_text", None) or str(result)).strip()
    except asyncio.TimeoutError:
        return "ERROR: timeout"
    except Exception as e:
        return f"ERROR: {e}"


async def main_async():
    # Basic env check
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("ERROR: OPENAI_API_KEY not set in environment. Load your .env or set the env var.")
        return

    # instantiate async client (some SDKs pick up key from env automatically)
    client = AsyncOpenAI()  # if your SDK needs api_key arg, change to AsyncOpenAI(api_key=api_key)
    model = "gpt-5"

    # Load prompt template
    try:
        with open(PROMPT_PATH, 'r', encoding='utf-8') as f:
            prompt_template = f.read()
    except FileNotFoundError:
        print(f"ERROR: Prompt file not found at {PROMPT_PATH}")
        return

    # Load existing output if present
    existing_types = {}
    output_path = pathlib.Path(OUTPUT_CSV)
    if output_path.exists():
        with open(output_path, newline='', encoding='utf-8') as outf:
            out_reader = csv.DictReader(outf)
            for out_row in out_reader:
                key = out_row.get('Company name', '').strip()
                val = out_row.get(COMPANY_TYPE_COL, '').strip()
                if val:
                    existing_types[key] = val

    # Read input CSV
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

    print(f"Read {len(rows)} rows from {INPUT_CSV}. Existing classified: {len(existing_types)}")

    # Prepare rows to process
    rows_to_process = []
    for row in rows:
        company_name = row.get('Company name', '').strip()
        business_model = row.get('BUSINESS_MODEL', '').strip()

        if company_name in existing_types and existing_types[company_name]:
            row[COMPANY_TYPE_COL] = existing_types[company_name]
            continue

        if business_model in ('Not specified', 'N/A'):
            row[COMPANY_TYPE_COL] = ''
            continue

        rows_to_process.append(row)

    print(f"{len(rows_to_process)} rows will be processed (not skipped).")

    if not rows_to_process:
        # Nothing to do — write (or rewrite) the file to ensure the Company Type column is present
        with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        print("No rows to process. Output CSV written/updated.")
        return

    SEMAPHORE = asyncio.Semaphore(CONCURRENCY)

    async def process_row(row, idx):
        prompt = prompt_template
        for var, col in COL_MAP.items():
            prompt = prompt.replace(f'{{{var}}}', row.get(col, "").strip())
        output = await classify_company(client, model, prompt)
        row[COMPANY_TYPE_COL] = output
        print(f"[{idx+1}/{len(rows_to_process)}] {row.get('Company name','')}: {output[:120]}")
        return row

    async def sem_task(row, idx):
        async with SEMAPHORE:
            return await process_row(row, idx)

    tasks = [asyncio.create_task(sem_task(row, i)) for i, row in enumerate(rows_to_process)]
    # gather with return_exceptions so we can inspect if some tasks errored
    gathered = await asyncio.gather(*tasks, return_exceptions=True)

    # convert any exception objects into error strings and print them
    results = []
    for i, item in enumerate(gathered):
        if isinstance(item, Exception):
            print(f"Task {i} raised exception: {item}")
            # convert to error row so merging later keeps shape
            r = rows_to_process[i]
            r[COMPANY_TYPE_COL] = f"ERROR: {item}"
            results.append(r)
        else:
            results.append(item)

    # Merge results back into original row list
    processed_map = {r.get('Company name', '').strip(): r for r in results}
    final_rows = []
    for row in rows:
        cname = row.get('Company name', '').strip()
        if cname in processed_map:
            final_rows.append(processed_map[cname])
        else:
            final_rows.append(row)

    # Save output CSV (atomic replace)
    tmp_fd, tmp_path = tempfile.mkstemp(prefix='tmp_', suffix='.csv', dir=str(pathlib.Path(OUTPUT_CSV).parent))
    os.close(tmp_fd)
    with open(tmp_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(final_rows)
    os.replace(tmp_path, OUTPUT_CSV)

    print("Completed processing with concurrency.")


def main():
    """
    Synchronous entrypoint that is safe to call from:
      - a normal script (will block until done)
      - an async environment (it will schedule the task and return an asyncio.Task)
    If you call this from another async function, prefer to `await main_async()` directly.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        # schedule and return the Task (non-blocking). Caller can keep the returned Task if they want.
        print("Detected running event loop — scheduling processing as a background task.")
        return asyncio.create_task(main_async())
    else:
        # no loop running — run and block until finished
        return asyncio.run(main_async())


if __name__ == "__main__":
    main()
