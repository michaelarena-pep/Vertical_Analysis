import os
import csv
import pathlib
import tempfile
import asyncio
import time
from dotenv import load_dotenv
from openai import AsyncOpenAI

# ------------------------
# Configuration
# ------------------------
load_dotenv()

INPUT_CSV = os.getenv('INPUT_CSV', 'data/Website_comp_info_company_type.csv')
PROMPTS_DIR = os.getenv('PROMPTS_DIR', 'prompts/sub-verticals')
SUB_VERTICAL_COL = os.getenv('SUB_VERTICAL_COL', 'Sub Vertical')
COMPANY_COL = os.getenv('COMPANY_COL', 'Company name')
VERTICAL_COL = os.getenv('VERTICAL_COL', 'Vertical')
RECORD_ID_COL = os.getenv('RECORD_ID_COL', 'Record ID')  # <-- using Record ID as unique identifier

# Concurrency and save frequency (tunable via env)
CONCURRENCY = int(os.getenv('CONCURRENCY', '15'))
REQUEST_TIMEOUT = int(os.getenv('REQUEST_TIMEOUT', '45'))
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
SAVE_EVERY = int(os.getenv('SAVE_EVERY', '50'))  # save progress to disk every N processed rows

# Model selection
MODEL = os.getenv('OPENAI_MODEL', 'gpt-5')

# ------------------------
# Helper: classify with retry/backoff
# ------------------------
async def classify_sub_vertical(client, model, prompt):
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            coro = client.responses.create(model=model, input=prompt, reasoning={"effort": "high"})
            result = await asyncio.wait_for(coro, timeout=REQUEST_TIMEOUT)
            return (getattr(result, "output_text", None) or str(result)).strip()
        except asyncio.TimeoutError:
            last_exc = Exception('timeout')
            err_str = 'ERROR: timeout'
        except Exception as e:
            last_exc = e
            err_str = f"ERROR: {e}"

        # If we're going to retry, sleep with exponential backoff
        if attempt < MAX_RETRIES:
            backoff = 2 ** (attempt - 1)
            await asyncio.sleep(backoff)
        else:
            # final attempt failed; return error string
            return err_str

# ------------------------
# Main async flow
# ------------------------
async def main_async():
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        print("ERROR: OPENAI_API_KEY not set in environment. Load your .env or set the env var.")
        return

    client = AsyncOpenAI()
    model = MODEL

    # Pre-load all prompt templates into memory (fix for issue #2)
    prompt_cache = {}

    # Load default template
    default_template_path = pathlib.Path(PROMPTS_DIR) / 'Sub-Vertical-Template.txt'
    if default_template_path.exists():
        prompt_cache['__default__'] = default_template_path.read_text(encoding='utf-8')
    else:
        prompt_cache['__default__'] = ''

    # Map of vertical -> filename (same as original mapping)
    vertical_prompt_map = {
        "Alcohol": "Alcohol.txt",
        "Bakery": "bakery.txt",
        "Beverage": "beverage.txt",
        "Broadline": "Broadline.txt",
        "C-Store": "c-store.txt",
        "Ice Cream": "Ice-cream.txt",
        "Jan-San": "Jan-san.txt",
        "Meat": "meat.txt",
        "Produce": "produce.txt",
        "Seafood": "seafood.txt"
    }

    # Preload vertical-specific prompts if available
    for v, fname in vertical_prompt_map.items():
        p = pathlib.Path(PROMPTS_DIR) / fname
        if p.exists():
            prompt_cache[v] = p.read_text(encoding='utf-8')
        else:
            prompt_cache[v] = prompt_cache['__default__']

    # Read CSV once
    input_path = pathlib.Path(INPUT_CSV)
    if not input_path.exists():
        print(f"ERROR: Input CSV not found at {INPUT_CSV}")
        return

    with open(input_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        # Ensure essential columns exist in fieldnames
        for required in (SUB_VERTICAL_COL, RECORD_ID_COL):
            if required not in fieldnames:
                fieldnames.append(required)
        rows = list(reader)

    # Allowed verticals (only process rows whose Vertical is in this set)
    ALLOWED_VERTICALS = {
        "Alcohol", "Bakery", "Beverage", "Broadline", "C-Store",
        "Ice Cream", "Jan-San", "Meat", "Produce", "Seafood"
    }

    # Prepare rows to process
    rows_to_process = []
    for row in rows:
        # Use Record ID as the unique identifier (fix for issue #5)
        record_id = (row.get(RECORD_ID_COL) or '').strip()
        if not record_id:
            # If there's no Record ID, skip and warn (safer than matching on company name)
            print(f"Skipping row with missing {RECORD_ID_COL}: {row.get(COMPANY_COL, '')[:60]}")
            continue

        sub_vertical_raw = row.get(SUB_VERTICAL_COL)
        vertical_raw = (row.get(VERTICAL_COL) or '').strip()
        # Skip if Sub Vertical already has any text
        if sub_vertical_raw and sub_vertical_raw.strip():
            continue
        # Skip if Vertical is empty
        if not vertical_raw:
            continue
        # Skip if vertical not supported
        if vertical_raw not in ALLOWED_VERTICALS:
            continue

        rows_to_process.append(row)

    total = len(rows_to_process)
    print(f"{total} rows will be processed (not skipped).")

    SEMAPHORE = asyncio.Semaphore(CONCURRENCY)

    processed_count = 0
    processed_map = {}  # record_id -> processed row

    async def process_row(row, idx):
        nonlocal processed_count
        # Pull all relevant columns
        business_model = (row.get('BUSINESS_MODEL') or '').strip()
        products = (row.get('PRODUCTS') or '').strip()
        website_findings = (row.get('WEBSITE_FINDINGS') or '').strip()
        target_customers = (row.get('TARGET_CUSTOMERS') or '').strip()
        distribution_findings = (row.get('DISTRIBUTION FINDINGS') or '').strip()
        product_brands = (row.get('PRODUCT BRANDS') or '').strip()
        additional_info = (row.get('ADDITIONAL FINDINGS') or '').strip()
        vertical = (row.get(VERTICAL_COL) or '').strip()
        company_name = (row.get(COMPANY_COL) or '').strip()
        record_id = (row.get(RECORD_ID_COL) or '').strip()

        # Select prompt template from cache
        prompt_template = prompt_cache.get(vertical) or prompt_cache['__default__']

        # Build prompt safely using replacements
        prompt = prompt_template
        # Use a simple replace for placeholders that are known to appear in templates
        replacements = {
            '{company name}': company_name,
            '{VERTICAL}': vertical,
            '{BUSINESS_MODEL}': business_model,
            '{PRODUCTS}': products,
            '{WEBSITE_FINDINGS}': website_findings,
            '{TARGET_CUSTOMERS}': target_customers,
            '{DISTRIBUTION FINDINGS}': distribution_findings,
            '{PRODUCT BRANDS}': product_brands,
            '{ADDITIONAL FINDINGS}': additional_info,
        }
        for k, v in replacements.items():
            prompt = prompt.replace(k, v)

        output = await classify_sub_vertical(client, model, prompt)
        row[SUB_VERTICAL_COL] = output

        # Update processed_map by Record ID (fix for issue #5)
        processed_map[record_id] = row
        processed_count += 1

        print(f"[{idx+1}/{total}] {company_name} (ID={record_id}): {output[:120]}")

        # Periodic save to disk instead of every row (fix for issue #1)
        if processed_count % SAVE_EVERY == 0 or processed_count == total:
            await save_progress(rows, processed_map, fieldnames, input_path)

        return row

    async def sem_task(row, idx):
        async with SEMAPHORE:
            return await process_row(row, idx)

    # Launch tasks
    tasks = [asyncio.create_task(sem_task(row, i)) for i, row in enumerate(rows_to_process)]
    gathered = await asyncio.gather(*tasks, return_exceptions=True)

    # Merge results back into original row list using Record ID
    final_rows = []
    for row in rows:
        record_id = (row.get(RECORD_ID_COL) or '').strip()
        if record_id and record_id in processed_map:
            final_rows.append(processed_map[record_id])
        else:
            final_rows.append(row)

    # Final save (single write) (addresses issue #1)
    await save_final(rows=final_rows, fieldnames=fieldnames, input_path=input_path)

    print("Completed processing with concurrency.")

# ------------------------
# Helpers: save functions
# ------------------------
async def save_progress(rows, processed_map, fieldnames, input_path):
    """Saves current progress by merging processed_map into rows and writing to disk.
    This is called periodically (every SAVE_EVERY rows) rather than after every single row.
    """
    merged = []
    for r in rows:
        rid = (r.get(RECORD_ID_COL) or '').strip()
        if rid and rid in processed_map:
            merged.append(processed_map[rid])
        else:
            merged.append(r)

    # atomic write
    tmp_fd, tmp_path = tempfile.mkstemp(prefix='tmp_', suffix='.csv', dir=str(input_path.parent))
    os.close(tmp_fd)
    with open(tmp_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(merged)
    os.replace(tmp_path, str(input_path))
    print(f"Progress saved to {input_path} at {time.strftime('%Y-%m-%d %H:%M:%S')}")

async def save_final(rows, fieldnames, input_path):
    tmp_fd, tmp_path = tempfile.mkstemp(prefix='tmp_final_', suffix='.csv', dir=str(input_path.parent))
    os.close(tmp_fd)
    with open(tmp_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp_path, str(input_path))
    print(f"Final results saved to {input_path}")

# ------------------------
# Entrypoint
# ------------------------
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
