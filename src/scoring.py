import os
import csv
import pathlib
import asyncio
import traceback
from collections import defaultdict
from dotenv import load_dotenv, find_dotenv
from openai import AsyncOpenAI

# Load .env
load_dotenv()
print("Loaded .env from:", find_dotenv())

INPUT_CSV = 'data/net_new_web_info_company_type_parsed.csv'
SCORE_COL = 'Score'
VERTICAL_COL = 'Vertical'
URL_COL = 'URL'

CONCURRENCY = 10  # adjust concurrency as needed
REQUEST_TIMEOUT = 45
MAX_RETRIES = 3

input_path = pathlib.Path(INPUT_CSV)
with open(input_path, newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    fieldnames = reader.fieldnames
    rows = list(reader)

# If Score column missing, add it *without touching existing data*
if SCORE_COL not in fieldnames:
    new_fieldnames = fieldnames + [SCORE_COL]
    for row in rows:
        row[SCORE_COL] = ""  # leave blank

    # Rewrite CSV but keep ALL original data exactly the same
    with open(input_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=new_fieldnames)
        writer.writeheader()
        writer.writerows(rows)

# ------------------------
# Helpers
# ------------------------
def normalize_vertical(v: str) -> str:
    """Lowercase, remove spaces, hyphens, and slashes for consistent matching."""
    return v.lower().replace(" ", "").replace("-", "").replace("/", "")

# Normalize vertical keys
VERTICAL_PROMPT_MAP = {
    normalize_vertical(k): v for k, v in {
        "Alcohol": "alcohol.txt",
        "Bakery": "bakery.txt",
        "Beverage": "beverage.txt",
        "Broadline": "broadline.txt",
        "C-store": "C-store.txt",
        "Coffee": "coffee.txt",
        "Dairy": "dairy.txt",
        "Grocery": "Grocery.txt",
        "Ice-cream": "ice-cream.txt",
        "Jan-san": "jan-san.txt",
        "Meat": "meat.txt",
        "Other – Food": "other-food.txt",
        "Produce": "Produce.txt",
        "Retail": "Retail.txt",
        "Seafood": "seafood.txt",
        "Vegan-organic-natural": "vegan-organic-natural.txt"
    }.items()
}

# Normalize column map keys to lowercase
COLUMN_MAP = {
    "company_name": "Company Name",
    "BUSINESS_MODEL": "BUSINESS_MODEL",
    "WEBSITE_FINDINGS": "WEBSITE_FINDINGS",
    "TARGET_CUSTOMERS": "TARGET_CUSTOMERS",
    "DISTRIBUTION_FINDINGS": "DISTRIBUTION_FINDINGS"
}


async def classify_score_once(client, model, prompt: str):
    coro = client.responses.create(model=model, input=prompt)
    result = await asyncio.wait_for(coro, timeout=REQUEST_TIMEOUT)
    return getattr(result, "output_text", str(result)).strip()


async def run_all():
    input_path = pathlib.Path(INPUT_CSV)
    with open(input_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        header_map = {h.lower(): h for h in reader.fieldnames}
        fieldnames = list(reader.fieldnames)
        if SCORE_COL not in fieldnames:
            fieldnames.append(SCORE_COL)
        rows = list(reader)

    def get_row_value(row, col_name):
        return row.get(header_map.get(col_name.lower(), col_name), "")

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY not set in environment.")
    client = AsyncOpenAI(api_key=api_key)
    model = "gpt-5"
    SEMAPHORE = asyncio.Semaphore(CONCURRENCY)
    file_lock = asyncio.Lock()  # Add lock for thread-safe file writing

    async def process_row(row):
        if row.get(SCORE_COL, "").strip():
            return row

        try:
            vertical_raw = get_row_value(row, VERTICAL_COL)
            vertical = normalize_vertical(vertical_raw)

            # Skip rows where vertical not mapped or prompt file missing, leave score blank
            if vertical not in VERTICAL_PROMPT_MAP:
                print(f"{vertical_raw}: PROMPT FILE NOT FOUND")
                return row

            prompt_file = pathlib.Path('prompts/vertical-specific-scoring') / VERTICAL_PROMPT_MAP[vertical]
            if not prompt_file.exists():
                print(f"{vertical_raw}: PROMPT FILE NOT FOUND")
                return row

            prompt_template = prompt_file.read_text(encoding='utf-8')
            prompt_vars = {k: get_row_value(row, v) for k, v in COLUMN_MAP.items()}
            prompt = prompt_template.format_map(defaultdict(str, prompt_vars))

            last_exc = None
            score = None
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    async with SEMAPHORE:
                        score = await classify_score_once(client, model, prompt)
                    break
                except asyncio.TimeoutError as te:
                    last_exc = te
                    print(f"Timeout on attempt {attempt} for {row.get('Company name')} (vertical={vertical_raw})")
                except Exception as e:
                    last_exc = e
                    print(f"Attempt {attempt} failed for {row.get('Company name')} (vertical={vertical_raw}): {type(e).__name__}: {e}")

                if attempt < MAX_RETRIES:
                    backoff = 2 ** (attempt - 1)
                    await asyncio.sleep(backoff)

            if score is None:
                err_msg = f"ERROR: Failed after {MAX_RETRIES} attempts: {type(last_exc).__name__}: {str(last_exc)}"
                print(err_msg)
                row[SCORE_COL] = err_msg
            else:
                row[SCORE_COL] = score
                print(f"{vertical_raw} -> {str(score)[:100]}")

            # Save row immediately - use URL as unique identifier
            try:
                url = row.get(URL_COL, '').strip()
                async with file_lock:  # Protect file writes from concurrent access
                    with open(INPUT_CSV, 'r', newline='', encoding='utf-8') as f:
                        existing = list(csv.DictReader(f))
                    updated = False
                    for r in existing:
                        if r.get(URL_COL, '').strip() == url:
                            r[SCORE_COL] = row[SCORE_COL]
                            updated = True
                            break
                    if not updated:
                        new_row = {k: "" for k in existing[0].keys()} if existing else {}
                        for k in row:
                            new_row[k] = row[k]
                        existing.append(new_row)

                    write_fieldnames = list(existing[0].keys())
                    if SCORE_COL not in write_fieldnames:
                        write_fieldnames.append(SCORE_COL)
                        for r in existing:
                            r.setdefault(SCORE_COL, "")

                    with open(INPUT_CSV, 'w', newline='', encoding='utf-8') as f:
                        writer = csv.DictWriter(f, fieldnames=write_fieldnames)
                        writer.writeheader()
                        writer.writerows(existing)
            except Exception as e:
                print(f"Warning: failed to save row for {row.get('Company name')}: {e}")
                traceback.print_exc()

            return row

        except Exception as e:
            print(f"Unhandled exception processing {row.get('Company name')}: {type(e).__name__}: {e}")
            traceback.print_exc()
            row[SCORE_COL] = f"ERROR: Unhandled exception: {type(e).__name__}: {e}"
            return row

    tasks = [asyncio.create_task(process_row(row)) for row in rows]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    normalized_rows = []
    for idx, res in enumerate(results):
        if isinstance(res, Exception):
            print(f"Task {idx} raised exception: {res}")
            traceback.print_exc()
            err_row = rows[idx].copy()
            err_row[SCORE_COL] = f"ERROR: Exception in task: {type(res).__name__}: {res}"
            normalized_rows.append(err_row)
        elif res is None:
            print(f"Task {idx} returned None; converting to error row.")
            err_row = rows[idx].copy()
            err_row[SCORE_COL] = "ERROR: Task returned None"
            normalized_rows.append(err_row)
        elif not isinstance(res, dict):
            print(f"Task {idx} returned non-dict ({type(res).__name__}); converting to a row dict.")
            err_row = rows[idx].copy()
            err_row[SCORE_COL] = str(res)
            normalized_rows.append(err_row)
        else:
            normalized_rows.append(res)

    try:
        final_fieldnames = list(normalized_rows[0].keys()) if normalized_rows else fieldnames
        if SCORE_COL not in final_fieldnames:
            final_fieldnames.append(SCORE_COL)
        with open(input_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=final_fieldnames)
            writer.writeheader()
            writer.writerows(normalized_rows)
    except Exception as e:
        print(f"Failed to write final CSV: {e}")
        traceback.print_exc()

    result_map = {}
    for i, r in enumerate(normalized_rows):
        key = r.get('Company name') or f'__row_{i}'
        result_map[key] = r

    return result_map


def vertical_score_all():
    result = asyncio.run(run_all())
    print(f"Finished. Processed {len(result)} rows.")

    for company, row in result.items():
        score_preview = str(row.get(SCORE_COL, "") or "")[:120]
        print(f"{company}: {score_preview}")

    return result

def main():
    return vertical_score_all()

if __name__ == "__main__":
    main()