#!/usr/bin/env python3
"""
Reads cleaned_url_unknown_vert.csv, uses Website URL and a prompt to call Perplexity API (using the official SDK),
and writes all original columns plus a new column with the API result to Website_comp_info.csv.
Skips rows where Website Information is already populated.
"""

import csv
import pathlib
import asyncio
from perplexity import AsyncPerplexity, DefaultAioHttpClient
from dotenv import load_dotenv

csv.field_size_limit(10**7)  # adjust if needed

load_dotenv()

INPUT_CSV = 'data/cleaned_url_unknown_vert.csv'
OUTPUT_CSV = 'data/Website_comp_info.csv'
PROMPT_PATH = 'prompts/Website_info.txt'
URL_COL = 'Website URL'
RESULT_COL = 'Website Information'

def load_prompt(path):
    with open(path, 'r', encoding='utf-8') as f:
        return f.read()

async def async_call_perplexity_client(client, prompt_text):
    try:
        completion = await client.chat.completions.create(
            messages=[{"role": "user", "content": prompt_text}],
            model="sonar-reasoning",
            reasoning_effort="high",
            max_tokens=4500
        )
        if hasattr(completion, 'choices') and completion.choices:
            return completion.choices[0].message.content
        return str(completion)
    except Exception as e:
        return f"ERROR: {e}"

def main():
    async def async_main():
        prompt_template = load_prompt(PROMPT_PATH)
        input_path = pathlib.Path(INPUT_CSV)
        output_path = pathlib.Path(OUTPUT_CSV)

        # Load input CSV
        with open(input_path, newline='', encoding='utf-8') as inf:
            reader = csv.DictReader(inf)
            rows = list(reader)
            input_fieldnames = reader.fieldnames or []

        # Load existing output CSV to preserve all columns
        existing_rows = {}
        all_fieldnames = list(input_fieldnames)
        if output_path.exists():
            with open(output_path, newline='', encoding='utf-8') as outf:
                out_reader = csv.DictReader(outf)
                for r in out_reader:
                    key = r.get(URL_COL, '').strip()
                    existing_rows[key] = r
                    # Add any extra columns not in input CSV
                    for col in r.keys():
                        if col not in all_fieldnames:
                            all_fieldnames.append(col)
        # Ensure RESULT_COL is in fieldnames
        if RESULT_COL not in all_fieldnames:
            all_fieldnames.append(RESULT_COL)

        # Merge existing info
        for row in rows:
            url = row.get(URL_COL, '').strip()
            if url in existing_rows:
                row.update(existing_rows[url])

        semaphore = asyncio.Semaphore(6)  # limit concurrency

        async def limited_request(client, idx, prompt, url):
            async with semaphore:
                print(f'Querying Perplexity for: {url}')
                result = await async_call_perplexity_client(client, prompt)
                if result and "</think>" in result:
                    result = result.split("</think>", 1)[1].strip()
                rows[idx][RESULT_COL] = result

                # Save progress safely after each row
                output_path.parent.mkdir(parents=True, exist_ok=True)
                with open(output_path, 'w', newline='', encoding='utf-8') as outf:
                    writer = csv.DictWriter(outf, fieldnames=all_fieldnames)
                    writer.writeheader()
                    writer.writerows(rows)
                print(f'Progress saved after row {idx+1}/{len(rows)}')

        async with AsyncPerplexity(http_client=DefaultAioHttpClient()) as client:
            tasks = []
            for idx, row in enumerate(rows):
                url = row.get(URL_COL, '').strip()
                existing_result = row.get(RESULT_COL, '').strip()
                # Skip rows that already have a value
                if existing_result:
                    print(f"[{idx+1}/{len(rows)}] {url}: SKIPPED (already has info)")
                    continue
                # Skip rows without URL
                if not url:
                    print(f"[{idx+1}/{len(rows)}] (no URL) skipped")
                    continue
                # Build prompt
                if '{URL}' in prompt_template:
                    prompt = prompt_template.replace('{URL}', url)
                else:
                    prompt = f'{prompt_template}\n\nWebsite: {url}'
                tasks.append(limited_request(client, idx, prompt, url))
            await asyncio.gather(*tasks)

        print(f'Finished writing results to {OUTPUT_CSV}')

    asyncio.run(async_main())

if __name__ == '__main__':
    main()

# ~1.9 cents per website run.