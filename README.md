# GTM Engineering Data Enrichment Pipeline

This project automates the process of cleaning, enriching, and classifying company website data using a series of orchestrated Python scripts. The workflow is managed by `Main.py`, which sequentially runs each processing step to transform raw company data into a structured, enriched dataset.

The dataset is pulled from HubSpot with the following filters:

- Distributor Type == Unknown
- Website URL = Known
- Company Name = Known
- Number of Contacts > 0

## Workflow Overview

The pipeline consists of four main stages:

1. **URL Cleaning**
   - The script standardizes and cleans the 'Website URL' field in the input CSV, ensuring all URLs are in a consistent homepage format. It removes unwanted domains and adds missing protocols as needed. The cleaned data is saved for downstream processing.

2. **Website Information Extraction (Perplexity API)**
   - For each cleaned website URL, the pipeline queries the Perplexity API to extract relevant company information. This information is appended to the dataset, providing a richer context for each company.
   - The process is incremental and robust: it skips rows that already have website information and saves progress after each API call to prevent data loss.

3. **Vertical Classification (OpenAI API)**
   - Using the extracted website information, the pipeline calls the OpenAI API to classify each company into a business vertical. Only companies without an existing vertical classification are processed, and results are saved after each row for reliability.

4. **Data Quality and Cleanup**
   - The final step reviews the enriched data for excessively long or malformed website information fields. It corrects common typos and replaces problematic entries with 'N/A' to ensure the final dataset is clean and usable.

## Orchestration

All steps are coordinated by `Main.py`, which:
- Runs each script in the correct order.
- Ensures that each stage only processes new or missing data, making the workflow efficient and restartable.
- Handles incremental saving and error correction to maximize data integrity.

## Summary

By running `Main.py`, users can:
- Clean and standardize raw company website data.
- Enrich each entry with detailed company information from Perplexity.
- Automatically classify companies into verticals using OpenAI.
- Ensure the final dataset is free of common errors and ready for analysis or integration.

This pipeline is designed for reliability, incremental progress, and easy extension to new data sources or enrichment steps.
