Bulk Email & Subject Line Generator

CLI tool that reads a CSV of leads, fetches 3–6 recent hooks from Perplexity, generates a subject + email via OpenAI, and writes a new CSV with subject and emailBody. Fail‑fast: any row failure aborts the run.

Usage
- Ensure `.env` has `OPENAI_API_KEY` and `PERPLEXITY_API_KEY`.
- `pip install -r requirements.txt`
- `python -m src.cli --input leads.csv --batch-size 50 --concurrency 4 --batch-retries 1`
- Output is written to `output/` unless `--output` is provided.

Notes
- Uses `gpt-5-mini` (Responses API) with normal email output (no JSON); parses `Subject:` line + body.
- Uses `updatedprompt.txt` verbatim for generation instructions.
- Preserves original columns and adds/overwrites `subject` and `emailBody`.
