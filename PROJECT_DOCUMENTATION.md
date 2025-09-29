# Email Subject Line Generator - Complete Project Documentation
**Last Updated:** January 29, 2025

## Overview
This project generates personalized cold emails at scale by combining real-time company research (via Perplexity API) with AI-powered email generation (via OpenAI GPT-5-mini). It processes CSV files of leads and outputs emails with compelling subject lines based on recent company events.

## Architecture Flow
```
CSV Input → Perplexity Research → OpenAI Generation → CSV Output
         ↓                     ↓                   ↓
    (Company Data)      (Recent Hooks)      (Email + Subject)
```

## Core Components

### 1. Entry Point (`src/cli.py`)
- **Purpose:** Command-line interface for the pipeline
- **Key Functions:**
  - Parses arguments: `--input`, `--output`, `--concurrency`, `--batch-size`
  - Loads configuration from `.env` file
  - Initiates pipeline execution

### 2. Configuration (`src/config.py`)
- **Purpose:** Centralized configuration management
- **Environment Variables:**
  ```
  OPENAI_API_KEY         # Required for email generation
  PERPLEXITY_API_KEY     # Required for company research
  OPENAI_MODEL           # Default: "gpt-5-mini"
  PERPLEXITY_MODEL       # Default: "sonar"
  CONCURRENCY            # Default: 4 workers
  BATCH_SIZE             # Default: 50
  REQUEST_TIMEOUT        # Default: 60 seconds
  OUTPUT_DIR             # Default: "output"
  ```

### 3. Pipeline Orchestrator (`src/pipeline.py`)
- **Purpose:** Manages concurrent processing of rows
- **Key Features:**
  - ThreadPoolExecutor for parallel processing
  - Progress tracking (when PROGRESS=1)
  - Hook validation (2-8 hooks required)
  - Error handling with retries
  - CSV output with timestamps

### 4. Research Module (`src/research.py`)
- **Purpose:** Fetches recent company information via Perplexity
- **Key Features:**
  - Semaphore-based rate limiting (PPLX_MAX_CONC)
  - Direct Perplexity API only
  - Automatic endpoint detection based on API key prefix
  - Returns 3-6 "hooks" (recent events) per company
- **Rate Limits:**
  - Free tier: 50 requests/min
  - Tier 1: 300 requests/min

### 5. Email Generator (`src/generator.py`)
- **Purpose:** Creates personalized emails using OpenAI
- **Input:** Company data + research hooks
- **Output:** Subject line + email body
- **Prompt File:** `updatedprompt.txt`
- **Key Features:**
  - Uses GPT-5-mini's Responses API
  - Structured JSON output parsing
  - Thread-safe OpenAI client pooling

### 6. I/O Utilities (`src/io_utils.py`)
- **Purpose:** Handles CSV reading/writing
- **Features:**
  - Safe CSV reading with pandas
  - Timestamp-based output filenames
  - Preserves all original columns

### 7. Metrics (`src/metrics.py`)
- **Purpose:** Tracks token usage and API calls
- **Counters:**
  - OpenAI input/output tokens
  - Perplexity input/output tokens
  - Hook validation failures

## Prompt Structure (`updatedprompt.txt`)
The prompt enforces:
1. **Subject Format:** `{firstName} + Specific Hook + Context Angle`
2. **Email Structure:**
   - Opening with hook + compliment
   - "I wanted to..." paragraph (pain points)
   - "We run a..." paragraph (service pitch)
   - "For [Company]..." paragraph (specific benefit)
   - Loom offer + softener
3. **Key Requirements:**
   - Use "probably" not "often"
   - Use "a light SMS sequence" not "SMS/email"
   - Use "designed to get them to book an appointment"
   - 80-120 words total

## Running the System

### Basic Usage
```bash
python -m src.cli --input leads.csv --concurrency 12
```

### Direct Perplexity API Only
```bash
export CONCURRENCY=12
export PPLX_MAX_CONC=12
python -m src.cli --input leads.csv --concurrency 12
```

## Cost Analysis

### Direct Perplexity API
- **Sonar model:** $1.00 per 1M tokens
- **Per row:** ~$0.003 (3,000 tokens)
- **4,000 rows:** ~$12


### OpenAI (GPT-5-mini)
- **Input:** $0.15 per 1M tokens
- **Output:** $0.60 per 1M tokens
- **Per row:** ~$0.001

## Performance Benchmarks

| Configuration | Workers | Speed | 4k rows time | Cost |
|--------------|---------|--------|--------------|------|
| Direct API (Free) | 6 | 0.5 r/s | 2.2 hours | $12 |
| Direct API (Tier 1) | 12 | 1.0 r/s | 1.1 hours | $12 |

## Input CSV Requirements
Required columns:
- `firstName` - Contact's first name
- `organization_name` - Company name
- `organization_website_url` - Company website
- `title` - Contact's job title
- `email` - Contact's email

Optional columns:
- `industry` - Industry vertical
- `city` - Location
- `linkedin_url` - LinkedIn profile
- `fundingAmount` - Recent funding

## Output CSV Format
All original columns plus:
- `subject` - Generated subject line
- `emailBody` - Generated email body
- `hooks` - Research findings from Perplexity
- `hookTestFailed` - Whether hook validation failed

## Quality Verification Scripts
- `verify_quality.py` - Checks name/company accuracy in 100 random rows
- `verify_companies.py` - Validates company references
- `check_company_refs.py` - Detailed analysis of 20 rows
- `show_emails.py` - Display sample emails
- `test_perplexity.py` - Test Perplexity API responses

## Error Handling
- **429 Rate Limit:** Automatic retry with exponential backoff
- **402 Payment Required:** Check API credits
- **Hook Validation Failed:** Marks `hookTestFailed=True`, continues
- **Connection Errors:** Retries up to BATCH_RETRIES times

## Environment Setup
1. Install dependencies: `pip install -r requirements.txt`
2. Create `.env` file:
   ```
   OPENAI_API_KEY=sk-proj-...
   PERPLEXITY_API_KEY=pplx-...
   ```
3. Test with small dataset first (10-100 rows)

## Email Types Supported

### 1. Database Reactivation (`updatedprompt.txt`)
- Re-engages inactive leads via SMS campaigns
- References specific company achievements
- Soft call-to-action (Loom video)

### 2. MCA Funding (`fundingprompt.txt`)
- Offers merchant cash advance/funding options
- References company growth indicators
- Multiple lending options (MCAs, term loans, revenue-based)
- Industry context in subject line

### 3. Custom Offers (via Streamlit app)
- Supports any offer type (SEO, consulting, software demos, etc.)
- User-created prompt templates
- Full customization available

## New Features (January 2025)

### Email Formatting (`format_emails.py`)
- Post-processes CSV to add paragraph breaks
- Pattern-based text formatting
- Usage: `python format_emails.py output.csv`

### Web Interface (`streamlit_app.py`)
- Upload CSV and generate emails via web UI
- Custom prompt builder for any offer
- User provides their own API keys
- Deploy on Streamlit Cloud for SaaS

### Fixed Issues
- Pipeline now correctly uses `--prompt` parameter
- Removed all OpenRouter dependencies
- Standardized on gpt-5-mini throughout

## Running Different Email Types

### Database Reactivation
```bash
python -m src.cli --input leads.csv --prompt updatedprompt.txt --output results.csv
```

### MCA Funding
```bash
python -m src.cli --input leads.csv --prompt fundingprompt.txt --output results.csv
python format_emails.py results.csv  # Add paragraph breaks
```

### Custom via Streamlit
```bash
streamlit run streamlit_app.py
```

## Important Notes
- Token counting may underreport actual usage
- Perplexity Tier 1 ($10/mo) recommended for production
- Always test with small batches first
- Monitor costs in real-time via API dashboards
- IMPORTANT: Always use GPT-5-mini model, never any other model
- NO OpenRouter usage - direct APIs only
- For SaaS deployment, see STREAMLIT_DEPLOYMENT.md