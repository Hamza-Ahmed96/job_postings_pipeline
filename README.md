# Job Postings Pipeline

An end-to-end ELT pipeline that extracts job postings from the Adzuna API, loads raw data into Snowflake and MotherDuck, and transforms it into a star schema using dbt. The pipeline is controllable via a custom MCP server, allowing Claude to run extractions and loads directly through natural language.

---

## Architecture

```
Adzuna API
    │
    ▼
Extract (Python)
    │
    ├──► Raw JSON files (local)
    │
    ▼
Load (Python)
    │
    ├──► Snowflake  RAW.JOB_POSTINGS
    └──► MotherDuck data_jobs_adzuna.raw.job_postings
    │
    ▼
Transform (dbt)
    │
    ├──► Staging        stg_job_postings
    ├──► Dimensions     dim_company, dim_location, dim_date
    ├──► Fact           fact_job_postings
    └──► Marts          mart_salary_by_role, mart_remote_jobs, mart_hiring_trends
```

**Pattern: ELT** — data is loaded raw first, then transformed inside the warehouse. This preserves the original data and keeps transformation logic version-controlled in dbt.

---

## Stack

| Tool | Purpose |
|---|---|
| Python | Extract and load scripts |
| Adzuna API | Source of job postings data |
| Snowflake | Primary cloud data warehouse |
| MotherDuck | Serverless DuckDB cloud warehouse |
| dbt (dbt-snowflake) | Transformation layer — staging, dimensions, facts, marts |
| FastMCP | MCP server exposing pipeline tools to Claude |
| pandas | Data manipulation and loading |
| loguru | Logging |
| python-dotenv | Environment variable management |
| uv | Python package and environment management |

---

## Project Structure

```
job_postings_pipeline/
├── ingestion/
│   ├── extract.py          # Fetch from Adzuna API, paginate, save raw JSON
│   ├── load_snowflake.py   # Load raw JSON → Snowflake RAW.JOB_POSTINGS
│   ├── load_duckdb.py      # Load raw JSON → MotherDuck raw.job_postings
│   └── transform.py        # Legacy ETL field mapping (superseded by dbt)
├── mcpserver/
│   └── server.py           # MCP server — exposes pipeline tools to Claude
├── dbt_project/
│   ├── models/
│   │   ├── staging/        # stg_job_postings.sql
│   │   ├── dimensions/     # dim_company, dim_location, dim_date
│   │   ├── facts/          # fact_job_postings
│   │   └── marts/          # mart_salary_by_role, mart_remote_jobs, mart_hiring_trends
│   └── sources.yml
├── utils/
│   ├── useful_functions.py # handle_exceptions decorator
│   └── my_exception.py     # CustomException class
├── raw/                    # Raw JSON files from Adzuna (gitignored)
├── notes/                  # Developer notes
├── pyproject.toml
└── .env                    # API keys and credentials (gitignored)
```

---

## Setup

### Prerequisites
- Python 3.12+
- [uv](https://docs.astral.sh/uv/) installed
- Snowflake account
- MotherDuck account
- Adzuna API credentials

### Install dependencies

```bash
uv sync
```

### Configure environment variables

Create a `.env` file in the project root:

```env
ADZUNA_APP_ID=your_app_id
ADZUNA_APP_KEY=your_app_key

user=your_snowflake_user
password=your_snowflake_password
account=your_snowflake_account
warehouse=JOB_POSTINGS_WH
database=JOB_POSTINGS
schema=RAW

MOTHERDUCK_TOKEN=your_motherduck_token
```

---

## Running the Pipeline

### Manually

```bash
# Step 1 — Extract from Adzuna API
uv run ingestion/extract.py

# Step 2a — Load to Snowflake
uv run ingestion/load_snowflake.py

# Step 2b — Load to MotherDuck
uv run ingestion/load_duckdb.py
```

### Via MCP (Claude Code)

The pipeline is registered as an MCP server with Claude Code. Claude can run any of these tools directly:

| Tool | Description |
|---|---|
| `fetch_jobs` | Fetch job postings from Adzuna for given countries and job titles |
| `load_to_snowflake` | Load raw JSON files into Snowflake |
| `load_to_motherduck` | Load raw JSON files into MotherDuck |
| `run_full_pipeline` | Fetch + load to MotherDuck in one step |

**Register the MCP server:**

```bash
claude mcp add --transport stdio adzuna-pipeline -- uv run mcpserver/server.py
```

---

## Data

### Source

- **API:** [Adzuna](https://developer.adzuna.com/)
- **Countries:** `gb`
- **Job titles:** `data engineer`, `data scientist`, `data analyst`, `software engineer`, `finance analyst`
- **Pages per search:** capped at 10 (500 results max per job title)

### Staging Table Schema

Both Snowflake (`RAW.JOB_POSTINGS`) and MotherDuck (`data_jobs_adzuna.raw.job_postings`) use the same schema:

| Column | Type | Description |
|---|---|---|
| id | VARCHAR (PK) | Adzuna job ID |
| job_title_short | VARCHAR | Search term used to find this job |
| title | VARCHAR | Full job title from the posting |
| job_posted_date | DATETIME | When the job was posted |
| company_name | VARCHAR | Hiring company |
| location | VARCHAR | Job location |
| salary_min | FLOAT | Minimum advertised salary |
| salary_max | FLOAT | Maximum advertised salary |
| contract_type | VARCHAR | e.g. full_time, part_time |
| description | VARCHAR | Full job description text |
| load_date | DATETIME | When the record was loaded into the warehouse |

> Each pipeline run truncates and reloads the staging table — this is a snapshot pipeline, not an append pipeline.

---

## dbt Transformation Layer

The dbt project transforms raw staging data into a star schema optimised for analysis.

### Staging

`stg_job_postings` — cleans and casts types from `RAW.JOB_POSTINGS`. Handles nulls, standardises column names, and parses dates.

### Dimensions

| Model | Description |
|---|---|
| `dim_company` | Unique companies with surrogate keys |
| `dim_location` | Unique locations |
| `dim_date` | Date dimension with week, month, year attributes |

### Fact

`fact_job_postings` — one row per job posting, with foreign keys to all dimensions and salary metrics.

### Marts

| Model | Description |
|---|---|
| `mart_salary_by_role` | Average, min, max salary broken down by job title |
| `mart_remote_jobs` | Remote vs on-site job distribution by role and location |
| `mart_hiring_trends` | Posting volume over time by role |

### Running dbt

```bash
cd dbt_project

# Run all models
dbt run

# Test all models
dbt test

# Generate and serve docs
dbt docs generate
dbt docs serve
```

---

## MCP Server

The MCP server (`mcpserver/server.py`) exposes the pipeline as tools that Claude Code can call directly via natural language.

Built with [FastMCP](https://github.com/jlowin/fastmcp). Uses `stdio` transport — Claude Code spawns the server as a subprocess automatically on startup.

**Key design decisions:**
- `load_to_snowflake` and `load_to_motherduck` are independent — you can load to either or both without running both
- `run_full_pipeline` defaults to MotherDuck; use `fetch_jobs` + `load_to_snowflake` separately for Snowflake

---

## Notes

- `raw/` is gitignored — raw JSON files are not committed
- `.env` is gitignored — never commit credentials
- Duplicate job IDs across search results are handled with `drop_duplicates(subset="id")` before loading
- The pipeline is a snapshot (truncate + reload) — historical data is not retained across runs
