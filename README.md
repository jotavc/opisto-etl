# opisto-etl

ETL that syncs a seller’s inventory with **Opisto** (auto-parts marketplace): it extracts vehicle parts data from PostgreSQL, transforms it into a CSV that matches Opisto’s import schema, and uploads it via FTPS.

---

## Features

* **Modular ETL**: `extract/`, `transform/`, `load/`, `pipelines/`
* **SQL pushdown on extract**: explicit columns and filters
* **Deterministic transform**: rename, typing, truncation, defaults, URL cleanup
* **CSV for downstream systems**: `;` separator + comma decimal (e.g., `123,45`)
* **FTPS upload**
* **Orchestration** with Prefect 3 (local API server)
* **Unit tests** for transform and CSV formatting
* **Demo runner** that works with a sample CSV (no DB, no FTP)

---

## Architecture

```
PostgreSQL -> Extract (SQL pushdown) -> Transform (schema/typing) -> CSV -> FTPS
```

---

## Project structure

```
opisto-etl/
├─ src/opisto_etl/
│  ├─ extract/      # PostgreSQL extractor (SQL pushdown)
│  ├─ transform/    # Canonical schema & typing
│  ├─ load/         # CSV & FTPS
│  └─ pipelines/    # Flows & runners (entry points)
├─ tests/           # Unit tests (transform + CSV formatting)
├─ pyproject.toml   # Packaging & deps
├─ .env.example     # Config template
├─ .gitignore
└─ README.md
```

---

## Requirements

* **Python 3.11 or 3.12**
* **Prefect 3.x** (only for Prefect runs)
* **PostgreSQL** (for production run)
* **FTPS** (for production run)

---

## Quickstart

### Setup (once)

```bash
# Create and activate a virtual env
python -m venv .venv
# Windows PowerShell:
.venv\Scripts\Activate.ps1
# macOS/Linux:
# source .venv/bin/activate

# Install project (and dev extras if you want tests)
python -m pip install -U pip
python -m pip install -e ".[dev]"
```

### A) Demo (standalone - no Prefect, no DB, no FTP)

```bash
# Runs with data/sample_source.csv and writes Opisto_demo_*.csv
python -m opisto_etl.pipelines.pipeline_demo_standalone
```

### B) Demo with Prefect 3 (optional)

```bash
# Terminal 1 - start Prefect server (UI: http://127.0.0.1:4200)
prefect server start
```

```bash
# Terminal 2 - point to the local API and run the demo flow
prefect profile use default
prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"
python -m opisto_etl.pipelines.pipeline_demo
```

### C) Production run (DB + FTPS)

```bash
# Terminal 1
prefect server start
```

```bash
# Terminal 2
prefect profile use default
prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"
python -m opisto_etl.pipelines.pipeline_prod
```

---

## Configuration

Copy `.env.example` to `.env` and fill values:

```dotenv
# Database
DB_HOST=localhost
DB_PORT=5432
DB_NAME=db_name
DB_USER=db_user
DB_PASSWORD=db_password

# FTPS
FTP_HOST=ftp_host
FTP_USER=ftp_user
FTP_PASSWORD=ftp_password
FTP_REMOTE_DIR=/
```

---

## Running tests

```bash
pip install -e ".[dev]"
pytest -q
```

---

## Data contract (export CSV)

Final column order (`EXPORT_COLUMNS`):

```
Part_Name, Identifier, Brand, Gam, Model, VIN,
Vehicle_Identifier, Energy, Price, Quantity, Guarantee,
Description, Part_Condition, Licence_Plate, Version,
Finish, GearBox_Type, GearBox_Code, Engine_Code, Color,
Color_Code, Manufacturer_Reference, Shipping_Fees, Circulation_Date,
Mileage, CNIT, Doors_Number, Part_Pictures, Vehicle_Pictures,
KType, KBA_Nummer, Power_HP, Displacement
```

Key rules:

* `Price`: cents -> euros (÷100, 4 decimals)
* Strings trimmed + truncated (e.g., `Part_Name` 70 chars)
* Picture URLs: lists -> comma-joined strings
* `Manufacturer_Reference`: drop `SLV` prefix and trim
* Empty/optional fields as empty strings for compatibility

---

## License

**MIT — see LICENSE for details**
