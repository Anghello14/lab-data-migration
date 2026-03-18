# write_csv

ETL environment for extracting data from **Oracle** and writing it to **CSV files**.

## Pipeline

```
Oracle DB  →  Extract  →  Transform  →  Load  →  CSV File
```

## Directory structure

```
write_csv/
├── config/
│   └── config.ini       # Connection, CSV output and logging settings
├── logs/                # Runtime log files (git-ignored)
├── output/              # Generated CSV files (git-ignored)
├── src/
│   ├── extract/
│   │   └── __init__.py  # Oracle extraction logic
│   ├── transform/
│   │   └── __init__.py  # Data transformation and hashing
│   └── load/
│       └── __init__.py  # CSV writing logic
├── tests/
│   └── __init__.py      # Unit tests
├── main.py              # Pipeline entry point
├── requirements.txt     # Python dependencies
└── README.md
```

## Setup

### 1. Create and activate the virtual environment

```bash
cd write_csv
python3 -m venv venv
source venv/bin/activate        # Linux / macOS
# venv\Scripts\activate.bat    # Windows
```

### 2. Install dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

### 3. Configure connections

Edit `config/config.ini` and fill in your Oracle credentials and CSV output settings.

### 4. Run the pipeline

```bash
python main.py
```

## Libraries used

| Library | Purpose |
|---|---|
| `oracledb` | Oracle database connection |
| `pandas` | Data manipulation and CSV writing |
| `psycopg2-binary` | PostgreSQL driver (available if needed) |
| `SQLAlchemy` | ORM / connection layer (available if needed) |
| `hashlib` | Column hashing (built-in) |
| `logging` | Structured logging (built-in) |
