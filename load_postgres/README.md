# load_postgres

ETL environment for migrating data from **Oracle** to **PostgreSQL**.

## Pipeline

```
Oracle DB  →  Extract  →  Transform  →  Load  →  PostgreSQL
```

## Directory structure

```
load_postgres/
├── config/
│   └── config.ini       # Connection and logging settings
├── logs/                # Runtime log files (git-ignored)
├── src/
│   ├── extract/
│   │   └── __init__.py  # Oracle extraction logic
│   ├── transform/
│   │   └── __init__.py  # Data transformation and hashing
│   └── load/
│       └── __init__.py  # PostgreSQL loading logic (SQLAlchemy)
├── tests/
│   └── __init__.py      # Unit tests
├── main.py              # Pipeline entry point
├── requirements.txt     # Python dependencies
└── README.md
```

## Setup

### 1. Create and activate the virtual environment

```bash
cd load_postgres
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

Edit `config/config.ini` and fill in your Oracle and PostgreSQL credentials.

### 4. Run the pipeline

```bash
python main.py
```

## Libraries used

| Library | Purpose |
|---|---|
| `oracledb` | Oracle database connection |
| `pandas` | Data manipulation |
| `psycopg2-binary` | PostgreSQL driver |
| `SQLAlchemy` | ORM / connection layer for PostgreSQL |
| `hashlib` | Column hashing (built-in) |
| `logging` | Structured logging (built-in) |
