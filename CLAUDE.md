## Repository Overview
This repository is dedicated to the development of personal data projects. Most of the work in this repository is built using Python and SQL. For each tool used (Airflow, Dagster, dbt, etc), there is a dedicated folder containing the files that run within that tool. 
 
 pyprojects/
- ├── 📁 .github/
- ├── 📁 airflow/
- ├── 📁 dagster/
- ├── 📁 dbt/
- ├── 📁 others/
- ├── 📁 pipelines/
- ├── 📄 .gitignore
- ├── 📄 CLAUDE.md
- ├── 📄 dbt_database.sqbpro
- └── 📄 run_csv.py

  Main project folders:
  - airflow/ — Airflow orchestration
  - dagster/ — Dagster orchestration
  - dbt/ — dbt data transformation
  - pipelines/ — Python scripts 
  - others/ — Folder containing Jupyter notebooks and archived files

  Supporting folders and files:
  - .github/ — Git Hub Workflows
  - .gitignore/ — Git Ignore File
  - run_csv.py/ — Python script for .csv file ingestion in SQLite and DuckDB


### /dbt
Each model contains a `meta` config variable named `database`, which can have one of the following values:

* `sqlite`
* `duckdb`

The value of `meta.database` determines which dbt target must be used when executing the model:

* If `meta.database == 'sqlite'`, run the model using target `sqlite_dev`
* If `meta.database == 'duckdb'`, run the model using target `duckdb_dev`

Before executing any dbt command, first inspect the model configuration and identify the value of `meta.database`. Then select the appropriate target automatically.

Example:

If model `dim_rental_customer` contains:

```jinja
{{ config(
    meta = {
        "database": "sqlite"
    }
) }}
```

then execute:

```bash
dbt build --select dim_rental_customer --target sqlite_dev
```
