# Data Profiling Tool

This Python set performs automated data profiling for `.csv` files using DuckDB. The goal is to generate a structured JSON output containing metadata, descriptive statistics, and categorical insights for each table loaded from the source files.

## Workflow

### 1. Columns and Data Types

For each table created from the CSV files, retrieve:

* column names
* inferred data types

---

### 2. Descriptive Metrics

For every column in each table, compute profiling metrics according to the column type.

#### Numeric Columns

Collect:

* `count`: number of non-null values
* `mean`: average value
* `std`: standard deviation
* `min`: minimum value
* `25%`: first quartile
* `50%`: median
* `75%`: third quartile
* `max`: maximum value

#### Object / Categorical Columns

Collect:

* `count`: number of non-null values
* `unique`: number of distinct values
* `top`: most frequent value
* `freq`: frequency of the most frequent value

---

### 3. Categorical Values

For each non-key categorical column, retrieve the distinct possible values present in the data.

Primary keys and identifier-like columns should be excluded from this step to avoid generating excessively large outputs.

---

## Output

The functions generate a JSON structure containing:

* table metadata
* column data types
* descriptive statistics
* categorical distributions
* distinct categorical values

This output can be used for:

* exploratory data analysis (EDA)
* data quality validation
* schema understanding
* downstream transformation planning
* automated documentation

## Configuration
1. Create local venv

2. Install dependencies
` pip install -r requirements.txt `


2. Add local .env file with correct credentials

### SNOWFLAKE
```
SNOWFLAKE_USER="<YOUR_USER>"
SNOWFLAKE_ACCOUNT="<YOUR_ACCOUNT>"
SNOWFLAKE_PASSWORD="<YOUR_PASSWORD>"
SNOWFLAKE_WAREHOUSE="<YOUR_WAREHOUSE>"
SNOWFLAKE_DATABASE="<YOUR_DATABASE>"
SNOWFLAKE_SCHEMA="<YOUR_SCHEMA>"
```

### AWS
```
AWS_ACCESS_KEY_ID="<YOUR_ACCESS_KEY_ID>"
AWS_SECRET_ACCESS_KEY=<YOUR_SECRET_ACCESS_KEY>"
AWS_REGION=<YOUR_BUCKET_REGION>"
S3_BUCKET_NAME=<YOUR_BUCKET_NAME>"
```