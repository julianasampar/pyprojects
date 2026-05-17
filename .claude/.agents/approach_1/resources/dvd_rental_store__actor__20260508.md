# Source Discovery: dvd_rental_store__actor
**Generated:** 2026-05-08  
**Database:** dbt_database.db (SQLite)  
**Domain:** DVD Rental Store

---

## 1. Exploratory Analysis

### Columns & Data Types
| Column | Type | Nullable |
|--------|------|----------|
| actor_id | INTEGER | YES |
| first_name | TEXT | YES |
| last_name | TEXT | YES |
| last_update | TEXT | YES |

### Descriptive Metrics
| Metric | actor_id | first_name | last_name | last_update |
|--------|----------|------------|-----------|-------------|
| count | 200 | 200 | 200 | 200 |
| unique | 200 | — | — | 1 |
| min | 1 | — | — | 2013-05-26 14:47:57.62 |
| max | 200 | — | — | 2013-05-26 14:47:57.62 |

---

## 2. Data Structure

### Primary Key
`actor_id` — fully unique, 200 distinct values for 200 rows.

### Foreign Keys
None. This is a dimension/reference table.

### Categorical Values
No non-key categorical columns other than free-text names.

### Orchestration
`last_update` holds a single timestamp across all rows (`2013-05-26 14:47:57.62`), indicating a **full-load drop-and-recreate** strategy. The entire table was loaded at once with no incremental logic.

### Relationships
- `actor_id` → **dvd_rental_store__film_actor** (`actor_id`): allows joining each actor to the films they appeared in.

---

## 3. Data Behavior

- **200 actors** in total, each with a unique `actor_id`.
- All records share the same `last_update`, confirming a single bulk load.
- Names are stored as separate `first_name` and `last_name` columns (no combined field).
- No missing values observed in any column.

---

## 4. Column Descriptions

| Column | Description |
|--------|-------------|
| actor_id | Unique integer identifier for each actor. Serves as the primary key. |
| first_name | First name of the actor. |
| last_name | Last name (surname) of the actor. |
| last_update | Timestamp of the last modification to this record. All rows share the same value, reflecting the date of the bulk data load. |

## 5. Business Description

The `actor` table is a reference/dimension table that catalogues all actors available in the DVD rental store's film catalogue. It is used primarily to support queries about which actors appear in which films, enabling customer-facing features like "browse films by actor" or analytics such as "most featured actors in the catalogue." With 200 actors, this represents the complete cast catalogue of the store's 1,000-film library.
