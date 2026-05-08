# Source Discovery: dvd_rental_store__language
**Generated:** 2026-05-08  
**Database:** dbt_database.db (SQLite)  
**Domain:** DVD Rental Store

---

## 1. Exploratory Analysis

### Columns & Data Types
| Column | Type | Nullable |
|--------|------|----------|
| language_id | INTEGER | YES |
| name | TEXT | YES |
| last_update | TEXT | YES |

### Descriptive Metrics
| Metric | Value |
|--------|-------|
| Total rows | 6 |
| Unique language_id | 6 |

---

## 2. Data Structure

### Primary Key
`language_id` — fully unique, 6 distinct values for 6 rows.

### Foreign Keys
None. Reference/lookup table.

### Categorical Values (name)
English, French, German, Italian, Japanese, Mandarin  
*(Note: names are right-padded with spaces to a fixed length)*

### Orchestration
Full drop-and-recreate on each load.

### Relationships
- `language_id` → **dvd_rental_store__film** (`language_id`): every film has a language assigned.

---

## 3. Data Behavior

- Only **6 languages** are available in the system.
- In practice, **all 1,000 films use English** as their language — the other 5 languages (French, German, Italian, Japanese, Mandarin) exist in the reference table but are unused in the film catalogue.
- Language names contain trailing spaces (fixed-width character storage). Downstream models should apply `TRIM()` when joining or displaying language names.

---

## 4. Column Descriptions

| Column | Description |
|--------|-------------|
| language_id | Unique integer identifier for each language. Primary key. |
| name | Name of the language (e.g., "English", "French"). Note: values are right-padded with whitespace characters. |
| last_update | Timestamp of the last modification to this record. |

## 5. Business Description

The `language` table defines the possible spoken/audio languages for films in the DVD rental store catalogue. Although the system supports 6 languages, the entire current film catalogue is in English. The remaining language entries (French, German, Italian, Japanese, Mandarin) may represent future expansion capacity or original language metadata fields that were never fully populated in this dataset. Downstream analysts should apply TRIM() on the `name` column to avoid whitespace-related join or filter issues.
