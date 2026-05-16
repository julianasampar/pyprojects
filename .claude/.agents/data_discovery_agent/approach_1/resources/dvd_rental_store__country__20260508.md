# Source Discovery: dvd_rental_store__country
**Generated:** 2026-05-08  
**Database:** dbt_database.db (SQLite)  
**Domain:** DVD Rental Store

---

## 1. Exploratory Analysis

### Columns & Data Types
| Column | Type | Nullable |
|--------|------|----------|
| country_id | INTEGER | YES |
| country | TEXT | YES |
| last_update | TEXT | YES |

### Descriptive Metrics
| Metric | Value |
|--------|-------|
| Total rows | 109 |
| Unique country_id | 109 |

---

## 2. Data Structure

### Primary Key
`country_id` — fully unique, 109 distinct values for 109 rows.

### Foreign Keys
None. Top of the geographic hierarchy.

### Orchestration
Full drop-and-recreate on each load.

### Relationships
- `country_id` → **dvd_rental_store__city** (`country_id`): each city belongs to one country.

---

## 3. Data Behavior

- **109 countries** representing the full geographic spread of the DVD rental store's customer base.
- This is the top-level geographic reference table (country → city → address).

---

## 4. Column Descriptions

| Column | Description |
|--------|-------------|
| country_id | Unique integer identifier for each country. Primary key. |
| country | Name of the country (e.g., "Canada", "Australia"). |
| last_update | Timestamp of the last modification to this record. |

## 5. Business Description

The `country` table is the top-level geographic reference in the DVD rental store system. It supports geographic segmentation in analytics (e.g., revenue by country, customer distribution by region). The store's customer base spans 109 countries, reflecting a highly international clientele, which is notable for a physical DVD rental business and suggests this dataset represents a fictional/training scenario (Sakila sample database).
