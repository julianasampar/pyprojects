# Source Discovery: dvd_rental_store__city
**Generated:** 2026-05-08  
**Database:** dbt_database.db (SQLite)  
**Domain:** DVD Rental Store

---

## 1. Exploratory Analysis

### Columns & Data Types
| Column | Type | Nullable |
|--------|------|----------|
| city_id | INTEGER | YES |
| city | TEXT | YES |
| country_id | INTEGER | YES |
| last_update | TEXT | YES |

### Descriptive Metrics
| Metric | Value |
|--------|-------|
| Total rows | 600 |
| Unique city_id | 600 |
| Unique countries referenced | 109 |

---

## 2. Data Structure

### Primary Key
`city_id` — fully unique across all 600 rows.

### Foreign Keys
- `country_id` → **dvd_rental_store__country** (`country_id`): links each city to its parent country.

### Orchestration
Full drop-and-recreate on each load.

### Relationships
- `city_id` → **dvd_rental_store__address** (`city_id`): allows resolving the city for any address in the system.

---

## 3. Data Behavior

- **600 cities** span **109 countries**, yielding an average of ~5.5 cities per country.
- The city data supports the geographic normalisation of the address system: country → city → address.

---

## 4. Column Descriptions

| Column | Description |
|--------|-------------|
| city_id | Unique integer identifier for each city. Primary key. |
| city | Name of the city. |
| country_id | Foreign key referencing `dvd_rental_store__country`. Links the city to its country. |
| last_update | Timestamp of the last modification to this record. |

## 5. Business Description

The `city` table is a geographic reference table that sits between `country` and `address` in the location hierarchy. It enables full geographic resolution for any customer, staff member, or store: from street address up through city, district, and country. The dataset covers 600 cities across 109 countries, reflecting the global customer base of the DVD rental store.
