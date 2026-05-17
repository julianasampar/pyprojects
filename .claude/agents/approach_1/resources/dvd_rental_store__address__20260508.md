# Source Discovery: dvd_rental_store__address
**Generated:** 2026-05-08  
**Database:** dbt_database.db (SQLite)  
**Domain:** DVD Rental Store

---

## 1. Exploratory Analysis

### Columns & Data Types
| Column | Type | Nullable |
|--------|------|----------|
| address_id | INTEGER | YES |
| address | TEXT | YES |
| address2 | REAL | YES |
| district | TEXT | YES |
| city_id | INTEGER | YES |
| postal_code | REAL | YES |
| phone | REAL | YES |
| last_update | TEXT | YES |

### Descriptive Metrics
| Metric | Value |
|--------|-------|
| Total rows | 603 |
| Unique address_id | 603 |
| Non-null address2 | 0 (entirely NULL) |
| Non-null postal_code | 599 |
| Non-null phone | 601 |

---

## 2. Data Structure

### Primary Key
`address_id` — fully unique across all 603 rows.

### Foreign Keys
- `city_id` → **dvd_rental_store__city** (`city_id`): links address to its city.

### Categorical Values
- `district`: free-text field representing a state/region/district name.
- `address2`: completely NULL in the dataset — effectively unused.

### Orchestration
`last_update` present; all rows loaded in a single batch (full drop-and-recreate pattern based on actor/category tables' behaviour).

### Relationships
- `address_id` is referenced by:
  - **dvd_rental_store__customer** (`address_id`): links each customer to their mailing address.
  - **dvd_rental_store__staff** (`address_id`): links each staff member to their work address.
  - **dvd_rental_store__store** (`address_id`): links each store to its physical address.

---

## 3. Data Behavior

- **603 addresses** serve customers (599), staff (2), and stores (2).
- `address2` is entirely NULL — it was likely reserved for a secondary line (e.g., suite/apt number) but never populated.
- `postal_code` and `phone` are stored as REAL (float) instead of TEXT, which is unusual and may cause leading-zero truncation for some postal codes/phone numbers.
- 4 records missing `postal_code`; 2 records missing `phone`.

---

## 4. Column Descriptions

| Column | Description |
|--------|-------------|
| address_id | Unique integer identifier for each address. Primary key. |
| address | Primary street address line (e.g., "1952 Pune Lane"). |
| address2 | Secondary address line (e.g., apartment or suite number). Entirely NULL in this dataset. |
| district | State, province, or district within the country. |
| city_id | Foreign key referencing `dvd_rental_store__city`. Links the address to a specific city. |
| postal_code | ZIP or postal code for the address. Stored as REAL — caution with leading zeros. |
| phone | Contact phone number associated with the address. Stored as REAL — may lose leading zeros. |
| last_update | Timestamp of the last modification to this record. |

## 5. Business Description

The `address` table is a shared reference table that stores physical addresses for all entities in the DVD rental store system: customers, staff members, and store locations. It follows a normalised design, linking to `city` and through it to `country`. A key data quality concern is that `address2`, `postal_code`, and `phone` are stored as numeric REAL types, which is semantically incorrect and may cause data loss for values that start with zero.
