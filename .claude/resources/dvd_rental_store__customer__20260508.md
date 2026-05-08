# Source Discovery: dvd_rental_store__customer
**Generated:** 2026-05-08  
**Database:** dbt_database.db (SQLite)  
**Domain:** DVD Rental Store

---

## 1. Exploratory Analysis

### Columns & Data Types
| Column | Type | Nullable |
|--------|------|----------|
| customer_id | INTEGER | YES |
| store_id | INTEGER | YES |
| first_name | TEXT | YES |
| last_name | TEXT | YES |
| email | TEXT | YES |
| address_id | INTEGER | YES |
| activebool | TEXT | YES |
| create_date | TEXT | YES |
| last_update | TEXT | YES |
| active | INTEGER | YES |

### Descriptive Metrics
| Metric | Value |
|--------|-------|
| Total rows | 599 |
| Unique customer_id | 599 |
| Active customers (active=1) | 584 |
| Inactive customers (active=0) | 15 |
| Distinct stores | 2 |
| Earliest create_date | 2006-02-14 |
| Latest create_date | 2006-02-14 |

---

## 2. Data Structure

### Primary Key
`customer_id` — fully unique across 599 rows.

### Foreign Keys
- `store_id` → **dvd_rental_store__store** (`store_id`): the store where the customer registered.
- `address_id` → **dvd_rental_store__address** (`address_id`): the customer's mailing address.

### Categorical Values
- `activebool`: only `'t'` observed — even for inactive customers (see Data Behavior).
- `active`: `1` (active) or `0` (inactive).
- `store_id`: `1` or `2`.

### Orchestration
`create_date` is uniform (`2006-02-14`) across all customers, indicating a one-time bulk load.  
`last_update` reflects individual-level updates. Full drop-and-recreate strategy likely used.

### Relationships
- `customer_id` → **dvd_rental_store__rental** (`customer_id`): all rentals made by this customer.
- `customer_id` → **dvd_rental_store__payment** (`customer_id`): all payments made by this customer.

---

## 3. Data Behavior

- All 599 customers were created on the same date (`2006-02-14`), suggesting a single bulk registration event (data migration or fictional seeding).
- **activebool vs active discrepancy**: `activebool` is `'t'` for ALL 599 customers — including the 15 who have `active = 0`. The `activebool` column appears to be a stale boolean flag that was not updated when customers were deactivated. **`active` (integer) should be treated as the authoritative activation status.**
- The top renting customer (customer_id=148, Eleanor Hunt, Saint-Denis, Réunion) made **46 rentals**.

---

## 4. Column Descriptions

| Column | Description |
|--------|-------------|
| customer_id | Unique integer identifier for each customer. Primary key. |
| store_id | Foreign key to `dvd_rental_store__store`. Indicates the store the customer is primarily associated with (their sign-up store). |
| first_name | Customer's first name. |
| last_name | Customer's last name (surname). |
| email | Customer's email address. |
| address_id | Foreign key to `dvd_rental_store__address`. The customer's registered mailing address. |
| activebool | Boolean flag for customer activation status, stored as TEXT ('t'/'f'). **Data quality issue: always 't' even for inactive customers. Do not use as the authoritative active flag.** |
| create_date | Date when the customer account was created. Uniform across all rows (2006-02-14). |
| last_update | Timestamp of the last modification to this record. |
| active | Integer flag for customer activation status: 1 = active, 0 = inactive. **Use this column as the authoritative activation flag** (15 inactive customers). |

## 5. Business Description

The `customer` table is the core customer dimension for the DVD rental store. It records the 599 registered customers, their store affiliation, contact details, geographic address, and activation status. A key data quality finding is that the `activebool` column is unreliable — it shows all customers as active even when `active = 0` — so the integer `active` column must be used for filtering active vs. inactive customers. All customers were registered on the same date, suggesting this is a historical snapshot from a data migration event.
