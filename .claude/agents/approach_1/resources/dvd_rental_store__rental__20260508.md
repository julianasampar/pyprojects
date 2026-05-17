# Source Discovery: dvd_rental_store__rental
**Generated:** 2026-05-08  
**Database:** dbt_database.db (SQLite)  
**Domain:** DVD Rental Store

---

## 1. Exploratory Analysis

### Columns & Data Types
| Column | Type | Nullable |
|--------|------|----------|
| rental_id | INTEGER | YES |
| rental_date | TEXT | YES |
| inventory_id | INTEGER | YES |
| customer_id | INTEGER | YES |
| return_date | TEXT | YES |
| staff_id | INTEGER | YES |
| last_update | TEXT | YES |

### Descriptive Metrics
| Metric | Value |
|--------|-------|
| Total rows | 16,044 |
| Unique rental_id | 16,044 |
| Unique customers | 599 |
| Unique inventory items rented | 4,580 |
| Unique staff | 2 |
| Non-null return_date | 15,862 |
| NULL return_date (open rentals) | 182 |
| Earliest rental_date | 2005-05-24 |
| Latest rental_date | 2006-02-14 |

### Rental Volume by Month
| Month | Rentals |
|-------|---------|
| 2005-05 | 1,156 |
| 2005-06 | 2,311 |
| 2005-07 | 6,709 |
| 2005-08 | 5,686 |
| 2006-02 | 182 |

---

## 2. Data Structure

### Primary Key
`rental_id` — fully unique across all 16,044 rows.

### Foreign Keys
- `inventory_id` → **dvd_rental_store__inventory** (`inventory_id`): the specific physical copy that was rented.
- `customer_id` → **dvd_rental_store__customer** (`customer_id`): the customer who rented the film.
- `staff_id` → **dvd_rental_store__staff** (`staff_id`): the staff member who processed the rental.

### Orchestration
Date range spans 2005-05 to 2006-02. The table appears to be a historical snapshot. Rentals peak in July–August 2005. The last 182 rentals (all dated 2006-02-14) have no return date, suggesting they represent the state of open/active rentals at snapshot time.

### Relationships
- `rental_id` → **dvd_rental_store__payment** (`rental_id`): the payment(s) collected for this rental.

---

## 3. Data Behavior

- **16,044 total rentals** by all 599 customers. Average: ~26.8 rentals per customer.
- **Top customer**: customer_id=148 made 46 rentals.
- **Rental duration**: Most returns occur within 1–9 days, consistent with the 3–7 day rental window defined in the film table (returns slightly past deadline suggest some late returns).
- **182 open rentals** (NULL `return_date`) all share `rental_date = 2006-02-14`, which is the last date in the dataset. These represent DVDs not yet returned at the time of the data snapshot — not missing data.
- **1,452 rentals have no associated payment** — mostly from the earliest batch (May 2005). This appears to be a data completeness issue in the payments extract, not necessarily unpaid rentals.
- **Payment dates are in 2007**, while rental dates are in 2005–2006. There is an ~18-month gap between when rentals occurred and when payments were recorded. **`payment_date` should not be used to infer when a rental transaction occurred.**
- Peak rental activity was in **July 2005** (6,709 rentals), dropping to near-zero by February 2006.

---

## 4. Column Descriptions

| Column | Description |
|--------|-------------|
| rental_id | Unique integer identifier for each rental transaction. Primary key. |
| rental_date | Timestamp when the DVD was checked out by the customer. |
| inventory_id | Foreign key to `dvd_rental_store__inventory`. The specific physical DVD copy that was rented. |
| customer_id | Foreign key to `dvd_rental_store__customer`. The customer who performed the rental. |
| return_date | Timestamp when the DVD was returned to the store. NULL for rentals that had not yet been returned at the time of the data snapshot (182 open rentals, all dated 2006-02-14). |
| staff_id | Foreign key to `dvd_rental_store__staff`. The staff member who processed the rental check-out. |
| last_update | Timestamp of the last modification to this record. |

## 5. Business Description

The `rental` table is the core transactional table of the DVD rental store, recording every instance of a customer checking out a DVD. It links together inventory (which physical copy), customer (who rented it), staff (who processed it), and timing (when rented and returned). Key analytical uses include: rental volume trends over time, customer rental frequency, per-film popularity (via inventory), and staff performance. Analysts must be aware that: (1) 182 open rentals lack return dates (they are legitimately still out as of the snapshot date), (2) 1,452 rentals lack payment records (data completeness issue in early May 2005), and (3) payment dates are recorded ~18 months after rental dates, so payment timing does not reflect rental activity timing.
