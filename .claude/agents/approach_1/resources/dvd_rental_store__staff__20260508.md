# Source Discovery: dvd_rental_store__staff
**Generated:** 2026-05-08  
**Database:** dbt_database.db (SQLite)  
**Domain:** DVD Rental Store

---

## 1. Exploratory Analysis

### Columns & Data Types
| Column | Type | Nullable |
|--------|------|----------|
| staff_id | INTEGER | YES |
| first_name | TEXT | YES |
| last_name | TEXT | YES |
| address_id | INTEGER | YES |
| email | TEXT | YES |
| store_id | INTEGER | YES |
| active | TEXT | YES |
| username | TEXT | YES |
| password | TEXT | YES |
| last_update | TEXT | YES |
| picture | TEXT | YES |

### Descriptive Metrics
| Metric | Value |
|--------|-------|
| Total rows | 2 |
| Unique staff_id | 2 |
| Active staff (active='t') | 2 |
| Stores covered | 2 |

---

## 2. Data Structure

### Primary Key
`staff_id` — fully unique, 2 distinct values.

### Foreign Keys
- `store_id` → **dvd_rental_store__store** (`store_id`): the store this staff member is assigned to.
- `address_id` → **dvd_rental_store__address** (`address_id`): the staff member's address.

### Categorical Values
- `active`: TEXT type, value `'t'` (true) for both staff members. Both are active.

### Orchestration
Full drop-and-recreate on each load. Near-static table.

### Relationships
- `staff_id` → **dvd_rental_store__rental** (`staff_id`): all rentals processed by this staff member.
- `staff_id` → **dvd_rental_store__payment** (`staff_id`): all payments collected by this staff member.
- `staff_id` → **dvd_rental_store__store** (`manager_staff_id`): each staff member manages one store.

---

## 3. Data Behavior

- Only **2 staff members** exist in the system, one per store.
  - **Staff 1 (Mike Hillyer)**: Store 1 manager, Lethbridge, Canada.
  - **Staff 2 (Jon Stephens)**: Store 2 manager, Woodridge, Australia.
- Both staff members are also the managers of their respective stores.
- Both staff handled roughly equal volumes of rentals and payments.
- `picture` column is present but NULL for all records in this dataset.
- `password` is stored — downstream models should never expose this column.

---

## 4. Column Descriptions

| Column | Description |
|--------|-------------|
| staff_id | Unique integer identifier for each staff member. Primary key. |
| first_name | Staff member's first name. |
| last_name | Staff member's last name (surname). |
| address_id | Foreign key to `dvd_rental_store__address`. The staff member's address. |
| email | Staff member's email address. |
| store_id | Foreign key to `dvd_rental_store__store`. The store where the staff member is assigned. |
| active | Activation status of the staff account: 't' = active, 'f' = inactive. Stored as TEXT. |
| username | Login username for the staff member's system account. |
| password | Hashed password for the staff member's system account. **Sensitive — exclude from downstream models.** |
| last_update | Timestamp of the last modification to this record. |
| picture | Binary picture/photo of the staff member. NULL in this dataset. |

## 5. Business Description

The `staff` table contains records for the two employees who manage and operate the two DVD rental store locations. Each staff member is also the manager of their assigned store. Staff members appear in rental and payment records as the processing agent, enabling per-staff performance analytics (e.g., number of rentals processed, revenue collected). The `password` column is a sensitive credential that should be masked or excluded in all analytical models.
