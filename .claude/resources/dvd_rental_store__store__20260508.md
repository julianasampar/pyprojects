# Source Discovery: dvd_rental_store__store
**Generated:** 2026-05-08  
**Database:** dbt_database.db (SQLite)  
**Domain:** DVD Rental Store

---

## 1. Exploratory Analysis

### Columns & Data Types
| Column | Type | Nullable |
|--------|------|----------|
| store_id | INTEGER | YES |
| manager_staff_id | INTEGER | YES |
| address_id | INTEGER | YES |
| last_update | TEXT | YES |

### Descriptive Metrics
| Metric | Value |
|--------|-------|
| Total rows | 2 |
| Unique store_id | 2 |

---

## 2. Data Structure

### Primary Key
`store_id` — fully unique, 2 distinct values.

### Foreign Keys
- `manager_staff_id` → **dvd_rental_store__staff** (`staff_id`): the staff member who manages this store.
- `address_id` → **dvd_rental_store__address** (`address_id`): the physical location of the store.

### Orchestration
Full drop-and-recreate on each load. Static reference table — rarely changes.

### Relationships
- `store_id` → **dvd_rental_store__customer** (`store_id`): customers affiliated with this store.
- `store_id` → **dvd_rental_store__staff** (`store_id`): staff who work at this store.
- `store_id` → **dvd_rental_store__inventory** (`store_id`): physical DVD inventory held at this store.

---

## 3. Data Behavior

- The business operates **2 stores**:
  - **Store 1**: 47 MySakila Drive, Lethbridge, **Canada** — managed by Mike Hillyer.
  - **Store 2**: 28 MySQL Boulevard, Woodridge, **Australia** — managed by Jon Stephens.
- Inventory is split near-evenly: Store 1 has **2,270** items; Store 2 has **2,311** items.

---

## 4. Column Descriptions

| Column | Description |
|--------|-------------|
| store_id | Unique integer identifier for each store. Primary key. |
| manager_staff_id | Foreign key to `dvd_rental_store__staff`. The staff member designated as store manager. |
| address_id | Foreign key to `dvd_rental_store__address`. The physical address of the store. |
| last_update | Timestamp of the last modification to this record. |

## 5. Business Description

The `store` table describes the two physical DVD rental store locations. The chain has one store in Lethbridge, Canada and another in Woodridge, Australia — each with its own manager, inventory, and customer base. Despite serving customers across 109 countries, all operational activity (inventory, rentals, payments, and staffing) is managed through these two stores. This table is the anchor for store-level aggregations in analytics (e.g., revenue per store, inventory turnover per location).
