# Source Discovery: dvd_rental_store__inventory
**Generated:** 2026-05-08  
**Database:** dbt_database.db (SQLite)  
**Domain:** DVD Rental Store

---

## 1. Exploratory Analysis

### Columns & Data Types
| Column | Type | Nullable |
|--------|------|----------|
| inventory_id | INTEGER | YES |
| film_id | INTEGER | YES |
| store_id | INTEGER | YES |
| last_update | TEXT | YES |

### Descriptive Metrics
| Metric | Value |
|--------|-------|
| Total rows | 4,581 |
| Unique inventory_id | 4,581 |
| Unique films with inventory | 958 |
| Films without inventory | 42 |
| Store 1 inventory items | 2,270 |
| Store 2 inventory items | 2,311 |

---

## 2. Data Structure

### Primary Key
`inventory_id` — fully unique across all 4,581 rows.

### Foreign Keys
- `film_id` → **dvd_rental_store__film** (`film_id`): the film this physical copy belongs to.
- `store_id` → **dvd_rental_store__store** (`store_id`): the store location where this copy is held.

### Orchestration
Full drop-and-recreate on each load.

### Relationships
- `inventory_id` → **dvd_rental_store__rental** (`inventory_id`): every rental event links to a specific physical copy.

---

## 3. Data Behavior

- 4,581 physical DVD copies spread across 2 stores for 958 distinct film titles.
- **42 films from the catalogue have no physical copies** — they cannot be rented.
- Average of ~4.78 copies per stocked film title (4,581 ÷ 958).
- Inventory is balanced nearly evenly between stores (2,270 vs. 2,311).
- Each rental in `dvd_rental_store__rental` references a specific `inventory_id`, meaning the system tracks which physical disc was rented — enabling per-copy utilisation analytics.

---

## 4. Column Descriptions

| Column | Description |
|--------|-------------|
| inventory_id | Unique integer identifier for each physical DVD copy. Primary key. Each row represents one physical disc. |
| film_id | Foreign key to `dvd_rental_store__film`. Identifies which film title this physical copy corresponds to. |
| store_id | Foreign key to `dvd_rental_store__store`. Identifies the store location where this DVD copy is stocked. |
| last_update | Timestamp of the last modification to this record. |

## 5. Business Description

The `inventory` table tracks every individual physical DVD copy held across the two store locations. It is the critical link between the film catalogue and actual rental activity — a film can only be rented if it has at least one inventory copy. With 4,581 copies across 958 film titles and 2 stores, the average film has ~5 copies per store. Analysts should note that 42 catalogue films have no inventory, effectively making them unavailable. The table enables per-copy utilisation analysis (e.g., identifying overused or underused copies), stock management queries, and store-level inventory reports.
