# Source Discovery: dvd_rental_store__film_category
**Generated:** 2026-05-08  
**Database:** dbt_database.db (SQLite)  
**Domain:** DVD Rental Store

---

## 1. Exploratory Analysis

### Columns & Data Types
| Column | Type | Nullable |
|--------|------|----------|
| film_id | INTEGER | YES |
| category_id | INTEGER | YES |
| last_update | TEXT | YES |

### Descriptive Metrics
| Metric | Value |
|--------|-------|
| Total rows | 1,000 |
| Unique (film_id, category_id) pairs | 1,000 |
| Unique films | 1,000 |
| Unique categories | 16 |

---

## 2. Data Structure

### Primary Key
Composite key: **(film_id, category_id)** — confirmed unique across all rows. However, since every film appears exactly once, `film_id` alone also uniquely identifies each row.

### Foreign Keys
- `film_id` → **dvd_rental_store__film** (`film_id`)
- `category_id` → **dvd_rental_store__category** (`category_id`)

### Orchestration
Full drop-and-recreate on each load.

### Relationships
- Bridge table between `film` and `category`.
- In practice, each film belongs to **exactly one category** (1:1 mapping from film perspective).

---

## 3. Data Behavior

- Despite being modelled as a many-to-many bridge, **each film has exactly one category** in this dataset.
- Film count per category ranges from **51 (Music)** to **74 (Sports)**.
- Top 5 categories by film count: Sports (74), Foreign (73), Family (69), Documentary (68), Animation (66).
- Bottom 5 categories: Music (51), Horror (56), Travel (57), Classics (57), Comedy (58).

---

## 4. Column Descriptions

| Column | Description |
|--------|-------------|
| film_id | Foreign key to `dvd_rental_store__film`. Identifies the film. Part of the composite primary key. Effectively unique — each film appears in exactly one category. |
| category_id | Foreign key to `dvd_rental_store__category`. Identifies the genre/category assigned to the film. |
| last_update | Timestamp of the last modification to this record. |

## 5. Business Description

The `film_category` table assigns genre categories to films in the DVD rental store catalogue. Although structurally a many-to-many bridge (allowing multi-genre classification), the data shows that every film has exactly one category, making this effectively a 1-to-1 relationship in practice. This simplifies genre-based analytics: each film can be unambiguously attributed to a single genre without risk of double-counting.
