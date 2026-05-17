# Source Discovery: dvd_rental_store__category
**Generated:** 2026-05-08  
**Database:** dbt_database.db (SQLite)  
**Domain:** DVD Rental Store

---

## 1. Exploratory Analysis

### Columns & Data Types
| Column | Type | Nullable |
|--------|------|----------|
| category_id | INTEGER | YES |
| name | TEXT | YES |
| last_update | TEXT | YES |

### Descriptive Metrics
| Metric | Value |
|--------|-------|
| Total rows | 16 |
| Unique category_id | 16 |

---

## 2. Data Structure

### Primary Key
`category_id` — fully unique, 16 distinct values for 16 rows.

### Foreign Keys
None. This is a reference/lookup table.

### Categorical Values (name)
Action, Animation, Children, Classics, Comedy, Documentary, Drama, Family, Foreign, Games, Horror, Music, New, Sci-Fi, Sports, Travel

### Orchestration
Full drop-and-recreate on each load. No incremental logic detected.

### Relationships
- `category_id` → **dvd_rental_store__film_category** (`category_id`): each film is assigned to exactly one of these 16 categories.

---

## 3. Data Behavior

- A small, static lookup table of **16 genre/category names**.
- The category "New" likely refers to recently released films rather than a genre.
- Film distribution across categories is fairly even, ranging from 51 (Music) to 74 (Sports) films per category.
- Each film belongs to **exactly one category** (confirmed by film_category uniqueness analysis).

---

## 4. Column Descriptions

| Column | Description |
|--------|-------------|
| category_id | Unique integer identifier for each film category. Primary key. |
| name | Human-readable genre or classification name for the category (e.g., "Action", "Comedy"). |
| last_update | Timestamp of the last modification to this record. |

## 5. Business Description

The `category` table defines the 16 film genres/classifications used to organise the DVD rental store's catalogue. It enables genre-based browsing and filtering for customers, and supports analytics such as identifying the most rented or most profitable film categories. The category "New" is notable as a release-status label rather than a genre, which may require special handling in reporting.
