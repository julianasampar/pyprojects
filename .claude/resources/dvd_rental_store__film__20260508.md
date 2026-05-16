# Source Discovery: dvd_rental_store__film
**Generated:** 2026-05-08  
**Database:** dbt_database.db (SQLite)  
**Domain:** DVD Rental Store

---

## 1. Exploratory Analysis

### Columns & Data Types
| Column | Type | Nullable |
|--------|------|----------|
| film_id | INTEGER | YES |
| title | TEXT | YES |
| description | TEXT | YES |
| release_year | INTEGER | YES |
| language_id | INTEGER | YES |
| rental_duration | INTEGER | YES |
| rental_rate | REAL | YES |
| length | INTEGER | YES |
| replacement_cost | REAL | YES |
| rating | TEXT | YES |
| last_update | TEXT | YES |
| special_features | TEXT | YES |
| fulltext | TEXT | YES |

### Descriptive Metrics — Numeric Columns
| Metric | rental_duration | rental_rate | length (min) | replacement_cost | release_year |
|--------|----------------|------------|--------------|-----------------|--------------|
| count | 1,000 | 1,000 | 1,000 | 1,000 | 1,000 |
| min | 3 | 0.99 | 46 | 9.99 | 2006 |
| max | 7 | 4.99 | 185 | 29.99 | 2006 |
| avg | 4.99 | 2.98 | 115.27 | 19.98 | 2006 |

---

## 2. Data Structure

### Primary Key
`film_id` — fully unique across all 1,000 rows.

### Foreign Keys
- `language_id` → **dvd_rental_store__language** (`language_id`): the audio language of the film.

### Categorical Values
- **rating**: G (178), PG (194), PG-13 (223), R (195), NC-17 (210) — fairly uniform distribution.
- **rental_duration**: 3, 4, 5, 6, 7 days — evenly distributed (~191–212 films each).
- **rental_rate**: 0.99 (341), 2.99 (323), 4.99 (336) — three pricing tiers, near-equal split.
- **replacement_cost**: 21 distinct values from $9.99 to $29.99 ($1 increments).
- **special_features**: Combinations of {Trailers, Commentaries, Deleted Scenes, Behind the Scenes} — 15 possible combinations.

### Orchestration
`release_year` is uniform (2006 for all films). `last_update` reflects individual record timestamps. Full drop-and-recreate pattern.

### Relationships
- `film_id` → **dvd_rental_store__film_actor** (`film_id`): actors that appear in this film.
- `film_id` → **dvd_rental_store__film_category** (`film_id`): the genre category assigned to this film.
- `film_id` → **dvd_rental_store__inventory** (`film_id`): physical DVD copies of this film.

---

## 3. Data Behavior

- All 1,000 films were released in **2006** — uniform release year, no historical back-catalogue variation.
- All films use **English** as language (language_id pointing to English), despite 6 languages existing in the reference table.
- **42 films have no inventory copies** — they exist in the catalogue but cannot be rented.
- Film lengths range from 46 to 185 minutes (avg ~115 min / ~1h 55m).
- `special_features` is stored as a PostgreSQL array-formatted string (e.g., `{Trailers,"Deleted Scenes"}`), requiring parsing in downstream models.
- `fulltext` appears to be a tsvector/full-text-search representation of title and description, stored as TEXT.
- Rental pricing follows exactly 3 price points (0.99, 2.99, 4.99) — no custom pricing per film.

---

## 4. Column Descriptions

| Column | Description |
|--------|-------------|
| film_id | Unique integer identifier for each film. Primary key. |
| title | Title of the film (e.g., "Academy Dinosaur"). |
| description | Short synopsis or plot summary of the film. |
| release_year | Year the film was released. Uniform value of 2006 across all records. |
| language_id | Foreign key to `dvd_rental_store__language`. The audio language of the film. All films use English in this dataset. |
| rental_duration | Number of days a customer is allowed to keep the rented film. Range: 3–7 days. |
| rental_rate | Daily rental price in USD. Three tiers: $0.99, $2.99, $4.99. |
| length | Runtime of the film in minutes. Range: 46–185 min. |
| replacement_cost | Cost charged to a customer if the DVD is lost or damaged. Range: $9.99–$29.99 in $1 increments. |
| rating | MPAA film rating: G, PG, PG-13, R, or NC-17. |
| last_update | Timestamp of the last modification to this record. |
| special_features | Comma-separated list of bonus features included on the DVD, formatted as a PostgreSQL array string (e.g., `{Trailers,"Deleted Scenes"}`). Values: Trailers, Commentaries, Deleted Scenes, Behind the Scenes. Requires parsing. |
| fulltext | Full-text search representation (tsvector format) of the film title and description. Intended for text-search indexing; not useful for standard analytics. |

## 5. Business Description

The `film` table is the central catalogue of the DVD rental store's 1,000-film library. It stores pricing (rental rate, replacement cost), availability constraints (rental duration), content metadata (rating, genre via film_category, language), and supporting content (special features). A key insight is that the catalogue uses exactly three rental pricing tiers and five duration options, suggesting a deliberate tiered pricing strategy rather than per-title pricing. 42 films have no physical copies in inventory, meaning they are "phantom" catalogue entries that cannot currently be rented. Analysts should be aware that `special_features` requires array parsing and `fulltext` should generally be excluded from analytical models.
