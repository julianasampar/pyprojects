# Source Discovery: dvd_rental_store__film_actor
**Generated:** 2026-05-08  
**Database:** dbt_database.db (SQLite)  
**Domain:** DVD Rental Store

---

## 1. Exploratory Analysis

### Columns & Data Types
| Column | Type | Nullable |
|--------|------|----------|
| actor_id | INTEGER | YES |
| film_id | INTEGER | YES |
| last_update | TEXT | YES |

### Descriptive Metrics
| Metric | Value |
|--------|-------|
| Total rows | 5,462 |
| Unique (actor_id, film_id) pairs | 5,462 |
| Unique actors | 200 |
| Unique films | 997 |

---

## 2. Data Structure

### Primary Key
Composite key: **(actor_id, film_id)** — each actor-film combination is unique. Confirmed: 5,462 rows = 5,462 unique pairs.

### Foreign Keys
- `actor_id` → **dvd_rental_store__actor** (`actor_id`)
- `film_id` → **dvd_rental_store__film** (`film_id`)

### Orchestration
Full drop-and-recreate on each load.

### Relationships
This is a **many-to-many bridge table** between `actor` and `film`:
- Each film can have many actors.
- Each actor can appear in many films.
- Average: ~5.5 actor appearances per film; ~27.3 films per actor.

---

## 3. Data Behavior

- 200 actors are linked to **997 out of 1,000 films** — 3 films have no actor assigned.
- Average of ~5.5 actors per film and ~27.3 film credits per actor.
- The table is a pure association/bridge — no additional attributes beyond the keys and `last_update`.

---

## 4. Column Descriptions

| Column | Description |
|--------|-------------|
| actor_id | Foreign key to `dvd_rental_store__actor`. Identifies the actor. Part of the composite primary key. |
| film_id | Foreign key to `dvd_rental_store__film`. Identifies the film the actor appeared in. Part of the composite primary key. |
| last_update | Timestamp of the last modification to this record. |

## 5. Business Description

The `film_actor` table is a many-to-many bridge table that records which actors appear in which films. It enables queries such as "which films did Actor X appear in?" or "who are the cast members of Film Y?", supporting both customer-facing browsing features and analytics like identifying the most-featured actors or the cast size per film. With 5,462 associations across 200 actors and 997 films, the average film features approximately 5–6 actors.
