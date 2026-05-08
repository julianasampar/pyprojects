# Source Discovery Analysis
**Database:** `dbt_database.db`  
**Path:** `/Users/julianasampar/Desktop/learning_dev/personal_dev/pyprojects/dbt_database.db`  
**Size:** ~760 MB  
**Total Tables:** 34  
**Analysis Date:** 2026-05-08  

---

## Table of Contents

1. [Database Overview](#1-database-overview)
2. [Domain: DVD Rental Store](#2-domain-dvd-rental-store)
   - [Schemas & Descriptive Stats](#21-schemas--descriptive-stats)
   - [Primary Keys](#22-primary-keys)
   - [Foreign Keys](#23-foreign-keys)
   - [Categorical Values](#24-categorical-values)
   - [Relationships](#25-relationships)
   - [Data Behavior](#26-data-behavior)
3. [Domain: NYC Open Data](#3-domain-nyc-open-data)
   - [Schemas & Descriptive Stats](#31-schemas--descriptive-stats)
   - [Primary Keys](#32-primary-keys)
   - [Foreign Keys](#33-foreign-keys)
   - [Categorical Values](#34-categorical-values)
   - [Relationships](#35-relationships)
   - [Data Behavior](#36-data-behavior)
4. [dbt Downstream Models](#4-dbt-downstream-models)
5. [Data Quality Observations](#5-data-quality-observations)

---

## 1. Database Overview

The database contains **34 tables** organized across 2 source domains and multiple dbt transformation layers:

| Layer          | Tables                                                                                                         | Count |
|----------------|----------------------------------------------------------------------------------------------------------------|-------|
| **Source**     | `dvd_rental_store__*`, `nyc_air_quality__historical`, `nyc_central_park_squirrels_census__*`, `nyc_street_tree_census__2015` | 19    |
| **Staging**    | `stg__nyc_central_park_squirrels_census__2018`, `stg__nyc_central_park_squirrels_census__2020`, `stg__nyc_street_tree_census__2015`, `stg_nyc_air_quality__historical` | 4     |
| **Intermediate** | `int_air_quality`, `int_central_park_squirrels`, `int_manhattan_trees`, `int_nyc_trees`                    | 4     |
| **Marts/Facts**  | `fct_nyc_air_quality`, `fct_nyc_environment_quality`, `fct_nyc_squirrels_by_trees`                         | 3     |
| **Dimensions** | `dim_air_quality_indicators`, `dim_date`, `dim_nyc_neighbourhoods`, `dim_tree_species`                        | 4     |

---

## 2. Domain: DVD Rental Store

A classic OLTP-style relational dataset representing a DVD rental business with customers, films, inventory, rentals and payments. Source tables are prefixed with `dvd_rental_store__`.

### 2.1 Schemas & Descriptive Stats

---

#### `dvd_rental_store__actor`
> Stores information about actors featured in films.

**Row Count:** 200

| Column       | Type    | Non-Null | Unique | Min | Max | Avg   |
|--------------|---------|----------|--------|-----|-----|-------|
| actor_id     | INTEGER | 200      | 200    | 1   | 200 | 100.5 |
| first_name   | TEXT    | 200      | 128    | —   | —   | —     |
| last_name    | TEXT    | 200      | 121    | —   | —   | —     |
| last_update  | TEXT    | 200      | 1      | 2013-05-26 14:47:57 | 2013-05-26 14:47:57 | — |

---

#### `dvd_rental_store__address`
> Physical addresses associated with customers, staff, and stores.

**Row Count:** 603

| Column      | Type    | Non-Null | Notes                                            |
|-------------|---------|----------|--------------------------------------------------|
| address_id  | INTEGER | 603      | PK                                               |
| address     | TEXT    | 603      | Street address                                   |
| address2    | REAL    | —        | Secondary address line; mostly null, stored as REAL (type mismatch) |
| district    | TEXT    | 603      | State/province/district                          |
| city_id     | INTEGER | 603      | FK → city.city_id                                |
| postal_code | REAL    | —        | Stored as REAL (type mismatch — should be TEXT)  |
| phone       | REAL    | —        | Stored as REAL (type mismatch — should be TEXT)  |
| last_update | TEXT    | 603      | —                                                |

---

#### `dvd_rental_store__category`
> Film genre categories.

**Row Count:** 16

| Column      | Type    | Non-Null | Notes |
|-------------|---------|----------|-------|
| category_id | INTEGER | 16       | PK    |
| name        | TEXT    | 16       | Genre name |
| last_update | TEXT    | 16       | All updated on 2006-02-15 |

---

#### `dvd_rental_store__city`
> City reference table.

**Row Count:** 600

| Column     | Type    | Non-Null | Notes                     |
|------------|---------|----------|---------------------------|
| city_id    | INTEGER | 600      | PK                        |
| city       | TEXT    | 600      | City name                 |
| country_id | INTEGER | 600      | FK → country.country_id   |
| last_update| TEXT    | 600      | —                         |

---

#### `dvd_rental_store__country`
> Country reference table.

**Row Count:** 109

| Column     | Type    | Non-Null | Notes  |
|------------|---------|----------|--------|
| country_id | INTEGER | 109      | PK     |
| country    | TEXT    | 109      | Country name |
| last_update| TEXT    | 109      | —      |

---

#### `dvd_rental_store__customer`
> Registered store customers.

**Row Count:** 599

| Column      | Type    | Non-Null | Min  | Max  | Avg   | Notes                        |
|-------------|---------|----------|------|------|-------|------------------------------|
| customer_id | INTEGER | 599      | 1    | 599  | —     | PK                           |
| store_id    | INTEGER | 599      | —    | —    | —     | FK → store.store_id; 2 stores |
| first_name  | TEXT    | 599      | —    | —    | —     | —                            |
| last_name   | TEXT    | 599      | —    | —    | —     | —                            |
| email       | TEXT    | 599      | —    | —    | —     | 0 nulls                      |
| address_id  | INTEGER | 599      | —    | —    | —     | FK → address.address_id      |
| activebool  | TEXT    | 599      | —    | —    | —     | Only value: 't'; inconsistent with `active` field |
| create_date | TEXT    | 599      | 2006-02-14 | 2006-02-14 | — | All created same day    |
| last_update | TEXT    | 599      | —    | —    | —     | —                            |
| active      | INTEGER | 599      | 0    | 1    | 0.975 | 584 active (1), 15 inactive (0) |

---

#### `dvd_rental_store__film`
> Film catalog with rental pricing and metadata.

**Row Count:** 1,000

| Column           | Type    | Non-Null | Min   | Max    | Avg    | Notes                           |
|------------------|---------|----------|-------|--------|--------|---------------------------------|
| film_id          | INTEGER | 1,000    | 1     | 1,000  | —      | PK; all titles unique           |
| title            | TEXT    | 1,000    | —     | —      | —      | 1,000 unique titles             |
| description      | TEXT    | 1,000    | —     | —      | —      | 0 nulls                         |
| release_year     | INTEGER | 1,000    | 2006  | 2006   | 2006   | All films from 2006             |
| language_id      | INTEGER | 1,000    | —     | —      | —      | FK → language.language_id       |
| rental_duration  | INTEGER | 1,000    | 3     | 7      | 4.99   | Days allowed per rental         |
| rental_rate      | REAL    | 1,000    | 0.99  | 4.99   | 2.98   | Price per rental period         |
| length           | INTEGER | 1,000    | 46    | 185    | 115.3  | Film length in minutes          |
| replacement_cost | REAL    | 1,000    | 9.99  | 29.99  | 19.98  | Cost if film is lost/damaged    |
| rating           | TEXT    | 1,000    | —     | —      | —      | 5 possible values               |
| special_features | TEXT    | 1,000    | —     | —      | —      | Stored as array-like string     |
| fulltext         | TEXT    | 1,000    | —     | —      | —      | Full-text search vector         |

---

#### `dvd_rental_store__film_actor`
> Bridge table: maps actors to films (many-to-many).

**Row Count:** 5,462

| Column     | Type    | Non-Null | Notes                    |
|------------|---------|----------|--------------------------|
| actor_id   | INTEGER | 5,462    | Composite PK + FK → actor |
| film_id    | INTEGER | 5,462    | Composite PK + FK → film  |
| last_update| TEXT    | 5,462    | —                         |

---

#### `dvd_rental_store__film_category`
> Bridge table: maps films to categories (each film belongs to exactly 1 category).

**Row Count:** 1,000

| Column      | Type    | Non-Null | Notes                      |
|-------------|---------|----------|----------------------------|
| film_id     | INTEGER | 1,000    | Composite PK + FK → film   |
| category_id | INTEGER | 1,000    | Composite PK + FK → category |
| last_update | TEXT    | 1,000    | —                          |

> **Note:** 1,000 rows for 1,000 films → each film belongs to exactly **one** category (not a true many-to-many in practice).

---

#### `dvd_rental_store__inventory`
> Physical copies of films available at each store.

**Row Count:** 4,581

| Column       | Type    | Non-Null | Min | Max  | Notes                              |
|--------------|---------|----------|-----|------|------------------------------------|
| inventory_id | INTEGER | 4,581    | 1   | 4,581 | PK                                |
| film_id      | INTEGER | 4,581    | —   | —    | FK → film.film_id; 958/1000 films have copies |
| store_id     | INTEGER | 4,581    | —   | —    | FK → store.store_id; 2 stores      |
| last_update  | TEXT    | 4,581    | —   | —    | —                                  |

> **Note:** 42 films have no physical copies in inventory. Max copies of one film at one store: 8.

---

#### `dvd_rental_store__language`
> Film language reference.

**Row Count:** 6

| Column      | Type    | Non-Null | Notes         |
|-------------|---------|----------|---------------|
| language_id | INTEGER | 6        | PK            |
| name        | TEXT    | 6        | Language name (padded with spaces) |
| last_update | TEXT    | 6        | All 2006-02-15 |

---

#### `dvd_rental_store__payment`
> Payment transactions linked to rentals.

**Row Count:** 14,596

| Column       | Type    | Non-Null | Min     | Max    | Avg   | Notes                            |
|--------------|---------|----------|---------|--------|-------|----------------------------------|
| payment_id   | INTEGER | 14,596   | 17,503  | 32,098 | —     | PK; no duplicates                |
| customer_id  | INTEGER | 14,596   | —       | —      | —     | FK → customer; 599 distinct customers |
| staff_id     | INTEGER | 14,596   | —       | —      | —     | FK → staff; 2 distinct staff     |
| rental_id    | INTEGER | 14,596   | —       | —      | —     | FK → rental; 14,592 distinct rentals |
| amount       | REAL    | 14,596   | 0.00    | 11.99  | 4.20  | Payment amount in USD; 0 nulls   |
| payment_date | TEXT    | 14,596   | 2007-02-14 | 2007-05-14 | — | ~3-month payment window      |

> **Note:** Payment IDs start at 17,503 (not 1), suggesting data was exported from a larger dataset slice.

---

#### `dvd_rental_store__rental`
> Rental transactions linking customers to inventory items.

**Row Count:** 16,044

| Column       | Type    | Non-Null | Min        | Max        | Notes                            |
|--------------|---------|----------|------------|------------|----------------------------------|
| rental_id    | INTEGER | 16,044   | 1          | 16,049     | PK; no duplicates                |
| rental_date  | TEXT    | 16,044   | 2005-05-24 | 2006-02-14 | —                                |
| inventory_id | INTEGER | 16,044   | —          | —          | FK → inventory; 4,580 distinct   |
| customer_id  | INTEGER | 16,044   | —          | —          | FK → customer; 599 distinct      |
| return_date  | TEXT    | ~15,862  | 2005-05-25 | 2005-09-02 | **182 empty/null = unreturned**  |
| staff_id     | INTEGER | 16,044   | —          | —          | FK → staff; 2 distinct           |
| last_update  | TEXT    | 16,044   | —          | —          | —                                |

---

#### `dvd_rental_store__staff`
> Store employees.

**Row Count:** 2

| Column      | Type    | Non-Null | Notes                         |
|-------------|---------|----------|-------------------------------|
| staff_id    | INTEGER | 2        | PK                            |
| first_name  | TEXT    | 2        | —                             |
| last_name   | TEXT    | 2        | —                             |
| address_id  | INTEGER | 2        | FK → address.address_id       |
| email       | TEXT    | 2        | —                             |
| store_id    | INTEGER | 2        | FK → store.store_id           |
| active      | TEXT    | 2        | Both 't' (active)             |
| username    | TEXT    | 2        | Login username                |
| password    | TEXT    | 2        | Hashed password               |
| last_update | TEXT    | 2        | —                             |
| picture     | TEXT    | 2        | Profile picture reference     |

---

#### `dvd_rental_store__store`
> Physical store locations.

**Row Count:** 2

| Column           | Type    | Non-Null | Notes                          |
|------------------|---------|----------|--------------------------------|
| store_id         | INTEGER | 2        | PK                             |
| manager_staff_id | INTEGER | 2        | FK → staff.staff_id            |
| address_id       | INTEGER | 2        | FK → address.address_id        |
| last_update      | TEXT    | 2        | —                              |

---

### 2.2 Primary Keys

| Table               | Primary Key                    | Uniqueness Verified |
|---------------------|--------------------------------|---------------------|
| actor               | `actor_id`                     | ✅ 200 unique        |
| address             | `address_id`                   | ✅ 603 unique        |
| category            | `category_id`                  | ✅ 16 unique         |
| city                | `city_id`                      | ✅ 600 unique        |
| country             | `country_id`                   | ✅ 109 unique        |
| customer            | `customer_id`                  | ✅ 599 unique        |
| film                | `film_id`                      | ✅ 1,000 unique      |
| film_actor          | `(actor_id, film_id)`          | ✅ composite unique  |
| film_category       | `(film_id, category_id)`       | ✅ composite unique  |
| inventory           | `inventory_id`                 | ✅ 4,581 unique      |
| language            | `language_id`                  | ✅ 6 unique          |
| payment             | `payment_id`                   | ✅ 14,596 unique     |
| rental              | `rental_id`                    | ✅ 16,044 unique     |
| staff               | `staff_id`                     | ✅ 2 unique          |
| store               | `store_id`                     | ✅ 2 unique          |

---

### 2.3 Foreign Keys

| Child Table    | FK Column          | Parent Table | Parent PK   | Description                               |
|----------------|--------------------|--------------|-------------|-------------------------------------------|
| address        | `city_id`          | city         | `city_id`   | Address belongs to a city                 |
| city           | `country_id`       | country      | `country_id`| City belongs to a country                 |
| customer       | `store_id`         | store        | `store_id`  | Customer registered at a store            |
| customer       | `address_id`       | address      | `address_id`| Customer's home address                   |
| film           | `language_id`      | language     | `language_id`| Film's spoken language                   |
| film_actor     | `actor_id`         | actor        | `actor_id`  | Actor featured in a film                  |
| film_actor     | `film_id`          | film         | `film_id`   | Film featuring an actor                   |
| film_category  | `film_id`          | film         | `film_id`   | Film assigned to a category               |
| film_category  | `category_id`      | category     | `category_id`| Category assigned to a film              |
| inventory      | `film_id`          | film         | `film_id`   | Inventory copy is a specific film         |
| inventory      | `store_id`         | store        | `store_id`  | Inventory copy belongs to a store         |
| payment        | `customer_id`      | customer     | `customer_id`| Payment made by a customer               |
| payment        | `staff_id`         | staff        | `staff_id`  | Payment processed by a staff member       |
| payment        | `rental_id`        | rental       | `rental_id` | Payment associated to a rental            |
| rental         | `inventory_id`     | inventory    | `inventory_id`| Rental is for a specific inventory copy |
| rental         | `customer_id`      | customer     | `customer_id`| Rental made by a customer               |
| rental         | `staff_id`         | staff        | `staff_id`  | Rental processed by a staff member       |
| staff          | `address_id`       | address      | `address_id`| Staff member's address                   |
| staff          | `store_id`         | store        | `store_id`  | Staff member assigned to a store         |
| store          | `address_id`       | address      | `address_id`| Store's physical address                 |
| store          | `manager_staff_id` | staff        | `staff_id`  | Store's manager                          |

---

### 2.4 Categorical Values

#### Film Rating (`dvd_rental_store__film.rating`)
| Value | Count | % of Total |
|-------|-------|------------|
| PG-13 | 223   | 22.3%      |
| NC-17 | 210   | 21.0%      |
| R     | 195   | 19.5%      |
| PG    | 194   | 19.4%      |
| G     | 178   | 17.8%      |

#### Film Special Features (`dvd_rental_store__film.special_features`)
Values are stored as array-like strings. The four base features are:
`Trailers`, `Commentaries`, `Deleted Scenes`, `Behind the Scenes`  
They appear in all 15 possible combinations. Most common: `{Trailers, Commentaries, Behind the Scenes}` (79 films).

#### Film Categories (`dvd_rental_store__category.name`)
`Action`, `Animation`, `Children`, `Classics`, `Comedy`, `Documentary`, `Drama`, `Family`, `Foreign`, `Games`, `Horror`, `Music`, `New`, `Sci-Fi`, `Sports`, `Travel`

#### Film Languages (`dvd_rental_store__language.name`)
`English`, `Italian`, `Japanese`, `Mandarin`, `French`, `German`  
> 1,000/1,000 films are in **English**. The other 5 languages exist in the language table but are not actually used by any film.

#### Customer Status (`dvd_rental_store__customer`)
| `active` | `activebool` | Count | Meaning              |
|----------|--------------|-------|----------------------|
| 1        | t            | 584   | Active customer      |
| 0        | t            | 15    | Inactive customer    |

> `activebool` is always `'t'` regardless of the `active` field — these two columns are **inconsistent**.

---

### 2.5 Relationships

```
country (109)
  └── city (600)            [country_id]
        └── address (603)   [city_id]
              ├── customer (599)   [address_id]
              ├── staff (2)        [address_id]
              └── store (2)        [address_id]

language (6)
  └── film (1,000)           [language_id]
        ├── film_actor (5,462)    [film_id] ──── actor (200)   [actor_id]
        ├── film_category (1,000) [film_id] ──── category (16) [category_id]
        └── inventory (4,581)     [film_id]
              └── rental (16,044) [inventory_id]
                    ├── customer (599) [customer_id]
                    ├── staff (2)      [staff_id]
                    └── payment (14,596) [rental_id]
                          ├── customer (599) [customer_id]
                          └── staff (2)      [staff_id]

store (2)
  ├── inventory (4,581) [store_id]
  ├── customer (599)    [store_id]
  ├── staff (2)         [store_id]
  └── store (self) ←── staff [manager_staff_id]
```

**Key Relationship Insights:**
- `rental` + `inventory` + `film` → identify **which film** was rented  
- `rental` + `customer` → understand **customer rental behavior**  
- `payment` + `rental` → link **revenue to specific rentals**  
- `film_actor` + `actor` + `film` → identify **cast of each film**  
- `film_category` + `category` + `film` → identify **genre of each film**  
- `customer` + `address` + `city` + `country` → identify **customer geography**  

---

### 2.6 Data Behavior

- **ETL Pattern:** Full refresh (table drop and recreate). No incremental timestamp column beyond `last_update`.
- **Temporal Coverage:**
  - Customers created: 2006-02-14 (single batch load)
  - Rentals: 2005-05-24 → 2006-02-14
  - Payments: 2007-02-14 → 2007-05-14 *(~1 year after rentals — unusual lag)*
- **Rental Activity:**
  - 16,044 total rentals by 599 customers (avg ~26.8 rentals/customer)
  - 182 rentals with no return date (open/lost rentals — 1.1%)
  - Only 14,592 of 16,044 rentals have a payment → ~452 rentals were never paid
- **Inventory Utilization:**
  - 4,581 inventory copies for 1,000 films across 2 stores
  - 42 films have zero inventory copies (not available to rent)
  - Max 8 copies of a single film at one store

---

## 3. Domain: NYC Open Data

Three environmental and wildlife datasets from New York City's open data portal. All raw source tables are named with the `nyc_` prefix.

### 3.1 Schemas & Descriptive Stats

---

#### `nyc_air_quality__historical`
> Historical air quality measurements across NYC neighbourhoods and boroughs, covering multiple environmental indicators from 2005 to 2015.

**Row Count:** 18,862

| Column          | Type    | Non-Null | Min        | Max        | Notes                              |
|-----------------|---------|----------|------------|------------|------------------------------------|
| Unique ID       | INTEGER | 18,862   | 130,355    | 878,254    | Natural PK (sparse range)          |
| Indicator ID    | INTEGER | 18,862   | —          | —          | 21 distinct indicator IDs          |
| Name            | TEXT    | 18,862   | —          | —          | 18 distinct indicator names        |
| Measure         | TEXT    | 18,862   | —          | —          | 8 distinct measures                |
| Measure Info    | TEXT    | 18,862   | —          | —          | Units for the measure              |
| Geo Type Name   | TEXT    | 18,862   | —          | —          | 5 distinct geographic levels       |
| Geo Join ID     | INTEGER | 18,862   | —          | —          | Geo area identifier                |
| Geo Place Name  | TEXT    | 18,862   | —          | —          | 114 distinct geographic areas      |
| Time Period     | TEXT    | 18,862   | —          | —          | 57 distinct periods (annual/seasonal/multi-year) |
| Start_Date      | TEXT    | 18,862   | 01/01/2005 | 12/31/2015 | Period start date (MM/DD/YYYY)     |
| Data Value      | REAL    | 18,862   | 0.00       | 424.70     | Measured value; avg ~21.05; 0 nulls |
| Message         | REAL    | 0        | —          | —          | **All 18,862 values are NULL** — unused column |

---

#### `nyc_central_park_squirrels_census__2018`
> Field observations of squirrels in Central Park during October 2018. Each row represents one squirrel sighting.

**Row Count:** 3,023

| Column                  | Type    | Non-Null | Notes                                          |
|-------------------------|---------|----------|------------------------------------------------|
| X                       | REAL    | 3,023    | Longitude (-73.98 to -73.95)                   |
| Y                       | REAL    | 3,023    | Latitude (40.76 to 40.80)                      |
| Unique Squirrel ID      | TEXT    | 3,023    | Natural PK — **3,018 unique (5 duplicates)**   |
| Hectare                 | TEXT    | 3,023    | 339 distinct hectares (grid reference)         |
| Shift                   | TEXT    | 3,023    | AM (1,347) / PM (1,676)                        |
| Date                    | INTEGER | 3,023    | Stored as integer: 10062018–10202018 (Oct 6–20, 2018) |
| Hectare Squirrel Number | INTEGER | 3,023    | Sequential number within hectare per shift     |
| Age                     | TEXT    | 2,902    | Adult (2,568), Juvenile (330), ? (4); 121 nulls |
| Primary Fur Color       | TEXT    | 2,968    | Gray (2,473), Cinnamon (392), Black (103); 55 nulls |
| Highlight Fur Color     | TEXT    | —        | Secondary fur color notes                      |
| Combination of Primary and Highlight Color | TEXT | — | —                               |
| Color notes             | TEXT    | —        | Free-text color observations                   |
| Location                | TEXT    | 2,959    | Ground Plane (2,116), Above Ground (843); 64 nulls |
| Above Ground Sighter Measurement | TEXT | —   | Height estimate when above ground              |
| Specific Location       | TEXT    | —        | Free-text location details                     |
| Running                 | INTEGER | 3,023    | Binary (0/1) behavioral flag                   |
| Chasing                 | INTEGER | 3,023    | Binary (0/1) behavioral flag                   |
| Climbing                | INTEGER | 3,023    | Binary (0/1) behavioral flag                   |
| Eating                  | INTEGER | 3,023    | Binary (0/1) behavioral flag                   |
| Foraging                | INTEGER | 3,023    | Binary (0/1) behavioral flag                   |
| Other Activities        | TEXT    | —        | Free-text activity notes                       |
| Kuks                    | INTEGER | 3,023    | Binary (0/1) vocalization flag                 |
| Quaas                   | INTEGER | 3,023    | Binary (0/1) vocalization flag                 |
| Moans                   | INTEGER | 3,023    | Binary (0/1) vocalization flag                 |
| Tail flags              | INTEGER | 3,023    | Binary (0/1) behavioral flag                   |
| Tail twitches           | INTEGER | 3,023    | Binary (0/1) behavioral flag                   |
| Approaches              | INTEGER | 3,023    | Binary (0/1) human interaction flag            |
| Indifferent             | INTEGER | 3,023    | Binary (0/1) human interaction flag            |
| Runs from               | INTEGER | 3,023    | Binary (0/1) human interaction flag            |
| Other Interactions      | TEXT    | —        | Free-text interaction notes                    |
| Lat/Long                | TEXT    | 3,023    | Combined lat/lon string                        |

---

#### `nyc_central_park_squirrels_census__2020`
> Field observations of squirrels across NYC parks during 2020. Broader geographic scope than 2018 (not limited to Central Park).

**Row Count:** 433

| Column                         | Type    | Non-Null | Notes                                             |
|--------------------------------|---------|----------|---------------------------------------------------|
| Area Name                      | TEXT    | 433      | 4 areas: Central Manhattan, Upper Manhattan, Lower Manhattan, Brooklyn |
| Area ID                        | TEXT    | 433      | Area identifier                                   |
| Park Name                      | TEXT    | 433      | 20 distinct parks                                 |
| Park ID                        | INTEGER | 433      | Park identifier                                   |
| Date                           | TEXT    | 433      | All sightings: 01/03/2020 (single date)           |
| Squirrel ID                    | TEXT    | 433      | Natural PK — 433 unique ✅                        |
| Primary Fur Color              | TEXT    | 432      | Gray (390), Cinnamon (26), Black (16); 1 null     |
| Highlights in Fur Color        | TEXT    | —        | —                                                 |
| Color Notes                    | TEXT    | —        | —                                                 |
| Location                       | TEXT    | 399      | Ground Plane (220), Above Ground (84), combinations; 34 nulls |
| Above Ground (Height in Feet)  | TEXT    | —        | Height when above ground                          |
| Specific Location              | TEXT    | —        | —                                                 |
| Activities                     | TEXT    | —        | Free-text (vs. binary flags in 2018)              |
| Interactions with Humans       | TEXT    | —        | Free-text (vs. binary flags in 2018)              |
| Other Notes or Observations    | TEXT    | —        | —                                                 |
| Squirrel Latitude (DD.DDDDDD)  | REAL    | 433      | 40.70 to 40.86                                    |
| Squirrel Longitude (-DD.DDDDDD)| REAL    | 433      | -74.02 to 73.98 ⚠️ mix of negative/positive values |

---

#### `nyc_street_tree_census__2015`
> Complete inventory of street trees across all five NYC boroughs, collected during the 2015 NYC Street Tree Census.

**Row Count:** 683,788

| Column            | Type    | Non-Null  | Min  | Max    | Avg   | Notes                               |
|-------------------|---------|-----------|------|--------|-------|-------------------------------------|
| tree_id           | INTEGER | 683,788   | 3    | 722,694 | —    | PK (sparse — gaps in sequence)      |
| block_id          | INTEGER | 683,788   | —    | —      | —     | 101,390 distinct city blocks        |
| created_at        | TEXT    | 683,788   | 01/01/2016 | 12/31/2015 | — | Census collection date (2015)    |
| tree_dbh          | INTEGER | 683,788   | 0    | 450    | 11.28 | Diameter at breast height (inches)  |
| stump_diam        | INTEGER | 683,788   | 0    | 140    | —     | Stump diameter (0 for alive trees)  |
| curb_loc          | TEXT    | 683,788   | —    | —      | —     | OnCurb (656,896), OffsetFromCurb (26,892) |
| status            | TEXT    | 683,788   | —    | —      | —     | Alive (652,173), Stump (17,654), Dead (13,961) |
| health            | TEXT    | 652,172   | —    | —      | —     | Good (528,850), Fair (96,504), Poor (26,818); 31,616 nulls (dead/stumps) |
| spc_latin         | TEXT    | 652,169   | —    | —      | —     | 132 distinct species; 31,619 nulls  |
| spc_common        | TEXT    | 652,169   | —    | —      | —     | 132 distinct species (common names) |
| steward           | TEXT    | —         | —    | —      | —     | Presence/count of tree stewards     |
| guards            | TEXT    | —         | —    | —      | —     | Guard type (harmful/helpful/etc.)   |
| sidewalk          | TEXT    | —         | —    | —      | —     | Sidewalk damage flag                |
| user_type         | TEXT    | —         | —    | —      | —     | Data collector type                 |
| problems          | TEXT    | —         | —    | —      | —     | Pipe-delimited list of tree problems|
| root_stone/grate/other | TEXT | —     | —    | —      | —     | Root problem flags                  |
| trunk_wire/light/other | TEXT | —     | —    | —      | —     | Trunk problem flags                 |
| brch_light/shoe/other  | TEXT | —     | —    | —      | —     | Branch problem flags                |
| address           | TEXT    | 683,788   | —    | —      | —     | Street address of tree              |
| postcode          | INTEGER | 683,788   | —    | —      | —     | ZIP code                            |
| zip_city          | TEXT    | 683,788   | —    | —      | —     | City name from ZIP                  |
| community board   | INTEGER | 683,788   | —    | —      | —     | NYC community board number          |
| borocode          | INTEGER | 683,788   | —    | —      | —     | Borough code (1–5)                  |
| borough           | TEXT    | 683,788   | —    | —      | —     | 5 boroughs                          |
| cncldist          | INTEGER | 683,788   | —    | —      | —     | City council district               |
| st_assem          | INTEGER | 683,788   | —    | —      | —     | State assembly district             |
| st_senate         | INTEGER | 683,788   | —    | —      | —     | State senate district               |
| nta               | TEXT    | 683,788   | —    | —      | —     | Neighborhood Tabulation Area code   |
| nta_name          | TEXT    | 683,788   | —    | —      | —     | NTA name                            |
| boro_ct           | INTEGER | 683,788   | —    | —      | —     | Borough + census tract combined     |
| state             | TEXT    | 683,788   | —    | —      | —     | Always "New York"                   |
| latitude          | REAL    | 683,788   | —    | —      | —     | WGS84 latitude                      |
| longitude         | REAL    | 683,788   | —    | —      | —     | WGS84 longitude                     |
| x_sp / y_sp       | REAL    | 683,788   | —    | —      | —     | NY State Plane coordinates          |
| council district  | REAL    | 683,788   | —    | —      | —     | Duplicate of cncldist (REAL type)   |
| census tract      | REAL    | 683,788   | —    | —      | —     | Census tract number                 |
| bin               | REAL    | 683,788   | —    | —      | —     | Building identification number      |
| bbl               | REAL    | 683,788   | —    | —      | —     | Borough-block-lot identifier        |

---

### 3.2 Primary Keys

| Table                                    | Primary Key          | Uniqueness Verified                        |
|------------------------------------------|----------------------|--------------------------------------------|
| nyc_air_quality__historical              | `Unique ID`          | ✅ 18,862 unique                            |
| nyc_central_park_squirrels_census__2018  | `Unique Squirrel ID` | ⚠️ 3,018 unique out of 3,023 — **5 duplicates** |
| nyc_central_park_squirrels_census__2020  | `Squirrel ID`        | ✅ 433 unique                               |
| nyc_street_tree_census__2015             | `tree_id`            | ✅ 683,788 unique (sparse range 3–722,694)  |

---

### 3.3 Foreign Keys

The NYC source tables are **independent flat files** from NYC Open Data — they do not have explicit foreign key relationships with each other. Cross-domain joins are established in the downstream dbt intermediate and fact models using geographic identifiers (`nta`, `Geo Join ID`, borough, etc.).

---

### 3.4 Categorical Values

#### Air Quality — Indicator Names (`nyc_air_quality__historical`)
| Indicator Name                                          | Records |
|---------------------------------------------------------|---------|
| Nitrogen dioxide (NO2)                                  | 6,345   |
| Fine particles (PM 2.5)                                 | 6,345   |
| Ozone (O3)                                              | 2,115   |
| Asthma hospitalizations due to Ozone                    | 480     |
| Asthma emergency departments visits due to Ozone        | 480     |
| Asthma emergency department visits due to PM2.5         | 480     |
| Annual vehicle miles traveled (trucks)                  | 321     |
| Annual vehicle miles traveled (cars)                    | 321     |
| Annual vehicle miles traveled                           | 321     |
| Respiratory hospitalizations due to PM2.5 (age 20+)     | 240     |
| Deaths due to PM2.5                                     | 240     |
| Cardiovascular hospitalizations due to PM2.5 (age 40+)  | 240     |
| Cardiac and respiratory deaths due to Ozone             | 240     |
| Outdoor Air Toxics - Formaldehyde                       | 203     |
| Outdoor Air Toxics - Benzene                            | 203     |
| Boiler Emissions - Total SO2 Emissions                  | 96      |
| Boiler Emissions - Total PM2.5 Emissions                | 96      |
| Boiler Emissions - Total NOx Emissions                  | 96      |

#### Air Quality — Time Periods (sample)
`2005`, `2005-2007`, `2009-2011`, `2010`, `2011`, `2013`, `2014`, `2015`, `2015-2017`, `Annual Average 2015`, …  
57 distinct periods; mix of annual, biennial, and multi-year averages.

#### Air Quality — Geo Type Names
5 distinct levels: borough-level, community district, sub-borough, UHF42, citywide.

#### Squirrel Census 2018 — Key Categoricals
| Column           | Values                                | Nulls |
|------------------|---------------------------------------|-------|
| Shift            | AM (1,347), PM (1,676)                | 0     |
| Age              | Adult (2,568), Juvenile (330), ? (4)  | 121   |
| Primary Fur Color| Gray (2,473), Cinnamon (392), Black (103) | 55 |
| Location         | Ground Plane (2,116), Above Ground (843) | 64  |

#### Squirrel Census 2020 — Key Categoricals
| Column           | Values                                         | Nulls |
|------------------|------------------------------------------------|-------|
| Area Name        | Central Manhattan (174), Upper Manhattan (129), Lower Manhattan (72), Brooklyn (58) | 0 |
| Primary Fur Color| Gray (390), Cinnamon (26), Black (16)          | 1     |
| Location         | Ground Plane (220), Above Ground (84), Above Ground + Specific Location (54), ... | 34 |

#### Tree Census 2015 — Key Categoricals
| Column    | Values                                                             |
|-----------|--------------------------------------------------------------------|
| status    | Alive (652,173), Stump (17,654), Dead (13,961)                     |
| health    | Good (528,850), Fair (96,504), Poor (26,818); 31,616 null          |
| borough   | Queens (250,551), Brooklyn (177,293), Staten Island (105,318), Bronx (85,203), Manhattan (65,423) |
| curb_loc  | OnCurb (656,896), OffsetFromCurb (26,892)                          |

#### Top 10 Tree Species (2015 Census)
| Common Name         | Count  |
|---------------------|--------|
| London planetree    | 87,014 |
| honeylocust         | 64,264 |
| Callery pear        | 58,931 |
| pin oak             | 53,185 |
| Norway maple        | 34,189 |
| littleleaf linden   | 29,742 |
| cherry              | 29,279 |
| Japanese zelkova    | 29,258 |
| ginkgo              | 21,024 |
| Sophora             | 19,338 |

---

### 3.5 Relationships

The three NYC datasets can be joined through geographic identifiers in the downstream dbt models:

```
nyc_street_tree_census__2015
  [nta, borough, borocode]
       │
       ▼ (geo join via NTA/neighbourhood code)
nyc_air_quality__historical
  [Geo Join ID, Geo Place Name, Geo Type Name]

nyc_central_park_squirrels_census__2018 & 2020
  [Hectare / Park Name / Area Name]
       │
       ▼ (spatial join via lat/lon or NTA)
nyc_street_tree_census__2015
  [nta_name, latitude, longitude]
```

**Key Relationship Insights:**
- `nyc_street_tree_census__2015` + `nyc_air_quality__historical` → analyse **tree coverage vs. air quality** by neighbourhood
- `nyc_central_park_squirrels_census__2018/2020` + `nyc_street_tree_census__2015` → analyse **squirrel presence near trees**
- All three datasets can be connected to `dim_nyc_neighbourhoods` for consistent geographic aggregation

---

### 3.6 Data Behavior

- **ETL Pattern:** Full refresh (static snapshots, not incremental). Each source is a one-time census/export.
- **Temporal Coverage:**
  - Air Quality: 2005–2015 (annual and multi-year aggregations)
  - Squirrel Census 2018: October 6–20, 2018 (14-day field survey in Central Park)
  - Squirrel Census 2020: January 3, 2020 (single-day observation across NYC parks)
  - Street Tree Census: 2015 calendar year
- **Structural Differences Between Squirrel Censuses:**
  - 2018: 31 columns, binary behavioral/interaction flags, Central Park only, ~3K sightings
  - 2020: 17 columns, free-text activities/interactions, multi-borough, ~433 sightings
  - Schemas are NOT compatible for direct UNION — require transformation in staging

---

## 4. dbt Downstream Models

| Model                          | Type        | Description                                                 |
|--------------------------------|-------------|-------------------------------------------------------------|
| `stg_nyc_air_quality__historical`                   | Staging | Cleaned air quality source             |
| `stg__nyc_central_park_squirrels_census__2018`      | Staging | Standardised 2018 squirrel census      |
| `stg__nyc_central_park_squirrels_census__2020`      | Staging | Standardised 2020 squirrel census      |
| `stg__nyc_street_tree_census__2015`                 | Staging | Cleaned street tree census             |
| `int_air_quality`                                   | Intermediate | Enriched/joined air quality data    |
| `int_central_park_squirrels`                        | Intermediate | Combined squirrel census (2018+2020)|
| `int_manhattan_trees`                               | Intermediate | Manhattan-scoped tree data          |
| `int_nyc_trees`                                     | Intermediate | Full NYC tree data                  |
| `dim_air_quality_indicators`                        | Dimension   | Indicator lookup table               |
| `dim_date`                                          | Dimension   | Date spine                           |
| `dim_nyc_neighbourhoods`                            | Dimension   | NYC neighbourhood reference          |
| `dim_tree_species`                                  | Dimension   | Tree species lookup                  |
| `fct_nyc_air_quality`                               | Fact        | Air quality measurements             |
| `fct_nyc_environment_quality`                       | Fact        | Combined environment quality metrics |
| `fct_nyc_squirrels_by_trees`                        | Fact        | Squirrel sightings enriched with tree data |

---

## 5. Data Quality Observations

| # | Table | Issue | Severity | Detail |
|---|-------|-------|----------|--------|
| 1 | `dvd_rental_store__customer` | Inconsistent active flags | Medium | `activebool` is always `'t'` even for 15 inactive customers (`active = 0`) |
| 2 | `dvd_rental_store__address` | Column type mismatch | Low | `address2`, `postal_code`, `phone` stored as `REAL` — should be `TEXT` |
| 3 | `dvd_rental_store__language` | Trailing whitespace in names | Low | Language names are padded with spaces (e.g. `"English             "`) |
| 4 | `dvd_rental_store__film` | Unused language references | Low | Only English (language_id=1) is used by all 1,000 films; 5 language records have no films |
| 5 | `dvd_rental_store__inventory` | 42 films with no inventory | Medium | 42 out of 1,000 films have no copies — cannot be rented |
| 6 | `dvd_rental_store__rental` | Temporal inconsistency | Medium | Rentals occur in 2005–2006, payments in 2007 — ~1 year lag |
| 7 | `dvd_rental_store__rental` | 182 unreturned rentals | Low | `return_date` is null/empty for 1.1% of rentals |
| 8 | `dvd_rental_store__payment` | Non-sequential payment IDs | Info | IDs start at 17,503 — partial export from a larger dataset |
| 9 | `nyc_air_quality__historical` | `Message` column all NULL | Low | Column is completely empty (18,862 nulls) — can be dropped |
| 10 | `nyc_central_park_squirrels_census__2018` | 5 duplicate Squirrel IDs | Medium | Natural PK not fully unique — 3,018 unique out of 3,023 rows |
| 11 | `nyc_central_park_squirrels_census__2018` | Date stored as INTEGER | Low | `Date` column is `10062018` (MMDDYYYY as int) — needs parsing |
| 12 | `nyc_central_park_squirrels_census__2020` | Positive longitude values | Medium | Some longitude values are positive (should be negative for NYC) — data entry error |
| 13 | `nyc_central_park_squirrels_census__2020` | Single observation date | Info | All 433 records share date `01/03/2020` — no temporal variation |
| 14 | `nyc_street_tree_census__2015` | 31,616 null health values | Info | Expected — `health` is only applicable to alive trees; dead/stumps have null |
| 15 | `nyc_street_tree_census__2015` | `created_at` date range anomaly | Low | Min date `01/01/2016` is after max `12/31/2015` — likely a formatting/parsing issue |
| 16 | `nyc_street_tree_census__2015` | Duplicate column `council district` | Low | Column appears as both `cncldist` (INTEGER) and `council district` (REAL) |
