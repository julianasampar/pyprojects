# Source Discovery: dvd_rental_store__payment
**Generated:** 2026-05-08  
**Database:** dbt_database.db (SQLite)  
**Domain:** DVD Rental Store

---

## 1. Exploratory Analysis

### Columns & Data Types
| Column | Type | Nullable |
|--------|------|----------|
| payment_id | INTEGER | YES |
| customer_id | INTEGER | YES |
| staff_id | INTEGER | YES |
| rental_id | INTEGER | YES |
| amount | REAL | YES |
| payment_date | TEXT | YES |

### Descriptive Metrics
| Metric | Value |
|--------|-------|
| Total rows | 14,596 |
| Unique payment_id | 14,596 |
| Unique customers | 599 |
| Unique staff | 2 |
| Unique rental_ids referenced | 14,592 |
| Payments with amount = 0 | 24 |
| Min amount | $0.00 |
| Max amount | $11.99 |
| Avg amount | $4.20 |
| Earliest payment_date | 2007-02-14 |
| Latest payment_date | 2007-05-14 |

### Payment Volume and Revenue by Month
| Month | Payments | Revenue |
|-------|----------|---------|
| 2007-02 | 2,016 | $8,351.84 |
| 2007-03 | 5,644 | $23,886.56 |
| 2007-04 | 6,754 | $28,559.46 |
| 2007-05 | 182 | $514.18 |
| **Total** | **14,596** | **$61,312.04** |

---

## 2. Data Structure

### Primary Key
`payment_id` — fully unique across all 14,596 rows.

### Foreign Keys
- `customer_id` → **dvd_rental_store__customer** (`customer_id`)
- `staff_id` → **dvd_rental_store__staff** (`staff_id`)
- `rental_id` → **dvd_rental_store__rental** (`rental_id`)

### Orchestration
Payment dates are clustered in Feb–May 2007 — entirely separate from rental dates (2005–2006). This is a post-hoc payment extract. The May 2007 batch (182 records) corresponds to the 182 open rentals from 2006-02-14.

### Relationships
- Each payment row corresponds to one rental (`rental_id`), with **4 exceptions** where a single rental_id appears on multiple payment rows (e.g., rental_id=4591 has 5 payments). This may represent late fees or payment plan installments.

---

## 3. Data Behavior

- **Payment timing anomaly**: Rental dates are in 2005–2006, but `payment_date` values are all in 2007 (Feb–May). The ~18-month gap means `payment_date` does NOT reflect when the transaction occurred — it reflects when the payment was **recorded/processed** in the billing system. Use `rental_date` from the rental table for temporal analysis of when the business activity happened.
- **14,596 payments for 16,044 rentals**: 1,452 rentals lack any payment record — primarily the earliest May 2005 rentals and a few others.
- **182 open rentals (no return date) ALL have payment records**, with `payment_date = 2007-05-14` — the last batch date. These payments were pre-collected for rentals still outstanding.
- **4 rental_ids appear on multiple payment rows**: Only one confirmed case (rental_id=4591 with 5 payments totaling $12.95). This likely represents installment payments or late fees.
- **24 zero-amount payments**, all processed on 2007-05-14 — potentially representing promotional credits, waived fees, or data placeholders.
- Total revenue across all payments: **$61,312.04**.

---

## 4. Column Descriptions

| Column | Description |
|--------|-------------|
| payment_id | Unique integer identifier for each payment record. Primary key. |
| customer_id | Foreign key to `dvd_rental_store__customer`. The customer who made this payment. |
| staff_id | Foreign key to `dvd_rental_store__staff`. The staff member who collected or processed this payment. |
| rental_id | Foreign key to `dvd_rental_store__rental`. The rental transaction this payment is associated with. Most rentals have exactly one payment; 4 rental_ids appear on multiple payment rows. |
| amount | Payment amount in USD. Ranges from $0.00 to $11.99. Average: $4.20. 24 payments have $0.00 amount. |
| payment_date | Timestamp when the payment was recorded in the system. **Important: all payment dates are in 2007, while rentals occurred in 2005–2006. Do not use this field to infer when rental activity happened — use `rental_date` from the rental table instead.** |

## 5. Business Description

The `payment` table records all financial transactions associated with DVD rentals, capturing the amount charged per rental and the staff member who collected payment. It is the primary source for revenue analytics. Key caveats for analysts: (1) `payment_date` is not aligned with rental activity dates — all payments are recorded ~18 months after the rentals occurred, making it unsuitable for timeline analysis; (2) a small number of zero-amount payments exist (24), likely representing promotions or waivers; (3) most rentals have exactly one payment, but a handful have multiple payments that may represent late fees or installments. Total recorded revenue is **$61,312.04** across 14,596 payment transactions.
