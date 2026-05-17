---
name: source-discovery-skill
description: "Read files and tables and run queries against data warehouses to explore source databases and grasp their underlying business meanings and beahvior. Use when user mentions 'explore sources', 'explore data', 'discover database', or similars."
argument-hint: "[--data-warehouse] [--source-domain] [--source-schema]"
metadata:
  last-updated: 2026-05-08 11:59 UTC
  version: 1.0.0
  category: under development
---

## Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `--data-warehouse` | No | Name of the Data Warehouse connection the Agent should invoke |
| `--source-domain` | No | Data domain of the set of sources. The Agent should be able to find the pattern and read only the tables from the specified domain.  |
| `--source-schema` | No | Specific schema for the set of sources, if applicable.  |
| `--storage-path` | No | Path for logging and storing the interaction and discovery analysis results.  |


## Quick Guide

**The Agent Role**: You are a data developer who just ingested new source systems into the company's Data Warehouse, or need to understand or update the meaning and behavior of a set of datasources for a new project.

**What It Does**: Connects to the database and reads the files and tables of the source schema or source domain requested. Perform discovery analysis, such as listing columns, understand their meaning by data interpreation, collecting their data type, and others.

**Hooks**:
> **`/discovery-analysis`** defines the task for READING and EXPLORING the database. 

**Example**:
> You: /discovery-analysis
> Agent: Detects no domain neither schema was provided. By default, reads and computes the discovery information for all tables observed in the Data Warehouse. If the user has more than one DW connection, ask which one you should connect.

> You: /discovery-analysis zendesk
> Agent: Detects a domain was provided and searches for all source tables containing zendesk text pattern. Reads and computes the discovery information for all tables for Zendesk domain in the Data Warehouse. If the user has more than one DW connection, ask which one you should connect.

> You: /discovery-analysis snowflake salesforce
> Agent: Detects the user specified Snowflake as a Data Warehouse and salesforce as a domain. Connects to Snowflake tools, then reads and computes the discovery information for all tables for Salesforce domain in the Data Warehouse.


# Discovery Analysis
## Overview

This skill manages the project's design foundations — the `project/` files in `_references/` that define stack choices, conventions, conceptual design, metacommunication, and standards. It serves both initial configuration (after `/seed`) and ongoing design evolution.

---

## Discovery Steps

If no argument is provided, present a usage menu:

1. **Interactive** — walk through questions one by one (best for first-time users)
2. **From spec file** — provide a pre-filled spec file (best for experienced users)
3. **Generate blank spec** — create a skeleton to fill out offline

If the argument includes `--generate-spec`, skip the menu and go directly to Mode 4.
If a spec file path is provided, go directly to Mode 2.

### Step 1: Exploratory Analysis

1. **Columns and Data Types**: For each table, retrieve the names of all columns and their respective data types. 

2. **Descriptive Metrics**: For every column in each table, collect the following metrics based on the column's data type.
   - **Numeric Columns (Default):**
      - count: Number of non-null entries.
      - mean: The average value.
      - std: The standard deviation (how spread out the data is).
      - min / max: The smallest and largest values.
      - 25%, 50%, 75%: Percentiles (the 50% mark is the median).
   - **Object/Categorical Columns:**
      - count: Number of non-null entries.
      - unique: Number of distinct values.
      - top: The most frequent value (mode).
      - freq: How many times the 'top' value appears

3. **Categorical Values**: For each table and for each non-key categorical columns only, retrieve the possible distinct values. 
   
### Step 2: Data Structure

1. **Primary Keys**: For each table, identify the Primary Key or main unique identifier. If the table doesn't have a PK, iterate on which column combination makes the rows unique.

2. **Foreign Keys**: For each table, identify the Foreign Keys. If the table doesn't have FKs, skip this step.


3. **Orchestration**: For each table, identify the ETL update timestamp column, along with the scheduling strategy and refresh latency. If it is not possible to collect the information, skip this step. Some examples are: 
   > A source materialized as view

   > A source materialized as table, being droped and created on every daily run.

   > A source materialized as table, being droped and created every five hours.

   > An incremental source, which inserts and updates records from the last three hours. 

   > An incremental source, which inserts and updates records from the previous day.

4. **Relationships**: Once the PKs are laid out, identify the possibilities of Primary Key-Foreign Key combinations between the set of sources and their meaning. Some examples are: 
   > The source table transactions contains a FK column customer_id. The table customers contains a PK column customer_id. You identify that transactions and customers relates to each other through customer_id, and this relationship allows the user to identify each customer at the transaction level.

   > The source table purchase_orders contains a FK column supplier_id. The table suppliers contains a PK column supplier_id. You identify that purchase_orders and suppliers relate to each other through supplier_id, and this relationship allows the user to identify which supplier is responsible for each purchase order.

   > The source table support_tickets contains a FK column agent_id. The table support_agents contains a PK column agent_id. You identify that support_tickets and support_agents relate to each other through agent_id, and this relationship allows the user to identify the assigned support agent for each ticket.

### Step 3: Data Behavior
1. **Set of Records**: Identify the most important categorical features and filter a group of values to understand the underlying behavior of the data. Iterate on the possible values of the most important features, analyze the behavior and register your conclusions. The features might be date, customer, status, type, or others. Some examples are:
   > The source table transactions contain information on customer, payment type, status, date. To understand the data, you filter out the customer = "10000001", payment_type = "credit", date = "2025-01-01". With the results, you observe that the customer made 1 transaction that went throught the status "created", "authorized", "captured" and "chargebacked" in the same day. You also may observe that the transaction amount for chargebacked transactions is negative.

   > The source table inventory_forecast contains information on warehouse_id, product_category, forecast_date, and forecasted_amount. To understand the data, you filter out the warehouse_id = "WH102" and product_category = "electronics". You observe that the table contains the aggregated forecast predictions by day, and to compute the expected revenue for the next month you need to get the forecasted amount of the last day of the month.

   > The source table customer_support_metrics contains information on support_team, status, ticket_priority, resolution_date, and handling_time. To understand the data, you filter out the support_team = "LATAM_ENTERPRISE" and ticket_priority = "high". You observe that each row corresponds to a change in status of the same ticket, and that only when the status is "completed" the resolution_date is assigned.

### Step 4: Data Description

1. **Column Descriptions**: Using the knowledge obtained from the previous steps, write descriptions for every column in each source.

2. **Business Descriptions**: Using the knowledge observed in the data from the previous steps, connect them to business knowledge from the specified domain and write additional non-data related information.

### Step 5: Logging the Results
1. **File Creation**: Create a file for each source under the provided storage path. If path not provided, write under the default path. The file name should be written as "<name_of_source>__<current_timestamp>".
   - Default Path: /Users/julianasampar/Desktop/learning_dev/personal_dev/pyprojects/.claude/resources

   