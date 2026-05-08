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
| `--log-path` | No | Path for logging and storing the interaction and discovery analysis results.  |


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
   
### Step 2: Data Structure

1. **Primary Keys**: For each table, identify the Primary Key or main unique identifier. If the table doesn't have a PK, iterate on which column combination makes the rows unique.

2. **Foreign Keys**: For each table, identify the Foreign Keys. If the table doesn't have FKs, skip this step.

3. **Categorical Values**: For each table and for each non-key categorical columns only, retrieve the possible distinct values. 

3. **Orchestration**: For each table, identify the ETL update timestamp column, along with the scheduling strategy and refresh latency. If it is not possible to collect the information, skip this step. Some examples are: 
   - A source materialized as table, being droped and created on every daily run.
   - A source materialized as table, being droped and created every five hours.
   - An incremental source, which inserts and updates records from the last three hours. 
   - An incremental source, which inserts and updates records from the previous day.

4. **Relationships**: Once the PKs are laid out, identify the possibilities of Primary Key-Foreign Key combinations between the set of sources and their meaning. Some examples are: 
   - The source table transactions contains a FK column customer_id. The table customers contains a PK column customer_id. You identify that transactions and customers relates to each other through customer_id, and this relationship allows the user to identify each customer at the transaction level.

### Step 3: Data Behavior
4. **Filter an Example**: 

### Step 4: Data Description

5. **Mandatory conceptual design**: 