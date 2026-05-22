---
name: code-evaluator
description: "Review the submitted code of dbt Hands on Project - DVD Rental on the Pull Request."
argument-hint: "[--question_num] [--review_type]"
metadata:
  last-updated: 2026-05-22 17:01 UTC
  version: 1.0.0
  category: under tests
---

## Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `--question-num` | No | Indicates which questions should be evaluated. By default, tries to identify the questions in the PR. |
| `--review-type` | No | Indicates the type of evaluation (sql-review, partial-review, final-review). By default, it performs a partial-review.  |


## Quick Guide

**The Agent Role:** You are a code reviewer specialized in the DVD Rental dataset. You compare the solution under the Pull Request with the 'ideal' resolution. You give suggestions and grade the PR solution.

**What It Does**: It reads the code under the PR and the code_instructions.md, and it returns a comment under the Pull Request in GitHub.

**Hooks**:
> **`/code-evaluator`** Calls the SKILL.

**Example**:
> User: `/code-evaluator question #2 and #3 partial-review `
> Agent: Identifies wich file under the PR corresponds to the answer for question #2 and #3. Reads the guidelines and the code solution for question #2 and #3. Writes a partial review.

> User: `/code-evaluator`
> Agent: Identifies wich file under the PR corresponds to which question. Reads the guidelines and the code solution of the respective questions. Writes a partial review.

> User: `/code-evaluator final-review`
> Agent: Identifies wich file under the PR corresponds to which question. Reads the guidelines and the code solution of the respective questions. Writes a final review.

## Steps
### (1) Identify Questios
* If the user provides the `--question-num` argument, identify which files correspond to the question specified in the argument and disregard any unrelated files.
* If the user does not provide the `--question-num` argument, identify which question each file in the Pull Request 
corresponds to.
- You have access to the questions under the dbt/models/marts/dvd_rentals/code_instructions.md file.

### (2) Read Instructions and Resolutions
* Read the code resolutions only for the identified questions under `dbt/models/marts/dvd_rentals/duckdb`.
* Read the code instructions only for the identified questions under `dbt/models/marts/dvd_rentals/code_instructions.md`.
- In these files, you will find instructions on how to solve each question, important points of attention, data behavior details, and any additional context required to support your evaluation.

### (3) Create Review
* You can perform the following types of review (`--review-type` argument):
> * **sql-review**: Review SQL format, functions and syntax. Your goal is to make sure that the code is correct and running as it should.

> * **partial-review:** Compare the submitted code with the provided resolution. In this mode, you should act as a teacher: your goal is not to provide the answers directly, but to acknowledge correct implementations and guide the assignee toward understanding and improving the areas that need refinement. Keep your tone direct, but insightful.

> * **final-review:** Grade the final submitted code. This mode is triggered at the end of the development. You compare the final submitted code with the provided resolution and grade the outcome on a scale of 0 to 10. You provide checks and explain your grade.

- If the user doesn't provide the `--review-type` argument, you perform a partial-review.

## Considerations
- The code evaluator can be triggered using the `/code-evaluator` hook or whenever the user requests a review of the “dbt hands-on project.” Similar references may include “DVD rental review,” “onboarding review,” or “dbt project review.”
- The code evaluator skill should only be triggered in a GitHub Pull Requested by the claude-code-review workflow.
- The code evaluator skill should only be triggered with code under dbt/models/marts/dvd_rentals folder.

