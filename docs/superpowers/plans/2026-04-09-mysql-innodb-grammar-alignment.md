# MySQL InnoDB Grammar Alignment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Align the MySQL grammar with approved InnoDB 8.0.41-compatible syntax from the supplied dstore grammar while keeping dstore-only syntax isolated.

**Architecture:** Extend the existing MySQL grammar in place, preserving current query roots and key functions where possible. Use string-level regression tests to pin the intended grammar fragments and avoid introducing execution-only grammar that is not valid for InnoDB.

**Tech Stack:** Java, JUnit 5, `.yy` grammar files

---

### Task 1: Add failing grammar coverage

**Files:**
- Modify: `src/test/java/dbradar/mysql/TestMySQLGrammarAndTypeSupport.java`

- [ ] **Step 1: Add assertions for missing approved grammar**

Add assertions that require:
- `algorithm` to include `INPLACE`, `INSTANT`, `COPY`, `DEFAULT`
- `lock_clause` and `row_format_clause`
- `alter_table_multi_add_column`
- `alter_table_multi_drop_column`
- `for_lock_clause` and `lock_wait_option`
- `SELECT all_columns FROM system_information`
- `INFORMATION_SCHEMA.DSTORE_*` system-table entries

- [ ] **Step 2: Run the focused test and verify it fails**

Run: `./gradlew test --tests dbradar.mysql.TestMySQLGrammarAndTypeSupport`
Expected: FAIL because the current grammar does not yet contain the new fragments.

### Task 2: Implement the grammar changes

**Files:**
- Modify: `src/main/resources/dbradar/mysql/mysql.grammar.yy`

- [ ] **Step 1: Add the approved InnoDB grammar fragments**

Extend the grammar with:
- `for_lock_clause` and `lock_wait_option`
- `alter_table_multi_add_column`
- `alter_table_multi_drop_column`
- `lock_clause`
- `row_format_clause`
- broader `algorithm`
- `system_information` and `SELECT all_columns FROM system_information`

- [ ] **Step 2: Keep unsupported or non-InnoDB grammar out of the executable path**

Do not add unsupported InnoDB items such as `INSERT_METHOD`, `PACK_KEYS`, `DELAY_KEY_WRITE`, or foreign-key actions that do not run cleanly on InnoDB 8.0.41.

- [ ] **Step 3: Run the focused test and verify it passes**

Run: `./gradlew test --tests dbradar.mysql.TestMySQLGrammarAndTypeSupport`
Expected: PASS

### Task 3: Validate and summarize

**Files:**
- Modify: `src/test/java/dbradar/mysql/TestMySQLGrammarAndTypeSupport.java` (only if assertions need tightening)

- [ ] **Step 1: Run adjacent MySQL grammar tests**

Run: `./gradlew test --tests dbradar.mysql.TestMySQLVirtualColumnCoverage --tests dbradar.mysql.TestMySQLStressGenerationRegression`
Expected: PASS, or skip integration-heavy cases if a local MySQL instance is unavailable.

- [ ] **Step 2: Summarize the final split**

Produce two outputs:
- InnoDB-compatible grammar that was merged
- dstore-only grammar that remains separate and needs your later integration decision
