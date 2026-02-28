/* ============================================================
   FILE: silver_crm_prd_info.sql
   LAYER: Silver
   TABLE: crm_prd_info

   PURPOSE:
   Clean and transform product data from Bronze layer
   before loading into Silver layer.

   MAIN TASKS:
   1. Validate primary keys
   2. Derive category and product keys
   3. Clean text values
   4. Fix NULL / invalid values
   5. Standardize product line values
   6. Correct invalid date ranges
   7. Insert cleaned data into Silver table

   T-SQL Concepts Used:
   - SUBSTRING()
   - REPLACE()
   - TRIM()
   - CASE expressions
   - Window Functions (LEAD)
   - ISNULL()
============================================================ */


-- ============================================================
-- STEP 1: VIEW RAW PRODUCT DATA FROM BRONZE
-- ============================================================

SELECT
    prd_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
FROM bronze.crm_prd_info;



-- ============================================================
-- STEP 2: CHECK FOR NULL OR DUPLICATE PRIMARY KEYS
-- Expectation: No rows returned
-- ============================================================

SELECT
    prd_id,
    COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;



-- ============================================================
-- STEP 3: DERIVE NEW COLUMNS FROM prd_key
--
-- prd_key contains multiple pieces of information.
-- Example structure:
--   CO-RF-XXXXX
--
-- We extract:
--   cat_id  → first 5 characters
--   prd_key → remaining product key
============================================================ */

SELECT
    prd_id,
    prd_key,

    -- Extract category id and standardize format
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,

    -- Extract only product portion of key
    SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,

    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt

FROM silver.crm_prd_info
WHERE SUBSTRING(prd_key, 7, LEN(prd_key))
      NOT IN (SELECT sls_prd_key FROM bronze.crm_sales_details);



-- ============================================================
-- STEP 4: CHECK CATEGORY MATCH WITH ERP DATA
-- ERP uses underscore format instead of dash.
============================================================ */

SELECT id
FROM bronze.erp_px_cat_g1v2;

SELECT sls_prd_key
FROM bronze.crm_sales_details;



-- ============================================================
-- STEP 5: CHECK FOR UNWANTED SPACES
-- Expectation: No results
============================================================ */

SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);



-- ============================================================
-- STEP 6: CHECK FOR NULL OR NEGATIVE COST VALUES
-- Expectation: No results
--
-- Replace NULL costs with 0 using ISNULL()
============================================================ */

SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0;



SELECT
    prd_id,
    prd_key,
    REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
    SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
    prd_nm,
    ISNULL(prd_cost,0) AS prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
FROM bronze.crm_prd_info;



-- ============================================================
-- STEP 7: STANDARDIZE PRODUCT LINE VALUES
--
-- Abbreviations found:
-- M, R, S, T
--
-- Converted into readable business values.
============================================================ */

SELECT DISTINCT prd_line
FROM silver.crm_prd_info;



SELECT
    prd_id,
    prd_key,
    REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
    SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
    prd_nm,
    ISNULL(prd_cost,0) AS prd_cost,

    CASE UPPER(TRIM(prd_line))
        WHEN 'M' THEN 'Mountain'
        WHEN 'R' THEN 'Road'
        WHEN 'S' THEN 'Other Sales'
        WHEN 'T' THEN 'Touring'
        ELSE 'n/a'
    END AS prd_line,

    prd_start_dt,
    prd_end_dt

FROM bronze.crm_prd_info;



-- ============================================================
-- STEP 8: CHECK INVALID DATE RANGES
-- End date should never be before start date
============================================================ */

SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;



-- ============================================================
-- STEP 9: FIX DATE RANGES USING WINDOW FUNCTION
--
-- LEAD() looks at the next record inside the same product.
-- End date = next start date - 1 day
============================================================ */

SELECT
    prd_id,
    prd_key,
    REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
    SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
    prd_nm,
    ISNULL(prd_cost,0) AS prd_cost,

    CASE UPPER(TRIM(prd_line))
        WHEN 'M' THEN 'Mountain'
        WHEN 'R' THEN 'Road'
        WHEN 'S' THEN 'Other Sales'
        WHEN 'T' THEN 'Touring'
        ELSE 'n/a'
    END AS prd_line,

    CAST(prd_start_dt AS DATE) AS prd_start_date,

    CAST(
        LEAD(prd_start_dt)
        OVER (PARTITION BY prd_key ORDER BY prd_start_dt ASC) - 1
        AS DATE
    ) AS prd_end_dt

FROM bronze.crm_prd_info;



-- ============================================================
-- STEP 10: PREPARE SILVER TABLE STRUCTURE
============================================================ */

CREATE OR ALTER TABLE silver.crm_prd_info
(
    prd_id          INT,
    cat_id          NVARCHAR(50),
    prd_key         NVARCHAR(50),
    prd_nm          NVARCHAR(50),
    prd_cost        INT,
    prd_line        NVARCHAR(50),
    prd_start_dt    DATE,
    prd_end_dt      DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO



-- ============================================================
-- STEP 11: INSERT CLEANED DATA INTO SILVER
============================================================ */

INSERT INTO silver.crm_prd_info
(
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
SELECT
    prd_id,
    REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
    SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
    prd_nm,
    ISNULL(prd_cost,0),

    CASE UPPER(TRIM(prd_line))
        WHEN 'M' THEN 'Mountain'
        WHEN 'R' THEN 'Road'
        WHEN 'S' THEN 'Other Sales'
        WHEN 'T' THEN 'Touring'
        ELSE 'n/a'
    END,

    CAST(prd_start_dt AS DATE),

    CAST(
        LEAD(prd_start_dt)
        OVER (PARTITION BY prd_key ORDER BY prd_start_dt ASC) - 1
        AS DATE
    )

FROM bronze.crm_prd_info;



-- ============================================================
-- STEP 12: VALIDATION
============================================================ */

SELECT *
FROM silver.crm_prd_info;