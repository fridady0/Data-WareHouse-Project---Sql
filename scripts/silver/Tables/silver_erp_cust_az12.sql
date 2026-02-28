/* ============================================================
   FILE: silver_erp_cust_az12.sql
   LAYER: Silver
   TABLE: erp_cust_az12

   PURPOSE:
   Clean ERP customer demographic data before loading
   into Silver layer.

   TRANSFORMATIONS:
   1. Standardize customer ID (remove 'NAS' prefix)
   2. Validate customer linkage with CRM data
   3. Fix invalid birth dates
   4. Standardize gender values
============================================================ */


-- ============================================================
-- STEP 1: STANDARDIZE CUSTOMER ID (cid)
--
-- Some records contain prefix 'NAS'.
-- Example:
--      NAS12345 → 12345
--
-- This ensures ERP customers can join with CRM customers.
-- ============================================================

SELECT
    CASE
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
        ELSE cid
    END AS cid,
    bdate,
    gen
FROM bronze.erp_cust_az12
WHERE
    CASE
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
        ELSE cid
    END NOT IN
    (
        SELECT DISTINCT cst_key
        FROM silver.crm_cust_info
    );



-- ============================================================
-- STEP 2: IDENTIFY OUT-OF-RANGE BIRTH DATES
--
-- Rules:
--  • No future birth dates
--  • Age should not exceed ~100 years
-- ============================================================

SELECT DISTINCT
    bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1926-01-01'
   OR bdate > GETDATE();



-- ============================================================
-- STEP 3: CORRECT INVALID BIRTH DATES
--
-- Future dates are converted to NULL.
-- ============================================================

SELECT
    CASE
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
        ELSE cid
    END AS cid,

    CASE
        WHEN bdate > GETDATE() THEN NULL
        ELSE bdate
    END AS bdate,

    gen
FROM bronze.erp_cust_az12;



-- ============================================================
-- STEP 4: DATA STANDARDIZATION (Gender Column)
--
-- Source values found:
-- NULL, F, Female, M, Male, blank spaces
-- ============================================================

SELECT DISTINCT gen
FROM bronze.erp_cust_az12;



-- ============================================================
-- STEP 5: STANDARDIZE GENDER VALUES
============================================================ */

SELECT
    CASE
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
        ELSE cid
    END AS cid,

    CASE
        WHEN bdate > GETDATE() THEN NULL
        ELSE bdate
    END AS bdate,

    CASE
        WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
        WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
        ELSE 'n/a'
    END AS gen

FROM bronze.erp_cust_az12;



-- ============================================================
-- STEP 6: INSERT CLEANED DATA INTO SILVER TABLE
============================================================ */

INSERT INTO silver.erp_cust_az12
(
    cid,
    bdate,
    gen
)
SELECT
    CASE
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
        ELSE cid
    END AS cid,

    CASE
        WHEN bdate > GETDATE() THEN NULL
        ELSE bdate
    END AS bdate,

    CASE
        WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
        WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
        ELSE 'n/a'
    END AS gen

FROM bronze.erp_cust_az12;



-- ============================================================
-- STEP 7: VALIDATION
============================================================ */

SELECT *
FROM silver.erp_cust_az12;s