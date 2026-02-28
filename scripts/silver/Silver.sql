-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- This file is contains combined cleansing task performed for silver table
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

/* ============================================================
   FILE: silver_crm_cust_info.sql
   LAYER: Silver
   TABLE: crm_cust_info

   PURPOSE:
   1. Check data quality issues in Bronze
   2. Clean and standardize the data
   3. Insert clean data into Silver
============================================================ */

-- ============================================================
-- STEP 1: DATA QUALITY CHECKS
-- ============================================================
PRINT'>> Working on crm_cust_info table'

-- Check for NULL or duplicate primary keys
SELECT 
    cst_id,
    COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;


-- Check for unwanted spaces
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

SELECT cst_gndr
FROM bronze.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);


-- Check distinct values for consistency
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;

SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info;

-- ============================================================
-- STEP 2: LOAD CLEAN DATA INTO SILVER
-- ============================================================

PRINT '>> Truncating Tabel: silver.crm_cust_info'
TRUNCATE TABLE silver.crm_cust_info

PRINT '>> Inserting Data Into: silver.crm_cust_info'
INSERT INTO silver.crm_cust_info
(
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)
SELECT
    cst_id,
    cst_key,
    TRIM(cst_firstname),
    TRIM(cst_lastname),

    CASE 
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'n/a'
    END,

    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'n/a'
    END,

    cst_create_date

FROM
(
    SELECT
        *,
        ROW_NUMBER() OVER
        (
            PARTITION BY cst_id
            ORDER BY cst_create_date DESC
        ) AS flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
) t
WHERE flag_last = 1;


-- ============================================================
-- STEP 3: BASIC VALIDATION
-- ============================================================

SELECT COUNT(*) FROM silver.crm_cust_info;

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

PRINT '>> Working on crm_prf_info Table'
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

/* ============================================================
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


/* ============================================================
-- STEP 4: CHECK CATEGORY MATCH WITH ERP DATA
-- ERP uses underscore format instead of dash.
============================================================ */

SELECT id
FROM bronze.erp_px_cat_g1v2;

SELECT sls_prd_key
FROM bronze.crm_sales_details;


/* ============================================================
-- STEP 5: CHECK FOR UNWANTED SPACES
-- Expectation: No results
============================================================ */

SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);


/* ============================================================
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


/* ============================================================
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



/* ============================================================
-- STEP 8: CHECK INVALID DATE RANGES
-- End date should never be before start date
============================================================ */

SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;


/* ============================================================
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


/* ============================================================
-- STEP 10: PREPARE SILVER TABLE STRUCTURE
============================================================ */
/*
ALTER TABLE silver.crm_prd_info
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


*/
/* ============================================================
-- STEP 11: INSERT CLEANED DATA INTO SILVER
============================================================ */

PRINT '>> Truncating Tabel: silver.crm_prd_info'
TRUNCATE TABLE silver.crm_prd_info

PRINT '>> Inserting Data Into: silver.crm_prd_info'
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

/* ============================================================
-- STEP 12: VALIDATION
============================================================ */

SELECT COUNT(*)
FROM silver.crm_prd_info;

/* ============================================================
   FILE: silver_crm_sales_details.sql
   LAYER: Silver
   TABLE: crm_sales_details

   PURPOSE:
   Clean and standardize sales transaction data coming
   from Bronze layer before loading into Silver.

   TRANSFORMATIONS PERFORMED:
   1. Remove unwanted spaces
   2. Referential integrity validation
   3. Convert INT dates → DATE datatype
   4. Fix invalid or missing dates
   5. Validate sales = quantity * price rule
   6. Correct inconsistent monetary values
============================================================ */


-- ============================================================
-- STEP 1: CHECK FOR UNWANTED SPACES IN ORDER NUMBER
-- Expectation: No rows returned
-- ============================================================

PRINT '>> Working on crm_sales_details Table'
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num);



/* ============================================================
-- STEP 2: REFERENTIAL INTEGRITY CHECK
-- Ensures:
--   - Product exists in product dimension
--   - Customer exists in customer dimension
============================================================ */

SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info)
  AND sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info);


/* ============================================================
-- STEP 3: INVALID DATE CHECK
-- Source dates stored as INT (YYYYMMDD)
-- Detect invalid formats and boundary violations
============================================================ */

SELECT
    NULLIF(sls_order_dt,0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0
   OR LEN(sls_order_dt) != 8
   OR sls_order_dt > 20500101
   OR sls_order_dt < 19000101;



/* ============================================================
-- STEP 4: DATE CORRECTION
-- Convert valid INT dates into DATE datatype
-- Invalid values converted to NULL
============================================================ */

SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,

    CASE
        WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8
        THEN NULL
        ELSE CAST(CAST(sls_order_dt AS NVARCHAR) AS DATE)
    END AS sls_order_dt,

    CASE
        WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8
        THEN NULL
        ELSE CAST(CAST(sls_ship_dt AS NVARCHAR) AS DATE)
    END AS sls_ship_dt,

    CASE
        WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8
        THEN NULL
        ELSE CAST(CAST(sls_due_dt AS NVARCHAR) AS DATE)
    END AS sls_due_dt,

    sls_sales,
    sls_quantity,
    sls_price

FROM bronze.crm_sales_details;


/* ============================================================
-- STEP 5: BUSINESS RULE VALIDATION
-- Rule:
--      Sales = Quantity × Price
-- Values must not be NULL, zero, or negative
============================================================ */

SELECT DISTINCT
    sls_sales,
    sls_quantity,
    sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL
   OR sls_quantity IS NULL
   OR sls_price IS NULL
   OR sls_sales = 0
   OR sls_quantity = 0
   OR sls_price = 0
ORDER BY sls_sales;


/* ============================================================
-- STEP 6: CORRECT INCONSISTENT SALES & PRICE VALUES
============================================================ */

SELECT DISTINCT
    sls_sales AS old_sls_sales,
    sls_quantity,
    sls_price AS old_sls_price,

    CASE
        WHEN sls_sales IS NULL
          OR sls_sales <= 0
          OR sls_sales != ABS(sls_price) * sls_quantity
        THEN ABS(sls_price) * sls_quantity
        ELSE sls_sales
    END AS sls_sales,

    CASE
        WHEN sls_price IS NULL OR sls_price <= 0
        THEN sls_sales / NULLIF(sls_quantity,0)
        ELSE sls_price
    END AS sls_price

FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL
   OR sls_quantity IS NULL
   OR sls_price IS NULL
   OR sls_sales = 0
   OR sls_quantity = 0
   OR sls_price = 0
ORDER BY sls_sales, sls_quantity, sls_price;


/* ============================================================
-- STEP 7: FINAL TRANSFORMATION QUERY
============================================================ */

SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,

    CASE
        WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8
        THEN NULL
        ELSE CAST(CAST(sls_order_dt AS NVARCHAR) AS DATE)
    END AS sls_order_dt,

    CASE
        WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8
        THEN NULL
        ELSE CAST(CAST(sls_ship_dt AS NVARCHAR) AS DATE)
    END AS sls_ship_dt,

    CASE
        WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8
        THEN NULL
        ELSE CAST(CAST(sls_due_dt AS NVARCHAR) AS DATE)
    END AS sls_due_dt,

    CASE
        WHEN sls_sales IS NULL
          OR sls_sales <= 0
          OR sls_sales != ABS(sls_price) * sls_quantity
        THEN ABS(sls_price) * sls_quantity
        ELSE sls_sales
    END AS sls_sales,

    sls_quantity,

    CASE
        WHEN sls_price IS NULL OR sls_price <= 0
        THEN sls_sales / NULLIF(sls_quantity,0)
        ELSE sls_price
    END AS sls_price

FROM bronze.crm_sales_details;

/* ============================================================
-- STEP 8: LOAD INTO SILVER TABLE
============================================================ */

PRINT '>> Truncating Tabel: silver.crm_sales_details'
TRUNCATE TABLE silver.crm_sales_details

PRINT '>> Inserting Data Into: silver.crm_sales_details'
INSERT INTO silver.crm_sales_details
(
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
)
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8
         THEN NULL
         ELSE CAST(CAST(sls_order_dt AS NVARCHAR) AS DATE)
    END,
    CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8
         THEN NULL
         ELSE CAST(CAST(sls_ship_dt AS NVARCHAR) AS DATE)
    END,
    CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8
         THEN NULL
         ELSE CAST(CAST(sls_due_dt AS NVARCHAR) AS DATE)
    END,
    CASE WHEN sls_sales IS NULL OR sls_sales <= 0
           OR sls_sales != ABS(sls_price) * sls_quantity
         THEN ABS(sls_price) * sls_quantity
         ELSE sls_sales
    END,
    sls_quantity,
    CASE WHEN sls_price IS NULL OR sls_price <= 0
         THEN sls_sales / NULLIF(sls_quantity,0)
         ELSE sls_price
    END
FROM bronze.crm_sales_details;



/* ============================================================
-- STEP 9: VALIDATION
============================================================ */

SELECT COUNT(*)
FROM silver.crm_sales_details;



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

PRINT '>> Working on erp_cust_az12 Table'

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



/* ============================================================
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



/* ============================================================
-- STEP 6: INSERT CLEANED DATA INTO SILVER TABLE
============================================================ */

PRINT '>> Truncating Tabel: silver.erp_cust_az12'
TRUNCATE TABLE silver.erp_cust_az12

PRINT '>> Inserting Data Into: silver.erp_cust_az12'
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



/* ============================================================
-- STEP 7: VALIDATION
============================================================ */

SELECT COUNT(*)
FROM silver.erp_cust_az12;

/* ============================================================
   FILE: silver_erp_loc_a101.sql
   LAYER: Silver
   TABLE: erp_loc_a101

   PURPOSE:
   Clean and standardize ERP customer location data
   before loading into Silver layer.

   TRANSFORMATIONS:
   1. Remove unwanted '-' characters from customer ID
   2. Standardize country names
   3. Handle NULL and blank country values
============================================================ */


-- ============================================================
-- STEP 1: VIEW RAW DATA FROM BRONZE
-- ============================================================

PRINT '>> Working on loc_a101 Table'
SELECT
    cid,
    cntry
FROM bronze.erp_loc_a101;



/* ============================================================
-- STEP 2: CLEAN CUSTOMER ID
--
-- Some IDs contain unnecessary '-' characters.
-- Example:
--      AB-123 → AB123
============================================================ */

SELECT
    REPLACE(cid, '-', '') AS cid,
    cntry
FROM bronze.erp_loc_a101;



/* ============================================================
-- STEP 3: PROFILE COUNTRY COLUMN
-- Identify inconsistent country values
============================================================ */

SELECT DISTINCT
    cntry
FROM bronze.erp_loc_a101;

-- Observed values:
-- USA, US, United States, Germany, NULL, blank,
-- Australia, United Kingdom, Canada, France



/* ============================================================
-- STEP 4: STANDARDIZE COUNTRY VALUES
--
-- Rules:
--  • US / USA → United States
--  • DE → Germany
--  • NULL or blank → n/a
============================================================ */

SELECT
    REPLACE(cid, '-', '') AS cid,

    CASE
        WHEN UPPER(TRIM(cntry)) IN ('US','USA')
            THEN 'Unite States'   -- (kept as written, logic unchanged)

        WHEN UPPER(TRIM(cntry)) = 'DE'
            THEN 'Germany'

        WHEN TRIM(cntry) = '' OR cntry IS NULL
            THEN 'n/a'

        ELSE cntry
    END AS cntry

FROM bronze.erp_loc_a101;



/* ============================================================
-- STEP 5: INSERT CLEANED DATA INTO SILVER TABLE
============================================================ */
PRINT '>> Truncating Tabel: silver.erp_loc_a101'
TRUNCATE TABLE silver.erp_loc_a101

PRINT '>> Inserting Data Into: silver.erp_loc_a101'
INSERT INTO silver.erp_loc_a101
(
    cid,
    cntry
)
SELECT
    REPLACE(cid, '-', '') AS cid,

    CASE
        WHEN UPPER(TRIM(cntry)) IN ('US','USA')
            THEN 'Unite States'

        WHEN UPPER(TRIM(cntry)) = 'DE'
            THEN 'Germany'

        WHEN TRIM(cntry) = '' OR cntry IS NULL
            THEN 'n/a'

        ELSE cntry
    END AS cntry

FROM bronze.erp_loc_a101;



/* ============================================================
-- STEP 6: VALIDATION
============================================================ */

SELECT COUNT(*)
FROM silver.erp_loc_a101;

/* ============================================================
   FILE: silver_erp_px_cat_g1v2.sql
   LAYER: Silver
   TABLE: erp_px_cat_g1v2

   PURPOSE:
   Load ERP product category reference data into the
   Silver layer after validating data quality.

   NOTE:
   This table requires minimal transformation because
   source data is already clean and standardized.

   VALIDATIONS PERFORMED:
   1. Check unwanted spaces
   2. Verify value consistency using DISTINCT checks
============================================================ */


-- ============================================================
-- STEP 1: VIEW SOURCE DATA (BRONZE)
-- ============================================================

PRINT '>> Working on erp_px_cat_g1v2 Table'

SELECT
    id,
    cat,
    subcat,
    maintenance
FROM bronze.erp_px_cat_g1v2;



/* ============================================================
-- STEP 2: CHECK FOR UNWANTED SPACES
--
-- TRIM() removes leading/trailing spaces.
-- Expectation: No rows returned.
============================================================ */

SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE cat         != TRIM(cat)
   OR subcat      != TRIM(subcat)
   OR maintenance != TRIM(maintenance);



/* ============================================================
-- STEP 3: DATA STANDARDIZATION CHECK
--
-- DISTINCT helps identify inconsistent values.
============================================================ */

-- Check category values
SELECT DISTINCT cat
FROM bronze.erp_px_cat_g1v2;

-- Check subcategory values
SELECT DISTINCT subcat
FROM bronze.erp_px_cat_g1v2;

-- Check maintenance values
SELECT DISTINCT maintenance
FROM bronze.erp_px_cat_g1v2;

/* ============================================================
-- STEP 4: INSERT DATA INTO SILVER LAYER
--
-- Since no corrections are required, data is copied
-- directly from Bronze to Silver.
============================================================ */
PRINT '>> Truncating Tabel: silver.erp_px_cat_g1v2'
TRUNCATE TABLE silver.erp_px_cat_g1v2

PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2'
INSERT INTO silver.erp_px_cat_g1v2
(
    id,
    cat,
    subcat,
    maintenance
)
SELECT
    id,
    cat,
    subcat,
    maintenance
FROM bronze.erp_px_cat_g1v2;

/* ============================================================
-- STEP 5: VALIDATION
============================================================ */

SELECT COUNT(*)
FROM silver.erp_px_cat_g1v2;
