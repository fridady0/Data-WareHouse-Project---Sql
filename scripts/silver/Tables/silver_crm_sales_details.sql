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



-- ============================================================
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



-- ============================================================
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



-- ============================================================
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



-- ============================================================
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



-- ============================================================
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



-- ============================================================
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



-- ============================================================
-- STEP 8: LOAD INTO SILVER TABLE
============================================================ */

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



-- ============================================================
-- STEP 9: VALIDATION
============================================================ */

SELECT *
FROM silver.crm_sales_details;