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

SELECT
    cid,
    cntry
FROM bronze.erp_loc_a101;



-- ============================================================
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



-- ============================================================
-- STEP 3: PROFILE COUNTRY COLUMN
-- Identify inconsistent country values
============================================================ */

SELECT DISTINCT
    cntry
FROM bronze.erp_loc_a101;

-- Observed values:
-- USA, US, United States, Germany, NULL, blank,
-- Australia, United Kingdom, Canada, France



-- ============================================================
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



-- ============================================================
-- STEP 5: INSERT CLEANED DATA INTO SILVER TABLE
============================================================ */

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



-- ============================================================
-- STEP 6: VALIDATION
============================================================ */

SELECT *
FROM silver.erp_loc_a101;