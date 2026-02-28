
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

