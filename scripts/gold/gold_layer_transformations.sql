/* ==========================================================================================
   PROJECT       : DATA WAREHOUSE PROJECT
   LAYER         : GOLD
   FILE          : gold_layer_transformations.sql

   PURPOSE
   ------------------------------------------------------------------------------------------
   This script is used for:

        • Data integration across Silver tables
        • Data quality validation
        • Testing transformation logic
        • Verifying uniqueness & relationships

   NOTE:
   This script DOES NOT create objects.
   It is only used to test logic before creating final Gold views.
==========================================================================================*/


/* ==========================================================================================
   SECTION 1 — CUSTOMER DATA INTEGRATION
==========================================================================================*/

SELECT
    ci.cst_id,
    ci.cst_key,
    ci.cst_firstname,
    ci.cst_lastname,
    ci.cst_marital_status,
    ci.cst_gndr,
    ci.cst_create_date,
    ca.bdate,
    ca.gen,
    la.cntry
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS la
    ON ci.cst_key = la.cid;



/* ==========================================================================================
   CHECK FOR DUPLICATE CUSTOMERS
==========================================================================================*/

SELECT
    cst_id,
    COUNT(*)
FROM
(
    SELECT
        ci.cst_id,
        ci.cst_key,
        ci.cst_firstname,
        ci.cst_lastname,
        ci.cst_marital_status,
        ci.cst_gndr,
        ci.cst_create_date,
        ca.bdate,
        ca.gen,
        la.cntry
    FROM silver.crm_cust_info AS ci
    LEFT JOIN silver.erp_cust_az12 AS ca
        ON ci.cst_key = ca.cid
    LEFT JOIN silver.erp_loc_a101 AS la
        ON ci.cst_key = la.cid
) t
GROUP BY cst_id
HAVING COUNT(*) > 1;



/* ==========================================================================================
   GENDER DATA INTEGRATION
   CRM is treated as the master source.
==========================================================================================*/

SELECT DISTINCT
    ci.cst_gndr,
    ca.gen,

    CASE
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen,'n/a')
    END AS new_gender

FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS la
    ON ci.cst_key = la.cid
ORDER BY 1,2;



/* ==========================================================================================
   FINAL CUSTOMER DATASET (TEST VERSION)
==========================================================================================*/

SELECT
    ci.cst_id,
    ci.cst_key,
    ci.cst_firstname,
    ci.cst_lastname,
    ci.cst_marital_status,

    CASE
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen,'n/a')
    END AS gender,

    ci.cst_create_date,
    ca.bdate,
    la.cntry

FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS la
    ON ci.cst_key = la.cid;



/* ==========================================================================================
   SECTION 2 — PRODUCT DATA INTEGRATION
==========================================================================================*/

SELECT
    pn.prd_id,
    pn.cat_id,
    pn.prd_key,
    pn.prd_nm,
    pn.prd_cost,
    pn.prd_line,
    pn.prd_start_dt,
    pc.cat,
    pc.subcat,
    pc.maintenance

FROM silver.crm_prd_info AS pn
LEFT JOIN silver.erp_px_cat_g1v2 AS pc
    ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL;



/* ==========================================================================================
   CHECK PRODUCT KEY UNIQUENESS
==========================================================================================*/

SELECT
    prd_key,
    COUNT(*)
FROM
(
    SELECT
        pn.prd_id,
        pn.cat_id,
        pn.prd_key,
        pn.prd_nm,
        pn.prd_cost,
        pn.prd_line,
        pn.prd_start_dt,
        pc.cat,
        pc.subcat,
        pc.maintenance
    FROM silver.crm_prd_info AS pn
    LEFT JOIN silver.erp_px_cat_g1v2 AS pc
        ON pn.cat_id = pc.id
    WHERE prd_end_dt IS NULL
) t
GROUP BY prd_key
HAVING COUNT(*) > 1;



/* ==========================================================================================
   SECTION 3 — SALES FACT PREVIEW
==========================================================================================*/

SELECT
    sd.sls_ord_num AS order_number,
    pr.product_key,
    cu.customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS ship_date,
    sd.sls_due_dt   AS due_date,
    sd.sls_sales    AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price    AS price

FROM silver.crm_sales_details AS sd
LEFT JOIN gold.dim_products AS pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers AS cu
    ON sd.sls_cust_id = cu.customer_id;
