/* ============================================================
   SILVER LAYER – Transformation Table 
   Environment : SQL Server
   Architecture: Medallion (Silver = Data Transformation layer)
   Strategy    : Full load using TRUNCATE + BULK INSERT
============================================================ */


/* ============================================================
   SECTION 1: CRM SOURCE TABLES
   Purpose: Raw extraction from CRM system
============================================================ */

-- Recreate CRM customer master table (raw structure preserved)
IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;
GO

CREATE TABLE silver.crm_cust_info
(
    cst_id              INT,
    cst_key             NVARCHAR(50),
    cst_firstname       NVARCHAR(50),
    cst_lastname        NVARCHAR(50),
    cst_marital_status  NVARCHAR(50),
    cst_gndr            NVARCHAR(50),
    cst_create_date     DATE,
    -- dwh_create_date is metadata column - made by data engineer for extra information 
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO


-- Recreate CRM product master table
IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;
GO

CREATE TABLE silver.crm_prd_info
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


-- Recreate CRM transactional sales table
-- Note: Date fields stored as INT (source format: YYYYMMDD)
IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;
GO

CREATE TABLE silver.crm_sales_details
(
    sls_ord_num     NVARCHAR(50),
    sls_prd_key     NVARCHAR(50),
    sls_cust_id     INT,
    sls_order_dt    DATE,
    sls_ship_dt     DATE,
    sls_due_dt      DATE,
    sls_sales       INT,
    sls_quantity    INT,
    sls_price       INT,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO


/* ============================================================
   SECTION 2: ERP SOURCE TABLES
   Purpose: Supporting master and reference data from ERP
============================================================ */

-- ERP customer demographic attributes
IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE silver.erp_cust_az12;
GO

CREATE TABLE silver.erp_cust_az12
(
    cid     NVARCHAR(50),   -- ERP customer identifier
    bdate   DATE,
    gen     NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO


-- ERP customer location mapping
IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101;
GO

CREATE TABLE silver.erp_loc_a101
(
    cid     NVARCHAR(50),   -- Used later for dimensional joins
    cntry   NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO


-- ERP product category reference table
IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE silver.erp_px_cat_g1v2;
GO

CREATE TABLE silver.erp_px_cat_g1v2
(
    id          NVARCHAR(50),   -- Product identifier
    cat         NVARCHAR(50),
    subcat      NVARCHAR(50),
    maintenance NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

