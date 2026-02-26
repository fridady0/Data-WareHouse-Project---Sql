/* ============================================================
   DDL Script : Create Bronze Table
   Purpose : This script creates table in 'bronze' schema, 
   dropping existing tables if they already exist.
   Run this Script to re-define the DDL Structure of 'bronze'
   table.
============================================================ */
/* ============================================================
   FILE: 01_bronze_ddl.sql
   LAYER: Bronze
   TYPE : DDL (Table Creation Only)

   DESCRIPTION:
   This script creates all Bronze layer tables.

   The Bronze layer stores raw data exactly as received
   from source systems (CRM and ERP).

   No transformations, validations, or business rules
   are applied at this stage.
============================================================ */

/* ============================================================
   SECTION 1: CRM SOURCE TABLES
   These tables store raw data coming directly from the
   CRM system.
============================================================ */


-- ------------------------------------------------------------
-- Table: bronze.crm_cust_info
-- Purpose: Stores basic customer details from CRM.
-- This is raw source data and is not cleaned or modified.
-- ------------------------------------------------------------
IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;
GO

CREATE TABLE bronze.crm_cust_info
(
    cst_id              INT,            -- Internal customer ID
    cst_key             NVARCHAR(50),   -- Business/customer key
    cst_firstname       NVARCHAR(50),
    cst_lastname        NVARCHAR(50),
    cst_marital_status  NVARCHAR(50),
    cst_gndr            NVARCHAR(50),
    cst_create_date     DATE            -- Date customer record was created
);
GO


-- ------------------------------------------------------------
-- Table: bronze.crm_prd_info
-- Purpose: Stores product master data from CRM.
-- ------------------------------------------------------------
IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;
GO

CREATE TABLE bronze.crm_prd_info
(
    prd_id          INT,            -- Internal product ID
    prd_key         NVARCHAR(50),   -- Business/product key
    prd_nm          NVARCHAR(50),   -- Product name
    prd_cost        INT,            -- Product cost from source
    prd_line        NVARCHAR(50),   -- Product line/category
    prd_start_dt    DATETIME,       -- Product availability start date
    prd_end_dt      DATETIME        -- Product availability end date
);
GO


-- ------------------------------------------------------------
-- Table: bronze.crm_sales_details
-- Purpose: Stores sales transactions from CRM.
--
-- Note:
-- Date columns are stored as INT because that is how
-- they are received from the source system (YYYYMMDD).
-- These will be converted in later layers.
-- ------------------------------------------------------------
IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;
GO

CREATE TABLE bronze.crm_sales_details
(
    sls_ord_num     NVARCHAR(50),   -- Sales order number
    sls_prd_key     NVARCHAR(50),   -- Product key
    sls_cust_id     INT,            -- Customer ID
    sls_order_dt    INT,            -- Order date (format: YYYYMMDD)
    sls_ship_dt     INT,            -- Ship date (format: YYYYMMDD)
    sls_due_dt      INT,            -- Due date (format: YYYYMMDD)
    sls_sales       INT,            -- Total sales amount
    sls_quantity    INT,            -- Quantity sold
    sls_price       INT             -- Unit price
);
GO



/* ============================================================
   SECTION 2: ERP SOURCE TABLES
   These tables store supporting data coming from
   the ERP system.
============================================================ */


-- ------------------------------------------------------------
-- Table: bronze.erp_cust_az12
-- Purpose: Stores additional customer details from ERP.
-- This will later be combined with CRM customer data.
-- ------------------------------------------------------------
IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE bronze.erp_cust_az12;
GO

CREATE TABLE bronze.erp_cust_az12
(
    cid     NVARCHAR(50),   -- ERP customer identifier
    bdate   DATE,           -- Customer birth date
    gen     NVARCHAR(50)    -- Customer gender
);
GO


-- ------------------------------------------------------------
-- Table: bronze.erp_loc_a101
-- Purpose: Stores customer country/location details.
-- Used later for building dimension tables.
-- ------------------------------------------------------------
IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE bronze.erp_loc_a101;
GO

CREATE TABLE bronze.erp_loc_a101
(
    cid     NVARCHAR(50),   -- ERP customer identifier
    cntry   NVARCHAR(50)    -- Country name
);
GO


-- ------------------------------------------------------------
-- Table: bronze.erp_px_cat_g1v2
-- Purpose: Stores product category and subcategory
-- information from ERP.
-- ------------------------------------------------------------
IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE bronze.erp_px_cat_g1v2;
GO

CREATE TABLE bronze.erp_px_cat_g1v2
(
    id          NVARCHAR(50),   -- Product identifier
    cat         NVARCHAR(50),   -- Main category
    subcat      NVARCHAR(50),   -- Sub category
    maintenance NVARCHAR(50)    -- Maintenance type/details
);
GO
