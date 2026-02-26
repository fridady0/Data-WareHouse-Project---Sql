/* ============================================================
  Store Procedure : Load Bronze Layer (Source -> Bronze)

  Script Purpose: This stored procedure loads data into the 
  'bronze' schema form external CSV files. It performs FULL LOAD.
/* ============================================================
   FILE: proc_load_bronze.sql
   LAYER: Bronze
   TYPE : Stored Procedure (Data Loading)

   WHAT THIS FILE DOES:
   This stored procedure loads raw CSV files into
   Bronze layer tables.

   It:
   1. Clears old data (TRUNCATE)
   2. Inserts fresh data (BULK INSERT)
   3. Measures how long each step takes
   4. Handles errors safely using TRY...CATCH

   This script uses T-SQL features like:
   - CREATE OR ALTER PROCEDURE
   - TRY...CATCH (Error Handling)
   - BULK INSERT
   - Variables and DATEDIFF
============================================================ */


CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN

    /* ---------------------------------------------------------
       STEP 1: Declare Variables (T-SQL Concept)
       These variables are used to measure time taken.
    --------------------------------------------------------- */
    DECLARE @start_time        DATETIME,
            @end_time          DATETIME,
            @batch_start_time  DATETIME,
            @batch_end_time    DATETIME;

    -- Start timing the full Bronze load
    SET @batch_start_time = GETDATE();


    /* ---------------------------------------------------------
       TRY...CATCH (T-SQL Error Handling)
       If anything fails inside TRY,
       control automatically goes to CATCH block.
    --------------------------------------------------------- */
    BEGIN TRY

        PRINT '=====================================================';
        PRINT 'Starting Bronze Layer Load';
        PRINT '=====================================================';


        /* =====================================================
           PART 1: LOAD CRM TABLES
        ===================================================== */

        PRINT 'Loading CRM Tables...';


        /* ---------------- CRM Customer ----------------
           Process:
           1. Remove old data
           2. Load fresh CSV file
        ------------------------------------------------ */

        SET @start_time = GETDATE();  -- Start timer

        PRINT 'Clearing old data from bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;  -- Faster than DELETE

        PRINT 'Loading data into bronze.crm_cust_info';

        /* BULK INSERT (T-SQL Feature)
           Used to quickly load large CSV files into SQL Server */
        BULK INSERT bronze.crm_cust_info
        FROM 'F:\Projects\SQL Projects\Data Warehousing Project\Barra  Github Material\datasets\source_crm\cust_info.csv'
        WITH
        (
            FIRSTROW        = 2,   -- Skip header row
            FIELDTERMINATOR = ',', -- File is comma separated
            TABLOCK              -- Locks table for faster insert
        );

        SET @end_time = GETDATE();

        PRINT 'Time taken (seconds): '
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR);


        /* ---------------- CRM Product ---------------- */

        SET @start_time = GETDATE();

        PRINT 'Clearing old data from bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT 'Loading data into bronze.crm_prd_info';

        BULK INSERT bronze.crm_prd_info
        FROM 'F:\Projects\SQL Projects\Data Warehousing Project\Barra  Github Material\datasets\source_crm\prd_info.csv'
        WITH
        (
            FIRSTROW        = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();

        PRINT 'Time taken (seconds): '
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR);


        /* ---------------- CRM Sales ---------------- */

        SET @start_time = GETDATE();

        PRINT 'Clearing old data from bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT 'Loading data into bronze.crm_sales_details';

        BULK INSERT bronze.crm_sales_details
        FROM 'F:\Projects\SQL Projects\Data Warehousing Project\Barra  Github Material\datasets\source_crm\sales_details.csv'
        WITH
        (
            FIRSTROW        = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();

        PRINT 'Time taken (seconds): '
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR);



        /* =====================================================
           PART 2: LOAD ERP TABLES
        ===================================================== */

        PRINT 'Loading ERP Tables...';


        /* ---------------- ERP Customer ---------------- */

        SET @start_time = GETDATE();

        PRINT 'Clearing old data from bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT 'Loading data into bronze.erp_cust_az12';

        BULK INSERT bronze.erp_cust_az12
        FROM 'F:\Projects\SQL Projects\Data Warehousing Project\Barra  Github Material\datasets\source_erp\CUST_AZ12.csv'
        WITH
        (
            FIRSTROW        = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();

        PRINT 'Time taken (seconds): '
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR);


        /* ---------------- ERP Location ---------------- */

        SET @start_time = GETDATE();

        PRINT 'Clearing old data from bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT 'Loading data into bronze.erp_loc_a101';

        BULK INSERT bronze.erp_loc_a101
        FROM 'F:\Projects\SQL Projects\Data Warehousing Project\Barra  Github Material\datasets\source_erp\LOC_A101.csv'
        WITH
        (
            FIRSTROW        = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();

        PRINT 'Time taken (seconds): '
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR);


        /* ---------------- ERP Product Category ---------------- */

        SET @start_time = GETDATE();

        PRINT 'Clearing old data from bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT 'Loading data into bronze.erp_px_cat_g1v2';

        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'F:\Projects\SQL Projects\Data Warehousing Project\Barra  Github Material\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH
        (
            FIRSTROW        = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();

        PRINT 'Time taken (seconds): '
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR);



        /* =====================================================
           FINAL STEP: Total Time for Entire Bronze Load
        ===================================================== */

        SET @batch_end_time = GETDATE();

        PRINT 'Total Bronze Load Time (seconds): '
              + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR);

        PRINT 'Bronze Layer Load Completed Successfully';


    END TRY


    BEGIN CATCH

        /* -----------------------------------------------------
           CATCH Block (T-SQL Error Handling)
           If any error happens above,
           SQL Server jumps here automatically.
        ----------------------------------------------------- */

        PRINT '=====================================================';
        PRINT 'Error occurred during Bronze Load';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State  : ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '=====================================================';

    END CATCH;

END;
GO


-- Run the stored procedure
EXEC bronze.load_bronze;
