/*
================================================================================
Stored Procedure : sliver.load_sliver
Author           : Prateek Thakur
Database         : datawarehouse
Layer            : Silver Layer (Data Warehouse)
Created Date     : <Add Date>

Description:
This stored procedure performs data transformation, cleansing, and loading
of data from the Bronze layer into the Silver layer tables in the
datawarehouse database.

The Silver layer contains cleaned, standardized, and deduplicated data
ready for analytical processing.

Operations Performed:
1. Truncates existing Silver layer tables.
2. Extracts data from Bronze layer tables.
3. Performs data cleansing and transformation including:
   - Removing leading/trailing spaces
   - Standardizing categorical values (Gender, Marital Status, Country)
   - Removing hidden characters (newline / carriage return)
   - Handling NULL and invalid values
   - Deduplicating records using ROW_NUMBER()
   - Calculating product end dates using LEAD()
   - Correcting sales and price inconsistencies
4. Inserts the transformed data into Silver layer tables.

Source Tables (Bronze Layer):
- bronze.crm_cust_info
- bronze.crm_prd_info
- bronze.crm_sales_details
- bronze.erp_cust_az12
- bronze.erp_loc_a101
- bronze.erp_px_cat_g1v2

Target Tables (Silver Layer):
- sliver.crm_cust_info
- sliver.crm_prd_info
- sliver.crm_sales_details
- sliver.erp_cust_az12
- sliver.erp_loc_a101
- sliver.erp_px_cat_g1v2

Logging:
The procedure prints execution logs and load durations for each table
as well as total batch processing time.

Error Handling:
TRY...CATCH block captures errors and prints error message and error number.

Execution:
EXEC sliver.load_sliver

================================================================================
*/

-- This is the query that is doing the data transformation and data cleansing and insterting the data to the silver layer:-
CREATE OR ALTER PROCEDURE sliver.load_sliver AS
BEGIN
  DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
  BEGIN TRY 
    SET @batch_start_time = GETDATE();
    PRINT '=================================';
    PRINT 'Loading The Bronze Layer';
    PRINT '=================================';

    PRINT '---------------------------------';
    PRINT 'Loading CRM Tables';
    PRINT '---------------------------------';

    SET @start_time = GETDATE();
  -- Table-1:- sliver.crm_cust_info
    PRINT 'Truncating Table: sliver.crm_cust_info';
    TRUNCATE TABLE sliver.crm_cust_info;
    PRINT '>> Inserting Data Into: sliver.crm_cust_info'
    INSERT INTO sliver.crm_cust_info(
      cst_id,
      cst_key,
      cst_firstname,
      cst_lastname,
      cst_marital_status,
      cst_gndr,
      cst_create_date
    )
    select 
      cst_id,
      cst_key,
      trim(cst_firstname) as cst_firstname, -- Trimming the extra spaces.
      trim(cst_lastname) as cst_lastname,
      case 
        when upper(trim(cst_marital_status)) = 'M' then 'Married'
        when upper(trim(cst_marital_status)) = 'S' then 'Single'
        else 'N/A' 
      end cst_marital_status, -- Normailze the marital statusv values to readable format.
      case 
        when upper(trim(cst_gndr)) = 'M' then 'Male'
        when upper(trim(cst_gndr)) = 'F' then 'Female'
        else 'N/A'
      end cst_gndr, -- Normailze the marital statusv values to readable format.
      cst_create_date
    from 
    (
      select 
        *,
        ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date desc) as flag_last
      from bronze.crm_cust_info 
    )t where flag_last = 1; -- Remove the duplicates by selecting the most recent record per customer.
    SET @end_time = GETDATE();
    PRINT '>> Load Duration: '+ cast(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + ' seconds';
    PRINT '------------------';


    -- Table-2:- sliver.crm_prd_info:-
    SET @start_time = GETDATE();
    PRINT 'Truncating Table: sliver.crm_prd_info';
    TRUNCATE table sliver.crm_prd_info;
    PRINT '>> Inserting Data Into: sliver.crm_prd_info'
    INSERT INTO sliver.crm_prd_info(
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
      Replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id, -- Extract Category Id
      SUBSTRING(prd_key, 7, LEN(prd_key)) as prd_key, -- Extract Product Key
      prd_nm,
      ISNULL(prd_cost, 0) as prd_cost,
      case 
        when upper(trim(prd_line)) = 'M' then 'Mountain'
        when upper(trim(prd_line)) = 'R' then 'Road'
        when upper(trim(prd_line)) = 'S' then 'Other Sales'
        when upper(trim(prd_line)) = 'T' then 'Touring'
        else 'n/a'
      end as prd_line, -- Map product line codes to descriptive values
      cast(prd_start_dt as DATE),
      cast(
        LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 
        as DATE
      ) as prd_end_dt -- Calculate end date as one day before the next start date
    FROM bronze.crm_prd_info;
    SET @end_time = GETDATE();
    PRINT '>> Load Duration: '+ cast(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + ' seconds';
    PRINT '------------------';


    -- Table-3:- sliver.crm_sales_details:-
    SET @start_time = GETDATE();
    PRINT 'Truncating Table: sliver.crm_sales_details';
    TRUNCATE TABLE sliver.crm_sales_details;
    PRINT '>> Inserting Data Into: sliver.crm_sales_details'
    INSERT INTO sliver.crm_sales_details(
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
      CASE
        WHEN sls_order_dt = 0 or  len(sls_order_dt) != 8 then null
        ELSE cast(cast(sls_order_dt as varchar) as DATE)
      END sls_order_dt,
      case
        WHEN sls_ship_dt = 0 or  len(sls_ship_dt) != 8 then null
        ELSE cast(cast(sls_ship_dt as varchar) as DATE)
      END sls_ship_dt,
      CASE
        WHEN sls_due_dt = 0 or  len(sls_due_dt) != 8 then null
        ELSE cast(cast(sls_due_dt as varchar) as DATE)
      END sls_due_dt,
      CASE 
        WHEN sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
      END as sls_sales, -- Recalculate sales if original value is missing or incorrect
      CASE
        WHEN sls_price is null or sls_price <= 0 THEN sls_sales/NULLIF(sls_quantity,0)
        ELSE sls_price
      END as sls_price, -- Derive price if original value is invalid
      sls_quantity
    FROM bronze.crm_sales_details;
    SET @end_time = GETDATE();
    PRINT '>> Load Duration: '+ cast(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + ' seconds';
    PRINT '------------------';


    -- Table-4:- sliver.erp_cust_az12:-
    SET @start_time = GETDATE();
    PRINT 'Truncating Table: sliver.erp_cust_az12';
    TRUNCATE TABLE sliver.erp_cust_az12;
    PRINT '>> Inserting Data Into: sliver.erp_cust_az12'
    INSERT INTO sliver.erp_cust_az12(
      cid,
      bdate,
      gen
    )
    select 
      case 
        when cid like 'NAS%' then SUBSTRING(cid,4,len(cid)) -- Remove 'NAS' prefix if present
        else cid
      end as cid,
      case 
        when bdate > GETDATE() then null
        else bdate
      end as bdate, -- Set future birthdates to NULL
      CASE 
        WHEN UPPER(LTRIM(RTRIM(REPLACE(REPLACE(gen, CHAR(10), ''), CHAR(13), '')))) IN ('F','FEMALE') THEN 'Female'
        WHEN UPPER(LTRIM(RTRIM(REPLACE(REPLACE(gen, CHAR(10), ''), CHAR(13), '')))) IN ('M','MALE') THEN 'Male'
        ELSE 'n/a'
      END AS gen -- Normalize gender values and handle unknown cases
    from bronze.erp_cust_az12;
    SET @end_time = GETDATE();
    PRINT '>> Load Duration: '+ cast(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + ' seconds';
    PRINT '------------------';


    -- Table-5:- sliver.erp_loc_a101:-
    SET @start_time = GETDATE();
    PRINT 'Truncating Table: sliver.erp_loc_a101';
    TRUNCATE TABLE sliver.erp_loc_a101;
    PRINT '>> Inserting Data Into: sliver.erp_loc_a101'
    INSERT INTO sliver.erp_loc_a101(
      cid,
      cntry
    )
    select 
      REPLACE(cid,'-','') as cid,
      CASE 
        WHEN UPPER(LTRIM(RTRIM(REPLACE(REPLACE(cntry, CHAR(10), ''), CHAR(13), '')))) = 'DE' then 'Germany'
        WHEN UPPER(LTRIM(RTRIM(REPLACE(REPLACE(cntry, CHAR(10), ''), CHAR(13), '')))) IN ('US', 'USA') then 'United States'
        WHEN UPPER(LTRIM(RTRIM(REPLACE(REPLACE(cntry, CHAR(10), ''), CHAR(13), '')))) = '' or cntry is null then 'n/a'
        else trim(cntry)
      END as cntry -- Normalize and Handle missing or blank country codes.
    from bronze.erp_loc_a101;

    -- Table-6:- sliver.erp_px_cat_g1v2:-
    SET @start_time = GETDATE();
    PRINT 'Truncating Table: sliver.erp_px_cat_g1v2';
    TRUNCATE TABLE sliver.erp_px_cat_g1v2;
    PRINT '>> Inserting Data Into: sliver.erp_px_cat_g1v2'
    INSERT INTO sliver.erp_px_cat_g1v2(
      id,
      cat,
      subcat,
      maintenance
    )
    SELECT
      id,
      cat,
      subcat,
      case 
        WHEN UPPER(LTRIM(RTRIM(REPLACE(REPLACE(maintenance, CHAR(10), ''), CHAR(13), '')))) = 'Yes' then 'Yes'
        else 'No'
      end as maintenance
    from bronze.erp_px_cat_g1v2;
    SET @end_time = GETDATE();
    PRINT '>> Load Duration: '+ cast(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + ' seconds';
    PRINT '------------------';
    SET @batch_end_time = GETDATE();
    PRINT 'Loading Bronze Layer Is Completed';
    PRINT '>> Total Load Duration: '+ cast(DATEDIFF(second,@batch_start_time,@batch_end_time) as NVARCHAR) + ' seconds';
    PRINT '------------------'
  END TRY
  BEGIN CATCH
    PRINT '=============================';
    PRINT 'Error Occured During Loading In Bronze Layer';
    PRINT 'Error Message'+ Error_Message();
    PRINT 'Error Number'+ cast(Error_Number() as NVARCHAR);
    PRINT '=============================';
  END CATCH
END

-- Executing Stored Procedure:-
EXEC sliver.load_sliver;
