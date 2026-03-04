/*
===============================================================================
Bronze Layer Data Load Procedure
===============================================================================

Author      : Prateek (Data Engineer)
Project     : Data Warehouse and Analytics Project
Database    : datawarehouse
Layer       : Bronze Layer (Raw Data Ingestion)

Description :
This stored procedure loads raw data from CSV files into the Bronze layer
tables using SQL Server BULK INSERT.

MacOS SQL Server Limitation :
SQL Server is not natively supported on macOS. To run SQL Server on a Mac,
we use a Docker container. Because SQL Server runs inside the container,
it cannot directly access files from the host machine's local file system.
We have to download the docker desktop for mac then do some configuration 
to run the sql server in local machine.

Problem :
When using BULK INSERT, SQL Server expects the file path to exist inside
the SQL Server environment. If the CSV file exists only on the Mac host
machine (for example: /Users/prateek/Desktop/data.csv), SQL Server inside
the Docker container cannot see or access that file.

Solution :
The CSV files must first be copied into the SQL Server Docker container
using the docker cp command.

Example :
Command for terminal:-
docker cp cust_info.csv sqlserver:/var/opt/mssql/data/cust_info.csv

After copying the file, SQL Server can access it using the path inside
the container: /var/opt/mssql/data/cust_info.csv

ETL Process :
1. Truncate existing data from Bronze tables.
2. Load raw data from CSV files using BULK INSERT.
3. Measure load duration for monitoring.
4. Handle errors using TRY...CATCH.

Source Systems :
CRM System
 - crm_cust_info
 - crm_prd_info
 - crm_sales_details

ERP System
 - erp_cust_az12
 - erp_loc_a101
 - erp_px_cat_g1v2

===============================================================================

*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS

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
    PRINT '>> Truncating Table: bronze.crm_cust_info';
    Truncate TABLE bronze.crm_cust_info; 
    PRINT '>> Inserting Data Into Table: bronze.crm_cust_info';
    BULK INSERT bronze.crm_cust_info
    FROM '/var/opt/mssql/data/cust_info.csv'
    WITH (
      FIRSTROW = 2,
      FIELDTERMINATOR = ',',
      TABLOCK
    );
    SET @end_time = GETDATE();
    PRINT '>> Load Duration: '+ cast(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + ' seconds';
    PRINT '------------------'

    SET @start_time = GETDATE();
    PRINT '>> Truncating Table: bronze.crm_prd_info';
    Truncate TABLE bronze.crm_prd_info; 
    PRINT '>> Inserting Data Into Table: bronze.crm_prd_info';
    BULK INSERT bronze.crm_prd_info
    FROM '/var/opt/mssql/data/prd_info.csv'
    WITH (
      FIRSTROW = 2,
      FIELDTERMINATOR = ',',
      TABLOCK
    );
    SET @end_time = GETDATE();
    PRINT '>> Load Duration: '+ cast(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + ' seconds';
    PRINT '------------------'

    SET @start_time = GETDATE();
    PRINT '>> Truncating Table: bronze.crm_sales_details';
    Truncate TABLE bronze.crm_sales_details; 
    PRINT '>> Inserting Data Into Table: bronze.crm_sales_details';
    BULK INSERT bronze.crm_sales_details
    FROM '/var/opt/mssql/data/sales_details.csv'
    WITH (
      FIRSTROW = 2,
      FIELDTERMINATOR = ',',
      TABLOCK
    );
    SET @end_time = GETDATE();
    PRINT '>> Load Duration: '+ cast(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + ' seconds';
    PRINT '------------------'


    PRINT '---------------------------------';
    PRINT 'Loading ERP Tables';
    PRINT '---------------------------------';

    SET @start_time = GETDATE();
    PRINT '>> Truncating Table: bronze.erp_cust_az12';
    Truncate TABLE bronze.erp_cust_az12; 
    PRINT '>> Inserting Data Into Table: bronze.erp_cust_az12';
    BULK INSERT bronze.erp_cust_az12
    FROM '/var/opt/mssql/data/CUST_AZ12.csv'
    WITH (
      FIRSTROW = 2,
      FIELDTERMINATOR = ',',
      TABLOCK
    );
    SET @end_time = GETDATE();
    PRINT '>> Load Duration: '+ cast(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + ' seconds';
    PRINT '------------------'

    SET @start_time = GETDATE();
    PRINT '>> Truncating Table: bronze.erp_loc_a101';
    Truncate TABLE bronze.erp_loc_a101; 
    PRINT '>> Inserting Data Into Table: bronze.erp_loc_a101';
    BULK INSERT bronze.erp_loc_a101
    FROM '/var/opt/mssql/data/LOC_A101.csv'
    WITH (
      FIRSTROW = 2,
      FIELDTERMINATOR = ',',
      TABLOCK
    );
    SET @end_time = GETDATE();
    PRINT '>> Load Duration: '+ cast(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + ' seconds';
    PRINT '------------------'

    SET @start_time = GETDATE();
    PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
    Truncate TABLE bronze.erp_px_cat_g1v2; 
    PRINT '>> Inserting Data Into Table: bronze.erp_px_cat_g1v2';
    BULK INSERT bronze.erp_px_cat_g1v2
    FROM '/var/opt/mssql/data/PX_CAT_G1V2.csv'
    WITH (
      FIRSTROW = 2,
      FIELDTERMINATOR = ',',
      TABLOCK
    );
    SET @end_time = GETDATE();
    PRINT '>> Load Duration: '+ cast(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + ' seconds';
    PRINT '------------------'

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

-- Executing Store Procedure:-
EXEC bronze.load_bronze;
