/*
===============================================================================
Data Warehouse Project - Bronze Layer Table Creation
===============================================================================

Author      : Prateek (Data Engineer)
Project     : Data Warehouse and Analytics Project
Database    : datawarehouse
Layer       : Bronze (Raw Data Layer)

Description :
This script creates the Bronze layer tables used to store raw data
ingested from source systems before transformation.

Source Systems:
1. CRM System
   - crm_cust_info
   - crm_prd_info
   - crm_sales_details

2. ERP System
   - erp_loc_a101
   - erp_cust_az12
   - erp_px_cat_g1v2

Purpose:
- Store raw ingested data from source systems (CSV files)
- Preserve original data structure
- Serve as the first stage of the ETL pipeline

Notes:
- Existing tables are dropped before creation to ensure a clean load.
- Data will later be transformed and moved to Silver and Gold layers.

===============================================================================
*/

use datawarehouse;
-- layer.sourcesystem_tablename:-
IF OBJECT_ID('bronze.crm_cust_info','U') IS NOT NULL
  drop table bronze.crm_cust_info
GO
CREATE TABLE bronze.crm_cust_info(
  cst_id INT,
  cst_key NVARCHAR(50),
  cst_firstname NVARCHAR(50),
  cst_lastname NVARCHAR(50),
  cst_marital_status NVARCHAR(50),
  cst_gndr NVARCHAR(50),
  cst_create_date DATE
);


IF OBJECT_ID('bronze.crm_prd_info','U') IS NOT NULL
  drop table bronze.crm_prd_info
GO
CREATE TABLE bronze.crm_prd_info(
  prd_id INT,
  prd_key NVARCHAR(50),	
  prd_nm NVARCHAR(50),	
  prd_cost INT,
  prd_line NVARCHAR(50),
  prd_start_dt DATETIME,
  prd_end_dt DATETIME
);


IF OBJECT_ID('bronze.crm_sales_details','U') IS NOT NULL
  drop table bronze.crm_sales_details
GO
CREATE TABLE bronze.crm_sales_details(
  sls_ord_num NVARCHAR(50),
  sls_prd_key NVARCHAR(50),
  sls_cust_id INT,
  sls_order_dt INT,
  sls_ship_dt INT,
  sls_due_dt INT,
  sls_sales INT,
  sls_quantity INT,
  sls_price INT
);


-- layer.sourceSystem_tableName:- (bronze.erp_loc_a101)

IF OBJECT_ID('bronze.erp_loc_a101','U') IS NOT NULL
  drop table bronze.erp_loc_a101
GO
CREATE TABLE bronze.erp_loc_a101(
  cid NVARCHAR(50),
  cntry NVARCHAR(50)
);


IF OBJECT_ID('bronze.erp_cust_az12','U') IS NOT NULL
  drop table bronze.erp_cust_az12
GO
CREATE TABLE bronze.erp_cust_az12 (
  cid NVARCHAR(50),
  bdate DATE,
  gen NVARCHAR(50)
);


IF OBJECT_ID('bronze.erp_px_cat_g1v2','U') IS NOT NULL
  drop table bronze.erp_px_cat_g1v2
GO
CREATE TABLE bronze.erp_px_cat_g1v2 (
  id NVARCHAR(50) ,
  cat NVARCHAR(50),
  subcat NVARCHAR(50) ,
  maintenance NVARCHAR(50)
);
