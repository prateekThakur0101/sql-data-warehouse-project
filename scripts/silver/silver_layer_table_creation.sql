/* 
================================================================================
Script Name  : Silver Layer Table Creation
Database     : datawarehouse
Layer        : Silver
Author       : Prateek Thakur
Created Date : 06-03-2206

Description  :
This script creates tables in the Silver layer of the Data Warehouse.

The Silver layer stores cleaned and transformed data from the Bronze layer.
These tables represent structured datasets from CRM and ERP source systems.

Tables Created:
1. sliver.crm_cust_info       - Customer master information from CRM
2. sliver.crm_prd_info        - Product information from CRM
3. sliver.crm_sales_details   - Sales transaction details from CRM
4. sliver.erp_loc_a101        - Location data from ERP
5. sliver.erp_cust_az12       - Customer demographic data from ERP
6. sliver.erp_px_cat_g1v2     - Product category hierarchy from ERP

Notes:
- Existing tables will be dropped before creation.
- dwh_create_date captures the record load timestamp.
================================================================================
*/

USE datawarehouse;
-- layer.sourcesystem_tablename:-

IF OBJECT_ID('sliver.crm_cust_info','U') IS NOT NULL
  drop table sliver.crm_cust_info
GO
CREATE TABLE sliver.crm_cust_info(
  cst_id INT,
  cst_key NVARCHAR(50),
  cst_firstname NVARCHAR(50),
  cst_lastname NVARCHAR(50),
  cst_marital_status NVARCHAR(50),
  cst_gndr NVARCHAR(50),
  cst_create_date DATE,
  dwh_create_date DATETIME DEFAULT GETDATE()
);


IF OBJECT_ID('sliver.crm_prd_info','U') IS NOT NULL
  drop table sliver.crm_prd_info
GO
CREATE TABLE sliver.crm_prd_info(
  prd_id INT,
  cat_id NVARCHAR(50),
  prd_key NVARCHAR(50),	
  prd_nm NVARCHAR(50),	
  prd_cost INT,
  prd_line NVARCHAR(50),
  prd_start_dt DATE,
  prd_end_dt DATE,
  dwh_create_date DATETIME DEFAULT GETDATE()
);


IF OBJECT_ID('sliver.crm_sales_details','U') IS NOT NULL
  drop table sliver.crm_sales_details
GO
CREATE TABLE sliver.crm_sales_details(
  sls_ord_num NVARCHAR(50),
  sls_prd_key NVARCHAR(50),
  sls_cust_id INT,
  sls_order_dt DATE,
  sls_ship_dt DATE,
  sls_due_dt DATE,
  sls_sales INT,
  sls_quantity INT,
  sls_price INT,
  dwh_create_date DATETIME DEFAULT GETDATE()
);


-- layer.sourceSystem_tableName:- (sliver.erp_loc_a101)

IF OBJECT_ID('sliver.erp_loc_a101','U') IS NOT NULL
  drop table sliver.erp_loc_a101
GO
CREATE TABLE sliver.erp_loc_a101(
  cid NVARCHAR(50),
  cntry NVARCHAR(50),
  dwh_create_date DATETIME DEFAULT GETDATE()
);


IF OBJECT_ID('sliver.erp_cust_az12','U') IS NOT NULL
  drop table sliver.erp_cust_az12
GO
CREATE TABLE sliver.erp_cust_az12 (
  cid NVARCHAR(50),
  bdate DATE,
  gen NVARCHAR(50),
  dwh_create_date DATETIME DEFAULT GETDATE()
);


IF OBJECT_ID('sliver.erp_px_cat_g1v2','U') IS NOT NULL
  drop table sliver.erp_px_cat_g1v2
GO
CREATE TABLE sliver.erp_px_cat_g1v2 (
  id NVARCHAR(50) ,
  cat NVARCHAR(50),
  subcat NVARCHAR(50) ,
  maintenance NVARCHAR(50),
  dwh_create_date DATETIME DEFAULT GETDATE()
);
