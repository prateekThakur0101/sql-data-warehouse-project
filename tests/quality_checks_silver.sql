/*
================================================================================
Script Name  : Silver Layer Data Quality Checks
Author       : Prateek Thakur
Database     : datawarehouse
Layer        : Silver Layer
Created Date : 06-03-2026

Description:
This script performs data validation and quality checks on the Silver layer
tables after the ETL transformation process.

The goal of these checks is to ensure that the cleaned and transformed data
meets the expected data quality standards before it is used for analytics
or further processing in the Gold layer.

Validation Checks Performed:
1. Primary Key Validation
   - Detect NULL values or duplicate records in primary key columns.

2. Data Cleansing Verification
   - Identify unwanted leading or trailing spaces in text fields.

3. Data Standardization Checks
   - Verify categorical fields such as gender, marital status, product line,
     and country are standardized.

4. Data Consistency Checks
   - Validate relationships between columns such as:
     Sales = Quantity × Price.

5. Date Validation
   - Identify invalid, future, or logically incorrect date values.

Tables Validated:
1. sliver.crm_cust_info
2. sliver.crm_prd_info
3. sliver.crm_sales_details
4. sliver.erp_cust_az12
5. sliver.erp_loc_a101
6. sliver.erp_px_cat_g1v2

Expected Outcome:
All validation queries should return **no results** unless data quality
issues exist.

These checks help ensure the Silver layer contains clean, standardized,
and reliable data for downstream analytics and reporting.

================================================================================
*/

-- For Table-1:- sliver.crm_cust_info:-
-- 1. Checking the nulls and duplicates for primary key.
-- Expectation:- No Results.
select
  cst_id,
  count(*)
from sliver.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id IS NULL;

-- 2. Check for unwanted Spaces
-- Expectation: No Results.
SELECT 
  cst_firstname
FROM sliver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT 
  cst_lastname
FROM sliver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

-- 3. Data Standardization & Consistency
SELECT 
  DISTINCT cst_marital_status
FROM sliver.crm_cust_info;

SELECT 
  DISTINCT cst_gndr
FROM sliver.crm_cust_info;

-- Final Silver Table:-
select * from sliver.crm_cust_info;


------------------------------------------------------------------------------------------------------
-- For Table-2:- sliver.crm_prd_info:-
-- 1. Check For Nulls or Duplicates in Primary Key
-- Expectation: No Result
SELECT
  prd_id,
  COUNT (*)
FROM sliver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check for unwanted Spaces
-- Expectation: No Results
SELECT prd_nm
FROM sliver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- Check for NULLs or Negative Numbers
-- Expectation: No Results
SELECT prd_cost
FROM sliver.crm_prd_info
WHERE prd_cost < 0 or prd_cost IS NULL;

-- Data Standardization & Consistency
SELECT DISTINCT prd_line
FROM sliver.crm_prd_info

-- Checking for invalid date orders:-
select * 
from sliver.crm_prd_info
where prd_end_dt < prd_start_dt;

-- Final Silver Table:-
select * from sliver.crm_prd_info;


------------------------------------------------------------------------------------------------------
-- Table-3:- silver.crm_sales_details:-
-- Check for invalid dates:-
select
  NULLIF(sls_order_dt,0) as sls_order_dt
from sliver.crm_sales_details
where sls_order_dt <= 0 
or len(sls_order_dt) != 8
or sls_order_dt > 20500101
or sls_order_dt < 19000101

-- Checking for invalid date orders:-
-- Check for Invalid Date Orders
SELECT
*
FROM sliver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

-- Check Data Consistency: Between Sales, Quantity, and Price
-- > Sales = Quantity * Price
-- > Values must not be NULL, zero, or negative.

-- Rules
-- If Sales is negative, zero, or null, derive it using Quantity and Price.
-- If Price is zero or null, calculate it using Sales and Quantity.
-- If Price is negative, convert it to a positive value.
SELECT
distinct
  sls_quantity, 
  sls_sales,
  sls_price
FROM sliver. crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
or sls_sales is NULL
or sls_quantity is null
or sls_price is NULL
or sls_sales <= 0 
or sls_quantity <= 0
or sls_price <= 0
order by sls_sales, sls_quantity, sls_price;

-- Final silver table:-
SELECT * from sliver.crm_sales_details;

------------------------------------------------------------------------------------------------------
-- Table-4:- silver.erp_cust_az12:-
-- Identify Out-of-Range Dates
SELECT DISTINCT
bdate
FROM sliver.erp_cust_az12
where bdate < '1924-01-01' OR bdate > GETDATE();

-- Data standardization and consistency:-
SELECT 
DISTINCT gen
FROM sliver.erp_cust_az12;

-- Final Silver Table:-
select * from sliver.erp_cust_az12;

------------------------------------------------------------------------------------------------------
-- Table-5:- silver.erp_loc_a101:-
-- Correcting the cid for joining condition.
SELECT
REPLACE(cid, '-', '') cid, 
cntry
FROM sliver.erp_loc_a101;

-- Data Standardization & Consistency
select 
  distinct cntry
from sliver.erp_loc_a101
ORDER BY cntry;

-- Final silver table:-
select * from sliver.erp_loc_a101;

------------------------------------------------------------------------------------------------------
-- Table-6:- silver.erp_px_cat_g1v2:-
-- Checking unwanted spaces.
select * 
from sliver.erp_px_cat_g1v2
where cat != TRIM(cat) or subcat != trim(subcat) or maintenance != trim(maintenance);

-- Data Standardization & Consistency
SELECT
distinct maintenance
from sliver.erp_px_cat_g1v2;

-- Final Silver table
select * from sliver.erp_px_cat_g1v2;
