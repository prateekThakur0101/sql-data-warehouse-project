/*
===========================
Create Database and Schemas
===========================

Script Purpose:
This script creates a new database named 'DataWarehouse' after checking if it already exists.
If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas within the database: 'bronze' , 'silver' , and 'gold'.

WARNING:
Running this script will drop the entire 'Datawarehouse' database if it exists.
All data in the database will be permanently deleted. Proceed with caution and ensure you have proper backups before running this script.
*/ 

-- Drop and recreate the 'Datawarehouse' database
IF EXISTS (SELECT 1 FROM sys. databases WHERE name = 'DataWarehouse')
BEGIN
  ALTER DATABASE Datawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
  DROP DATABASE Datawarehouse;
END;
GO

-- Create the 'Datawarehouse' database
CREATE DATABASE DataWarehouse;
GO

USE datawarehouse;
GO
  
-- Creating schema for each layer:-
create schema bronze;
GO

create schema sliver;
GO

create schema gold;
GO
