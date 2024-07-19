-- Key objects in Snowflake


--- ******** Warehouse ************
-- A virtual warehouse is a cluster of compute resources in Snowflake. 
-- It provides the computational power for running queries, loading data, and performing other operations.

-- Create a Warehouse
create or replace warehouse my_warehouse
with warehouse_size = 'XSMALL'
auto_suspend = 60
auto_resume = true
INITIALLY_SUSPENDED = TRUE;

-- Suspend/Resume a Warehouse
alter warehouse my_warehouse suspend;
alter warehouse my_warehouse resume;


-- Monitor Warehouse Usage
select * from snowflake.account_usage.warehouse_metering_history
where warehouse_name = 'my_warehouse';


--- ******** Database ************
-- A database is a logical grouping of schemas, tables, views, and other objects.

 -- Create a Database 
create or replace database my_database;

-- Drop a Database
drop database my_database;

-- Show Databases
show databases;


 --- ******** Schema ************
--  A schema is a logical container within a database that holds tables, views, and other objects.

-- Create a Schema
create or replace schema my_schema;

-- Drop a Schema 
drop schema my_schema;

-- Show Schemas
show schemas;


--- ******** Table ************
-- Tables are structured data containers, similar to tables in a relational database.

-- Create a Table
create or replace table my_table(
id int,
name string,
created_at timestamp
);

-- Drop a Table
drop table my_table;


-- Show Tables
show tables;


--- ******** Views ************
-- Views are saved queries that can be treated like tables in SELECT statements.

-- Create a View
create or replace view my_view 
as
   select 
        id,
        name 
   from my_table
   where created_at > '2023-01-01';

-- Drop a View
drop view my_view;


-- Show Views
SHOW VIEWS;

--- ******** Role ************
-- Roles are used to manage access control by grouping privileges that can be assigned to users or other roles.

-- Create a Role
create role my_role;


-- Drop a Role
drop role my_role;

-- Show Roles
show roles;

--- ******** User ************
-- Create a User
create or replace user my_user
password = 'abcde@2024'
default_role = my_role
default_warehouse = my_warehouse
default_namespace = my_database.my_schema;


-- Drop a User
drop user my_user;


-- Show Users
show users;

--- ******** Stage ************
-- Stages are storage locations in Snowflake where data files can be staged for loading into tables or unloading from tables.
-- Internal Stage
-- Internal stages are stored within the Snowflake environment.
CREATE STAGE my_database.my_schema.my_stage 
COMMENT = 'Internal stage for demo data from sources';

-- External Stages
-- External stages point to external storage locations. Below are examples for AWS S3, Azure Blob Storage, and Google Cloud Storage (GCS).


-- AWS S3

CREATE STAGE my_database.my_schema.my_stage 
URL = 's3://your-bucket-name/your-path/'
CREDENTIALS = (AWS_KEY_ID='your-aws-key-id' AWS_SECRET_KEY='your-aws-secret-key')
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- Azure Blob Storage
CREATE STAGE my_database.my_schema.my_stage 
URL = 'azure://your-container-name.blob.core.windows.net/your-path/'
CREDENTIALS = (AZURE_SAS_TOKEN='your-sas-token')
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"');


-- Google Cloud Storage (GCS)
CREATE STAGE my_database.my_schema.my_stage 
URL = 'gcs://your-bucket-name/your-path/'
CREDENTIALS = (GCS_KEY_ID='your-gcs-key-id' GCS_SECRET_KEY='your-gcs-secret-key')
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"');

































































-- Step 3: User and Role Management
