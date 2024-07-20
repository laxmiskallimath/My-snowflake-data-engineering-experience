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
-- Data is stored and maintained in databases.
-- Each Snowflake DB can have n number of:
-- Schemas
-- Tables
-- Views
-- Databases are independent of Virtual Warehouses. Queries within a database can be executed using any warehouse available within the account.
-- Databases can be managed either using SQL commands or using Snowflake UI.

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
create or replace role my_role;


-- Drop a Role
drop role my_role;

-- Show Roles
show roles;

--- ******** User ************
-- Users are individual accounts that can connect to Snowflake. They are assigned roles that define their access rights.

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


--- ******** File Format ************
--- File Format types

-- File formats are Snowflake objects created to maintain information that specifies the data stored in a source file. 
--It can be created as a named file format or can directly be used while; writing the 'COPY INTO' command as parameters.
--The named file format is used and recommended when a requirement is to load a similar format of data regularly.
--There are two types of file formats that are supported by Snowflake. It also provides a set of options depending upon the types of files to load data from.
-- ** Structured: CSV 
--Any CSV file with a valid delimiter can be used for loading data in Snowflake. The default delimiter is ',' (comma). It also provides options for specific data storage like data enclosed in double quotes " " or single quotes ' ' etc and error handling.

-- ** Semi-Structured: JSON, Avro, ORC, Parquet, XML file types.
-- Semi-structured data in Snowflake is loaded directly into the single column with VARIANT data type. Once the data is loaded into the table we can use it as normal structured data and can perform any operation such as data extraction.

-- Named file format :

-- Named file format can be created either using SQL query or Snowflake UI.

-- Example: CSV file format:

CREATE OR REPLACE FILE FORMAT my_database.my_schema.my_csv_fileformat
TYPE = 'CSV'
COMPRESSION = 'AUTO'
FIELD_DELIMITER = ','-- We can also use Custom Field Delimiter ex : pipe (|) character
RECORD_DELIMITER = '\n'
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = 'NONE'
TRIM_SPACE = FALSE
ERROR_ON_COLUMN_COUNT_MISMATCH= FALSE
ESCAPE = 'NONE'
ESCAPE_UNENCLOSED_FIELD = '\134'
DATE_FORMAT = 'AUTO'
TIMESTAMP_FORMAT = 'AUTO'
NULL_IF =('')
COMMENT = 'File format for .csv data';

//JSON file Format

CREATE FILE FORMAT my_database.my_schema.my_json_fileformat
TYPE = 'JSON' 
COMPRESSION = 'AUTO' 
ENABLE_OCTAL = FALSE 
ALLOW_DUPLICATE = FALSE 
STRIP_OUTER_ARRAY = TRUE 
STRIP_NULL_VALUES = FALSE 
IGNORE_UTF8_ERRORS = FALSE 
COMMENT = 'File format for JSON data';

--- File format commands

-- Used to check the current values of a named File format.
Describe File Format my_database.my_schema.my_json_fileformat;

-- USed to display all the file formats which are accessible to a single account, or database, or schema.
SHOW FILE FORMATS;

-- Used to remove the specified file format from Snowflake
Drop File format my_database.my_schema.my_json_fileformat;


--- ******** Stage ************
-- Stages are locations where data files are stored and from which data can be loaded into tables or unloaded from tables. Stages can be either internal or external.

-- Types of Stages

-- User Stages: Each user in Snowflake has a personal stage automatically created for them.
-- Table Stages: Each table in Snowflake has a stage automatically created for it.
-- Internal Named Stages: Stages created by users within Snowflake.
-- External Named Stages: Stages that reference external cloud storage locations (e.g., AWS S3, Azure Blob Storage, Google Cloud Storage).

-- 1. User Stages
-- User Stage is automatically created for each Snowflake user and named @~.

-- Load data from a CSV file in the user stage to a table
COPY INTO my_table
FROM @~/my_data.csv
FILE_FORMAT = (TYPE = 'CSV');

--2.Table Stages
-- Table Stage is automatically created for each Snowflake table and named @%<table_name>.

-- Load data from a CSV file in the table stage to a table
COPY INTO my_table
FROM @%my_table/my_data.csv
FILE_FORMAT = (TYPE = 'CSV');

-- 3. Internal Named Stages
-- Internal Named Stage is created by users and can store files within Snowflake.

-- Create an internal named stage
CREATE or REPLACE STAGE my_stage
COMMENT = 'Internal stage for data from sources';

-- Load data from the internal named stage to a table
COPY INTO my_table
FROM @my_stage/file.csv
FILE_FORMAT = (TYPE = 'CSV');


-- 4. External Named Stages

-- External Named Stage references an external cloud storage location.

-- Create an external named stage for AWS S3

create or replace stage my_database.my_schema.my_stage_s3
url='s3://-------------------------' -- location of data in AWS
storage_integration = my_int;

-- Load data from the external named stage to a table
COPY INTO my_table
FROM @my_stage_s3/my_data.csv
FILE_FORMAT = (TYPE = 'CSV');


create or replace stage my_database.my_schema.my_stage_azure
URL = 'azure://-------------------------'
storage_integration = my_int;/*location of data in Aaure*/

-- Load data from the external named stage to a table
COPY INTO my_table
FROM @my_stage_azure/my_data.csv
FILE_FORMAT = (TYPE = 'CSV');


CREATE or replace stage my_database.my_schema.my_stage_gcs 
URL = 'gcs://-------------------------'
storage_integration = my_int;/*location of data in GCP*/


-- Load data from the external named stage to a table
COPY INTO my_table
FROM @my_stage_gcs/my_data.csv
FILE_FORMAT = (TYPE = 'CSV');

-- Load bulk data from flat files to stage we use put command but it will not work in snowflake webui it only works in SNOWSQL 
put file : //C:\Users\laxmi.s02\Desktop\Snowflake Session\snowflakedata (1)
file format = my_csv;














