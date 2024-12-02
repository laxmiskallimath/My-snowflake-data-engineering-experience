-- We have different COPY command options for data loading.
/*
1) FILE_FORMAT
2) ON_ERROR >> SKIP_FILE,ABORT_STATEMENT,CONTINUE
3) VALIDATION MODE >> return_errors AND return_N_rows
4) SIZE_LIMIT
5) RETURN_FAILED_ONLY >> TRUE/FALSE (WILL WORK WITH ONLY on_error = continue OPTION)
6) TRUNCATECOLUMNS >> TRUE/FALSE (DEFAULT FALSE)
7) FORCE >> TRUE/FALSE (DEFAULT FALSE)
*/

-- VALIDATION MODE 
// PREPARE DATABASE ANND TABLE 

CREATE OR REPLACE DATABASE COPY_DB;

CREATE OR REPLACE TABLE COPY_DB.PUBLIC. ORDERS(
ORDER_ID VARCHAR(30),
AMOUNT VARCHAR(30),
PROFIT INT,
QUANTITY INT,
CATEGORY VARCHAR(30),
SUBCATEGORY  VARCHAR(30) 
);

//PREPARE THE STAGE OBJECT 

CREATE OR REPLACE STAGE COPY_DB.PUBLIC.AWS_STAGE_COPY
URL = 's3://snowflakebucket-copyoption/size/';

// list files in stage object 

list @COPY_DB.PUBLIC.AWS_STAGE_COPY;

// Load data using copy command 

copy into copy_db.public.orders
from @aws_stage_copy
file_format = (type = csv field_delimiter = ',' skip_header = 1)
pattern = '.*order.*'
validation_mode = return_errors;


copy into copy_db.public.orders
from @aws_stage_copy
file_format = (type = csv field_delimiter = ',' skip_header = 1)
pattern = '.*Orders.*'
validation_mode = return_5_rows;

list @aws_stage_copy;

-- Use files with errors 

create or replace stage copy_db.public.aws_stage_copy_with_errors
url = 's3://snowflakebucket-copyoption/returnfailed/';

list @copy_db.public.aws_stage_copy_with_errors;

-- show all errors 
truncate table copy_db.public.orders;

copy into copy_db.public.orders
from @copy_db.public.aws_stage_copy_with_errors
file_format = (type = csv field_delimiter = ',' skip_header = 1)
pattern = '.*Order.*'
validation_mode = return_errors;

--or 

COPY INTO copy_db.public.orders
from @copy_db.public.aws_stage_copy_with_errors
VALIDATION_MODE = 'RETURN_ERRORS';


-- Validate first n rows 

copy into copy_db.public.orders
from @copy_db.public.aws_stage_copy_with_errors
file_format = (type = csv field_delimiter = ',' skip_header = 1)
pattern = '.*Order.*'
validation_mode = return_5_rows;
 

--- ******** Assignment ************

-- 1. Create a table called employees with the following columns and data types

create or replace table employees(
customer_id int,
first_name varchar(50),
last_name varchar(50),
email varchar(50),
age int,
city varchar(50)
);

-- 2. Create a stage object pointing to 's3://snowflake-assignments-mc/copyoptions/example1'

create or replace stage copy_db.public.assignment_stage
url ='s3://snowflake-assignments-mc/copyoptions/example1';

-- 3. Create a file format object with the specification
create or replace file format assignment_file_format
TYPE = CSV
FIELD_DELIMITER=','
SKIP_HEADER=1;

-- 4. Use the copy option to only validate if there are errors and if yes what errors.

-- -- Use ON_ERROR
copy into employees
from @copy_db.public.assignment_stage
file_format = (format_name=COPY_DB.PUBLIC.ASSIGNMENT_FILE_FORMAT)
--files = ('employees.csv')
on_error = Continue;

select count(*) from employees;

-- Use validation mode
copy into employees
from @copy_db.public.assignment_stage
file_format = (format_name=COPY_DB.PUBLIC.ASSIGNMENT_FILE_FORMAT)
      VALIDATION_MODE = RETURN_ERRORS;
 
-- WORKING WITH REJECTED RECORDS 

--- Working with error results ---

-- Saving rejected files after VALIDATION_MODE

CREATE OR REPLACE TABLE COPY_DB.PUBLIC.ORDERS_8
(
ORDER_ID VARCHAR(30),
AMOUNT VARCHAR(30),
PROFIT INT,
QUANTITY INT,
CATEGORY VARCHAR(30),
SUBCATEGORY VARCHAR(30)
);


COPY INTO COPY_DB.PUBLIC.ORDERS_8
FROM @COPY_DB.PUBLIC.AWS_STAGE_COPY_WITH_ERRORS
FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1)
pattern ='.*Order.*'
validation_mode = return_errors;

// Storing Rejected / failed results in a table 
create or replace table rejected as 
select rejected_record from table (result_scan(last_query_id()));

-- 01b8bbd6-0001-3904-0006-b4ca00010b36

select * from rejected;


-- you can use query id directly also 
create or replace table rejected1 as 
select rejected_record from table (result_scan('01b8bbd6-0001-3904-0006-b4ca00010b36'));

select * from rejected1;

-- Adding additional records using last query id function.

insert into rejected
select rejected_record from table (result_scan(last_query_id()));


select * from rejected;

-- 2) Saving rejected records without  VALIDATION_MODE(On_error = continue)


CREATE OR REPLACE TABLE COPY_DB.PUBLIC.ORDERS_9
(
ORDER_ID VARCHAR(30),
AMOUNT VARCHAR(30),
PROFIT INT,
QUANTITY INT,
CATEGORY VARCHAR(30),
SUBCATEGORY VARCHAR(30)
);


COPY INTO COPY_DB.PUBLIC.ORDERS_9
FROM @COPY_DB.PUBLIC.AWS_STAGE_COPY_WITH_ERRORS
FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1)
pattern ='.*Order.*'
on_error = continue;


select * from table (validate (ORDERS_9,job_id => '_last'));


-- 3) Working with rejected records/ Processing/cleaning them 

select rejected_record  from rejected;

create or replace table rejected_values as 
select 
     split_part(rejected_record,',',1) as order_id,
     split_part(rejected_record,',',2) as amount,
     split_part(rejected_record,',',3) as profit,
     split_part(rejected_record,',',4) as quantity,
     split_part(rejected_record,',',5) as category,
     split_part(rejected_record,',',6) as subcategory
from 
     rejected;
     

select * from rejected_values;

-- SIZE LIMIT COMMAND IN COPY INTO COMMAND 

-- Specify maximum size (in bytes) of data loaded in that command (at least one file)
-- When the threshold is exceeded, the copy operation stop loading 


USE COPY_DB;

CREATE OR REPLACE TABLE COPY_DB.PUBLIC.ORDERS_SIZE_LIMIT(
ORDER_ID VARCHAR(30),
AMOUNT VARCHAR(30),
PROFIT INT,
QUANTITY INT,
CATEGORY VARCHAR(30),
SUBCATEGORY VARCHAR(30));


// PREPARE STAG OBJECT

CREATE OR REPLACE STAGE COPY_DB.PUBLIC.AWS_STAGE_COPY_1
URL = 's3://snowflakebucket-copyoption/size';


// List files in stage 

list @COPY_DB.PUBLIC.AWS_STAGE_COPY_1;

// Load data using copy command 

copy into copy_db.public.orders_size_limit
from @COPY_DB.PUBLIC.AWS_STAGE_COPY_1
file_format = (type = csv field_delimiter = ',',skip_header = 1)
pattern = '.*Order.*'
size_limit = 20000;

-- size lomit = 20k is exceeded with the first file already hence second file will not be loaded 
-- size lomit = 20k  means all of the files in stage are combined 


-- lets truncate the same table rload it again by changing size limit (Recreate the table with create or replace command)

truncate table copy_db.public.orders_size_limit;

-- // Load data using copy command where size_limit = 60000
copy into COPY_DB.PUBLIC.ORDERS_SIZE_LIMIT
from @COPY_DB.PUBLIC.AWS_STAGE_COPY_1
file_format = (type = csv field_delimiter = ',' skip_header = 1)
pattern = '.*Order.*'
size_limit = 60000;

--- Return_failed_only  opion in copy command 
-- This command will work with option when on_error = continue

-- Here we want to specify whether to return only files that have failed to load in the statement result in our coy command.


USE COPY_DB;

CREATE OR REPLACE TABLE COPY_DB.PUBLIC.ORDERS_Return_failed_only(
ORDER_ID VARCHAR(30),
AMOUNT VARCHAR(30),
PROFIT INT,
QUANTITY INT,
CATEGORY VARCHAR(30),
SUBCATEGORY VARCHAR(30));

// Create the stage object 

create or replace stage copy_db.public.aws_stage_copy_RFO
url = 's3://snowflakebucket-copyoption/returnfailed/';


list@copy_db.public.aws_stage_copy_RFO;


//Load data using copy command 

copy into copy_db.public.orders_return_failed_only
from @copy_db.public.aws_stage_copy_RFO
file_format = (type = csv field_delimiter = ',' skip_header = 1)
pattern = '.*Order.*'
return_failed_only = True;


-- Above command will with below masg sin ce we are not using on_wrror option in copy command 

/*Numeric value '7-' is not recognized
  File 'returnfailed/OrderDetails_error2 - Copy.csv', line 2, character 17
  Row 1, column "ORDERS_RETURN_FAILED_ONLY"["QUANTITY":4]
  If you would like to continue loading when an error is encountered, use other values such as 'SKIP_FILE' or 'CONTINUE' for the ON_ERROR option. For more information on loading options, please run 'info loading_data' in a SQL client.*/

copy into copy_db.public.orders_return_failed_only
from @copy_db.public.aws_stage_copy_RFO
file_format = (type = csv field_delimiter = ',' skip_header = 1)
pattern = '.*Order.*'
on_error = continue
return_failed_only = True;

--- TRUNCATECOLUMNS     

--- Specifies whether to truncate text strings that exceed the target column length.
--  TRUNCATECOLUMNS  = True  >> Strings are automatically truncated to the target column length.
--- TRUNCATECOLUMNS  = False >> COPY throws an error if a loaded string exceeds the target column length.
--- DEFAULT = FALSE 

CREATE OR REPLACE TABLE COPY_DB.PUBLIC.ORDERS_TC(
ORDER_ID VARCHAR(30),
AMOUNT VARCHAR(30),
PROFIT INT,
QUANTITY INT,
CATEGORY VARCHAR(10),
SUBCATEGORY VARCHAR(30));


// Create stage object 

create or replace stage copy_db.public.aws_stage_copy
url = 's3://snowflakebucket-copyoption/size/';

// List files in stage 

list@copy_db.public.aws_stage_copy;


// Load data using copy command 

copy into COPY_DB.PUBLIC.ORDERS_TC
from @copy_db.public.aws_stage_copy
file_format = (type = csv field_delimiter = ',' skip_header = 1)
pattern = '.*Order.*';


-- We get below error for column category whose length varchar(10) but exceeds specified length

/* User character length limit (10) exceeded by string 'Electronics'
  File 'size/Orders2.csv', line 5, character 18
  Row 4, column "ORDERS_TC"["CATEGORY":5]
  If you would like to continue loading when an error is encountered, use other values such as 'SKIP_FILE' or 'CONTINUE' for the ON_ERROR option. For more information on loading options, please run 'info loading_data' in a SQL client.*/

copy into COPY_DB.PUBLIC.ORDERS_TC
from @copy_db.public.aws_stage_copy
file_format = (type = csv field_delimiter = ',' skip_header = 1)
pattern = '.*Order.*'
truncatecolumns = true ; -- Default will be set to false which will throw the above error ,if its sets to true it will ignore and load data.

-- Now all of the rows loaded 

select * from COPY_DB.PUBLIC.ORDERS_TC;

-- In Result set we can observe category column having vaues with character length of 10 only. 
-- Furniture,Electronic,Clothing


--- ************************************* FORCE **********************************************************************

-- Lets explore FORCE option in copy command 
-- Specifies to load all files ,regardlesss  of whether they have been loaded previously and have not changed since they were loaded 
-- Note that this option reloads files, potentially duplicating data in a table 

USE COPY_DB;

CREATE OR REPLACE TABLE COPY_DB.PUBLIC.ORDERS_FORCE(
ORDER_ID VARCHAR(30),
AMOUNT VARCHAR(30),
PROFIT INT,
QUANTITY INT,
CATEGORY VARCHAR(30),
SUBCATEGORY VARCHAR(30));

// Create stage object 

create or replace stage copy_db.public.aws_stage_copy
url = 's3://snowflakebucket-copyoption/size/';

// List files in stage 

list@copy_db.public.aws_stage_copy;


// Load data using copy command

copy into copy_db.public.ORDERS_FORCE
from @COPY_DB.PUBLIC.AWS_STAGE_COPY_1
file_format = (type = csv field_delimiter = ',',skip_header = 1)
pattern = '.*Order.*';
 

// Lets try to rerun the above copy command 
// Not pssible to load file that have been loaded and data has not been modified 

copy into copy_db.public.ORDERS_FORCE
from @COPY_DB.PUBLIC.AWS_STAGE_COPY_1
file_format = (type = csv field_delimiter = ',',skip_header = 1)
pattern = '.*Order.*';

--- We can see Copy executed with 0 files processed.

select * from copy_db.public.ORDERS_FORCE;

// Using the FORCE option

copy into copy_db.public.ORDERS_FORCE
from @COPY_DB.PUBLIC.AWS_STAGE_COPY_1
file_format = (type = csv field_delimiter = ',',skip_header = 1)
pattern = '.*Order.*'
FORCE = TRUE;


----******************* LOAD HISTORY ***********************************************
-- Load history enables you to retrieve the history of data loaded into tables using the copy into <table> command.

--Each database in Snowflake does indeed have an information schema, which stores metadata about the database objects.
-- And the SNOWFLAKE system database contains both INFORMATION_SCHEMA and ACCOUNT_USAGE schemas.
-- INFORMATION_SCHEMA is available across all databases and is used for object-level metadata.
-- ACCOUNT_USAGE is part of the SNOWFLAKE system database and provides account-level usage and performance metadata.

-- Query load history within a database 

use copy_db;

-- We have a view called laod history in information schema 

select * from COPY_DB.INFORMATION_SCHEMA.LOAD_HISTORY;


-- Lets query views in account usage  schema in common database/ snowflake global data base.

select * from SNOWFLAKE.ACCOUNT_USAGE.LOAD_HISTORY;


// Filter on specific table and schema 

select * from snowflake.account_usage.load_history
where schema_name = 'PUBLIC' and table_name LIKE  'ORDERS%';

// Filter on specific table and schema by error count
select * from snowflake.account_usage.load_history
where schema_name = 'PUBLIC' and table_name LIKE  'ORDERS%'
AND ERROR_COUNT>0;


// Filter on specific table and schema on specific date.

select * from snowflake.account_usage.load_history
where  DATE(LAST_LOAD_TIME) <= DATEADD(days,-1,current_date);-- yesterday

---- *********** Assignment ******************

 create or replace database exercise_db;
 
 
 -- Create table
create or replace table employees_ass2(
  customer_id int,
  first_name varchar(50),
  last_name varchar(50),
  email varchar(50),
  age int,
  department varchar(50));

-- Create stage object
CREATE OR REPLACE STAGE EXERCISE_DB.public.aws_stage
    url='s3://snowflake-assignments-mc/copyoptions/example2';
 
-- create file format object
CREATE OR REPLACE FILE FORMAT EXERCISE_DB.public.aws_fileformat
TYPE = CSV
FIELD_DELIMITER=','
SKIP_HEADER=1;

-- Use validation mode

COPY INTO EXERCISE_DB.PUBLIC.employees_ass2
    FROM @aws_stage
      file_format= EXERCISE_DB.public.aws_fileformat
      VALIDATION_MODE = RETURN_ERRORS;


 -- Use TRUNCATECOLUMNS

COPY INTO EXERCISE_DB.PUBLIC.employees_ass2
    FROM @aws_stage
      file_format= EXERCISE_DB.public.aws_fileformat
      TRUNCATECOLUMNS = TRUE; 


select count(*) from employees_ass2;-- 62  



  



















































































































  




































  


































































































































 

      












































































