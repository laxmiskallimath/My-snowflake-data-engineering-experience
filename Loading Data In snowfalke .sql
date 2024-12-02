-- Database to manage stage objects 
create or replace database manage_db;

create or replace schema external_stages;

-- Creating  external stage 

create or replace stage MANAGE_DB.EXTERNAL_STAGES.aws_stage
url = 's3://bucketsnowflakes3'
credentials = (aws_key_id = 'ABCD_DUMMY_ID' aws_secret_key = '1234abcd_key');


-- Description of external stage 

desc stage MANAGE_DB.EXTERNAL_STAGES.AWS_STAGE;


-- Alter external stage 

alter stage aws_stage 
set credentials = (aws_key_id = 'XYZ_DUMMY_ID' aws_secret_key = '987xyz');

-- Publically accessible staging area 

create or replace stage MANAGE_DB.external_stages.aws_stage
url = 's3://bucketsnowflakes3';

-- List files in stage 

list @aws_stage;


----*******************************************************

create or replace database our_first_db;

-- Creating orders table 

create or replace table OUR_FIRST_DB.PUBLIC.orders (
order_id varchar(30),
amount int,
profit int,
quantity int,
category varchar(30),
subcategory varchar(30)
);

select * from OUR_FIRST_DB.PUBLIC.ORDERS;

--  First copy command 

copy into OUR_FIRST_DB.PUBLIC.ORDERS
from @MANAGE_DB.EXTERNAL_STAGES.AWS_STAGE
file_format = (type = csv field_delimiter = ',' skip_header = 1);


-- We are getting below error when tried to execute copy command 

--- Number of columns in file (2) does not match that of the corresponding table (6), use file format option error_on_column_count_mismatch=false to ignore this error

-- Lets list the files in stage 

list @MANAGE_DB.EXTERNAL_STAGES.AWS_STAGE;

-- Copy command with specified file(s)

copy into OUR_FIRST_DB.PUBLIC.ORDERS
from @MANAGE_DB.EXTERNAL_STAGES.AWS_STAGE
file_format = (type= csv field_delimiter = ',' skip_header = 1)
files = ('OrderDetails.csv');


select * from OUR_FIRST_DB.PUBLIC.ORDERS;


---- **************** Transforming data***************************

create or replace table OUR_FIRST_DB.PUBLIC.ORDERS_ex1
(
order_id varchar(30),
amount int
);


copy into OUR_FIRST_DB.PUBLIC.ORDERS_ex1
from (select s1.$1,s1.$2 from @MANAGE_DB.EXTERNAL_STAGES.AWS_STAGE s1)
file_format = (type = csv field_delimiter = ',' skip_header = 1)
files = ('OrderDetails.csv');

select * from OUR_FIRST_DB.PUBLIC.ORDERS_ex1;


-- Example - 2 -- Copy command using sql function (Subset of functions available)
create or replace table OUR_FIRST_DB.PUBLIC.ORDERS_ex2
(
order_id varchar(30),
amount int,
profit int,
profitable_flag varchar(30));

-- Copy command using sql function (subset of functions avaialble)
copy into OUR_FIRST_DB.PUBLIC.ORDERS_ex2
from (
      select 
          s1.$1,
          s1.$2,
          s1.$3,
          case 
          when  cast(s1.$3 as int) < 0 then 'not profitable' else 'proftable' end 
    from 
        @MANAGE_DB.EXTERNAL_STAGES.AWS_STAGE s1)
file_format = (type = csv field_delimiter = ',',skip_header = 1)
files = ('OrderDetails.csv');


select * from OUR_FIRST_DB.PUBLIC.ORDERS_ex2;

-- Example -3 - Copy command using a SQL function (substring function)
create or replace table OUR_FIRST_DB.PUBLIC.ORDERS_ex3
(
order_id varchar(30),
amount int,
profit int,
category_substring varchar(30)
);

-- Copy command using sql function
copy into OUR_FIRST_DB.PUBLIC.ORDERS_ex3 
from 

(
select 
    s1.$1,
    S1.$2,
    s1.$3,
    substr(s1.$5,1,5)
from @MANAGE_DB.EXTERNAL_STAGES.AWS_STAGE s1
)
file_format = (type=csv field_delimiter = ',' skip_header = 1)
files = ('OrderDetails.csv');


select * from OUR_FIRST_DB.PUBLIC.ORDERS_ex3 ;


--- ******** Additional Transformation Techniques *******************

-- Example - 4  Using subset of columns 

create or replace table our_first_db.public.orders_ex4
(
order_id varchar(30),
amount int,
profit int,
profit_flag varchar(30)
);


-- Using subset of columns  in copy command 
copy into our_first_db.public.orders_ex4 (order_id,amount)
from 
(
select 
   s1.$1,
   s1.$2
from @MANAGE_DB.EXTERNAL_STAGES.AWS_STAGE s1 
)
file_format = (type = csv field_delimiter = ',' skip_header = 1)
files = ('OrderDetails.csv');


list @MANAGE_DB.EXTERNAL_STAGES.AWS_STAGE;

select * from our_first_db.public.orders_ex4;

-- Example 5  - Table Auto Increment 
create or replace table our_first_db.public.orders_ex5(
order_id number autoincrement start 1 increment 1,
amount int ,
profit int,
profitable_flag varchar(30)
);

-- Copy command with auto increment ID 

copy into our_first_db.public.orders_ex5(profit,amount)
from 
    (
    select 
      s1.$2,
      s1.$3
    from @MANAGE_DB.EXTERNAL_STAGES.AWS_STAGE s1)
file_format = (type = csv field_delimiter = ',' skip_header = 1)
files = ('OrderDetails.csv');


select * from our_first_db.public.orders_ex5
where order_id > 15;

---- Copy Option : On_error
-- How to deal with errors when we are using copy command 

-- Create stage with error file 

create or replace stage MANAGE_DB.EXTERNAL_STAGES.AWS_STAGE_error_ex
url = 's3://bucketsnowflakes4';


-- List files in stage 

list @MANAGE_DB.EXTERNAL_STAGES.AWS_STAGE_error_ex;

-- Create example table 
create or replace table OUR_FIRST_DB.PUBLIC.ORDERS_ex6
(
order_id varchar(30),
amount int,
profit int,
quantity int,
category varchar(30),
subcategory varchar(30)
);

--  Demonstrating error msg

copy into OUR_FIRST_DB.PUBLIC.ORDERS_ex6
from @MANAGE_DB.EXTERNAL_STAGES.AWS_STAGE_ERROR_EX
file_format = (type = csv field_delimiter= ',' skip_header = 1)
files = ('OrderDetails_error.csv');


-- we get below error since profit column holds some string values instead of numeric

/*Numeric value 'one thousand' is not recognized
 File 'OrderDetails_error.csv', line 2, character 14
 Row 1, column "ORDERS_EX6"["PROFIT":3]
 If you would like to continue loading when an error is encountered, use other values such as 'SKIP_FILE' or 'CONTINUE' for the ON_ERROR option. For more information on loading options,
please run 'info loading_data' in a SQL client.*/

-- Validating the table is empty

select * from OUR_FIRST_DB.PUBLIC.ORDERS_ex6;

-- Error handling using on_error option  error_option = continue

copy into OUR_FIRST_DB.PUBLIC.ORDERS_ex6
from  @MANAGE_DB.EXTERNAL_STAGES.AWS_STAGE_ERROR_EX
file_format = (type = csv field_delimiter = ',' skip_header = 1)
files = ('OrderDetails_error.csv')
on_error = 'Continue'; -- on_error parameter.


-- Now we can see status as partially loaded 

-- Validating the results and truncating table 

select * from  OUR_FIRST_DB.PUBLIC.ORDERS_ex6;

select count(*) from  OUR_FIRST_DB.PUBLIC.ORDERS_ex6;

truncate table OUR_FIRST_DB.PUBLIC.ORDERS_ex6;

-- Error handling using on_error option  error_option = abort_statement
copy into 
  OUR_FIRST_DB.PUBLIC.ORDERS_ex6
from  @MANAGE_DB.EXTERNAL_STAGES.AWS_STAGE_ERROR_EX
file_format = (type = csv field_delimiter = ',' skip_header = 1)
files = ('OrderDetails_error.csv')
on_error = 'abort_statement'; -- no loading happens at all 


-- Validating the results and truncating table 

select * from  OUR_FIRST_DB.PUBLIC.ORDERS_ex6;

select count(*) from  OUR_FIRST_DB.PUBLIC.ORDERS_ex6;

 
-- Error handling using on_error option  error_option = skip_file

-- skip_file will skip the file with error and load correct file only 

copy into 
  OUR_FIRST_DB.PUBLIC.ORDERS_ex6
from  @MANAGE_DB.EXTERNAL_STAGES.AWS_STAGE_ERROR_EX
file_format = (type = csv field_delimiter = ',' skip_header = 1)
files = ('OrderDetails_error.csv','OrderDetails_error2.csv')
on_error = 'skip_file';

select * from  OUR_FIRST_DB.PUBLIC.ORDERS_ex6;

select count(*) from  OUR_FIRST_DB.PUBLIC.ORDERS_ex6;

truncate table OUR_FIRST_DB.PUBLIC.ORDERS_ex6;

-- Error handling using on_error option  error_option = skip_file_<number>

-- skip_file will skip the file with error and load correct file only 

copy into 
  OUR_FIRST_DB.PUBLIC.ORDERS_ex6
from  @MANAGE_DB.EXTERNAL_STAGES.AWS_STAGE_ERROR_EX
file_format = (type = csv field_delimiter = ',' skip_header = 1)
files = ('OrderDetails_error.csv','OrderDetails_error2.csv')
on_error = 'skip_file_3';

select * from  OUR_FIRST_DB.PUBLIC.ORDERS_ex6;

select count(*) from  OUR_FIRST_DB.PUBLIC.ORDERS_ex6;

truncate table OUR_FIRST_DB.PUBLIC.ORDERS_ex6;

-- skip percentage of records
copy into 
  OUR_FIRST_DB.PUBLIC.ORDERS_ex6
from  @MANAGE_DB.EXTERNAL_STAGES.AWS_STAGE_ERROR_EX
file_format = (type = csv field_delimiter = ',' skip_header = 1)
files = ('OrderDetails_error.csv','OrderDetails_error2.csv')
on_error = 'skip_file_5%';


select * from  OUR_FIRST_DB.PUBLIC.ORDERS_ex6;

select count(*) from  OUR_FIRST_DB.PUBLIC.ORDERS_ex6;

truncate table OUR_FIRST_DB.PUBLIC.ORDERS_ex6;


--- ********** FILE FORMAT OBJECT ***********************
-- CREATING THE TABLE 

CREATE OR REPLACE TABLE OUR_FIRST_DB.PUBLIC.ORDERS_7(
ORDER_ID VARCHAR(30),
AMOUNT INT,
PROFIT INT,
QUANTITY INT,
CATEGORY VARCHAR(30),
SUBCATEGORY VARCHAR(30)
);

-- CREATING SCHEMA TO KEEP THINGS ORGANIZED 

CREATE OR REPLACE SCHEMA MANAGE_DB.FILE_FORMATS;

-- CREATING FILE FORMAT OBJECT 

CREATE OR REPLACE FILE FORMAT MANAGE_DB.FILE_FORMATS.MY_FIE_FORMAT;


-- SEE PROPERTIES OF FILE FILE FORMAT OBJECT 

DESC FILE FORMAT MANAGE_DB.FILE_FORMATS.MY_FIE_FORMAT;

-- USING FILE FORMAT OBJECT IN COPY COMMAND 

COPY INTO OUR_FIRST_DB.PUBLIC.ORDERS_7
FROM 
 @MANAGE_DB.EXTERNAL_STAGES.AWS_STAGE_ERROR_EX
file_format = (FORMAT_NAME = MANAGE_DB.FILE_FORMATS.MY_FIE_FORMAT)
files = ('OrderDetails_error.csv')
ON_ERROR = 'SKIP_FILE_3';

-- ALTERING THE FILE FORMAT

ALTER FILE FORMAT MANAGE_DB.FILE_FORMATS.MY_FIE_FORMAT
SET SKIP_HEADER = 1;


TRUNCATE TABLE OUR_FIRST_DB.PUBLIC.ORDERS_7;
 
-- DEFINING PROPERTIES ON CREATION OF FILE FORMAT OBJECT 

CREATE OR REPLACE FILE FORMAT MANAGE_DB.FILE_FORMATS.MY_FIE_FORMAT_1
TYPE = JSON,
TIME_FORMAT  = AUTO ;

-- SEE PROPERTIES OF FILE FORMAT 

DESC  FILE FORMAT MANAGE_DB.FILE_FORMATS.MY_FIE_FORMAT_1;


-- USING NEW FILE FORMAT OBJECT IN COPY COMMAND 

COPY INTO OUR_FIRST_DB.PUBLIC.ORDERS_7
FROM 
 @MANAGE_DB.EXTERNAL_STAGES.AWS_STAGE_ERROR_EX
file_format = (FORMAT_NAME = MANAGE_DB.FILE_FORMATS.MY_FIE_FORMAT_1)
files = ('OrderDetails_error.csv')
ON_ERROR = 'SKIP_FILE_3';

-- WE WILL GET ERROR BELOW SINCE WE ARE LOADING CSV FILE USING COPY COMMAND NOT JSON 
-- 002019 (0A000): SQL compilation error:
-- JSON file format can produce one and only one column of type variant, object, or array. Load data into separate columns using the MATCH_BY_COLUMN_NAME copy option or copy with transformation.

-- SO LETS ALTER FILE FORMAT

-- ALTERING THE TYPE OF FILE FORMAT  IS NOT POSSIBLE 

ALTER FILE FORMAT MANAGE_DB.FILE_FORMATS.MY_FIE_FORMAT_1
SET TYPE = CSV;

-- HENCE RECREATE THE FILE FORMAT(DEFAULT IS CSV) 

CREATE OR REPLACE FILE FORMAT MANAGE_DB.FILE_FORMATS.MY_FILE_FORMAT_CSV;

DESCRIBE FILE FORMAT MANAGE_DB.FILE_FORMATS.MY_FILE_FORMAT_CSV;

-- OVER WRITTING PROPERTIES OF FILE FORMAT OBJECT

COPY INTO OUR_FIRST_DB.PUBLIC.ORDERS_7
FROM 
    @MANAGE_DB.EXTERNAL_STAGES.AWS_STAGE_ERROR_EX
    FILE_FORMAT = (FORMAT_NAME = MANAGE_DB.FILE_FORMATS.MY_FILE_FORMAT_CSV,FIELD_DELIMITER = ',' SKIP_HEADER = 1)
    FILES = ('OrderDetails_error.csv')
    ON_ERROR = 'SKIP_FILE_3';


DESC STAGE MANAGE_DB.EXTERNAL_STAGES.AWS_STAGE_ERROR_EX;

--Note : THE PROPERIES OF STAGE AND FILE FORMAT ARE SAME 







































































































































































































 





































































































          




)











































































