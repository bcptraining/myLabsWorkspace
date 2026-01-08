USE DATABASE HRMS;

-- create schema ETL;
USE SCHEMA ETL;


CREATE OR REPLACE FILE FORMAT HRMS.ETL.JSON_ETL_FILEFORMAT
TYPE = JSON;


CREATE OR REPLACE STAGE HRMS.ETL.AWS_ETL_JSON_STAGE
STORAGE_INTEGRATION = udemy_mc_a1_si
URL = 'S3://vdw-dev-ingest/loadingdatalabs/json/'
FILE_FORMAT = JSON_ETL_FILEFORMAT;

desc integration udemy_mc_a1_si;

list @HRMS.ETL.AWS_ETL_JSON_STAGE;

select * from @HRMS.ETL.AWS_ETL_JSON_STAGE;
CREATE or replace STORAGE INTEGRATION udemy_mc_a1_si
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::904233092605:role/vdw_dev_data_ingest_si_role'
STORAGE_ALLOWED_LOCATIONS = ('s3://vdw-dev-ingest/loadingdatalabs/',
  's3://vdw-dev-ingest/loadingdatalabs/labcsv/','s3://vdw-dev-ingest/loadingdatalabs/
    json/');

desc STORAGE INTEGRATION udemy_mc_a1_si;

CREATE OR REPLACE STAGE HRMS.ETL.AWS_ETL_JSON_STAGE
STORAGE_INTEGRATION = udemy_mc_a1_si
URL = 'S3://vdw-dev-ingest/loadingdatalabs/labcsv/'
FILE_FORMAT = JSON_ETL_FILEFORMAT;

LIST @AWS_ETL_JSON_STAGE;

--Loading Object Data

CREATE OR REPLACE TABLE EMPLOYEE_DATA_OBJECT
(
EMPLOYEE_INFO OBJECT
);


COPY INTO EMPLOYEE_DATA_OBJECT FROM  @AWS_ETL_JSON_STAGE
FILES =('employee_object.json');

SELECT * FROM EMPLOYEE_DATA_OBJECT;


SELECT EMPLOYEE_INFO FROM EMPLOYEE_DATA_OBJECT;


SELECT 
        EMPLOYEE_INFO:employee_id,        
        EMPLOYEE_INFO:employee_name, 
        EMPLOYEE_INFO:position, 
        EMPLOYEE_INFO:address         
FROM EMPLOYEE_DATA_OBJECT;


SELECT 
        EMPLOYEE_INFO:employee_id,        
        EMPLOYEE_INFO:employee_name, 
        EMPLOYEE_INFO:position, 
        EMPLOYEE_INFO:address.city,
        EMPLOYEE_INFO:address.street,
        EMPLOYEE_INFO:address.state,
        EMPLOYEE_INFO:address.zip_code
FROM EMPLOYEE_DATA_OBJECT;


SELECT 
        EMPLOYEE_INFO:employee_id :: STRING,        
        EMPLOYEE_INFO:employee_name :: STRING,   
        EMPLOYEE_INFO:position :: STRING,   
        EMPLOYEE_INFO:address.city :: STRING,   
        EMPLOYEE_INFO:address.street :: STRING,   
        EMPLOYEE_INFO:address:state :: STRING,   
        EMPLOYEE_INFO:address:zip_code :: STRING   
FROM EMPLOYEE_DATA_OBJECT;


SELECT 
        EMPLOYEE_INFO:employee_id :: STRING      AS EMPLOYEE_ID,        
        EMPLOYEE_INFO:employee_name :: STRING    AS EMPLOYEE_NAME,   
        EMPLOYEE_INFO:position :: STRING         AS POISTION,   
        EMPLOYEE_INFO:address.city :: STRING     AS CITY,
        EMPLOYEE_INFO:address.street :: STRING   AS STREET,
        EMPLOYEE_INFO:address:state :: STRING    AS STATE,
        EMPLOYEE_INFO:address:zip_code :: STRING AS ZIP_CODE
FROM EMPLOYEE_DATA_OBJECT;



--Loading Array Data


CREATE OR REPLACE TABLE EMPLOYEE_DATA_ARRAY
(
EMPLOYEE_INFO ARRAY
);

COPY INTO EMPLOYEE_DATA_ARRAY FROM  @AWS_ETL_JSON_STAGE
FILES =('employee_array.json');

SELECT * FROM EMPLOYEE_DATA_ARRAY;

SELECT EMPLOYEE_INFO FROM EMPLOYEE_DATA_ARRAY;

SELECT  EMPLOYEE_INFO[0] , 
        EMPLOYEE_INFO[1] , 
        EMPLOYEE_INFO[2] ,
        EMPLOYEE_INFO[3]
FROM EMPLOYEE_DATA_ARRAY;


SELECT  EMPLOYEE_INFO[0] :: STRING  PRIMARY_PHONE,
        EMPLOYEE_INFO[1] :: STRING  SECONDARY_PHONE,
        EMPLOYEE_INFO[2] :: STRING  TERTIARY_PHONE
FROM EMPLOYEE_DATA_ARRAY;


SELECT ARRAY_SIZE(EMPLOYEE_INFO) FROM EMPLOYEE_DATA_ARRAY;

--Loading JSON Data

CREATE OR REPLACE TABLE EMPLOYEE_DATA_JSON
(
EMPLOYEE_INFO VARIANT
);


COPY INTO EMPLOYEE_DATA_JSON FROM  @AWS_ETL_JSON_STAGE
FILES =('employee_E105.json') ;


SELECT * FROM EMPLOYEE_DATA_JSON;



SELECT         
        EMPLOYEE_INFO:employee_id,
        EMPLOYEE_INFO:employee_name,
        EMPLOYEE_INFO:position
FROM EMPLOYEE_DATA_JSON;


SELECT         
        EMPLOYEE_INFO:employee_id,
        EMPLOYEE_INFO:employee_name,
        EMPLOYEE_INFO:position,
        EMPLOYEE_INFO:phone_numbers
FROM EMPLOYEE_DATA_JSON;


SELECT         
        EMPLOYEE_INFO:employee_id,
        EMPLOYEE_INFO:employee_name,
        EMPLOYEE_INFO:position,
        EMPLOYEE_INFO:phone_numbers[0],
        EMPLOYEE_INFO:phone_numbers[1],
FROM EMPLOYEE_DATA_JSON;



SELECT         
        EMPLOYEE_INFO:employee_id,
        EMPLOYEE_INFO:employee_name,
        EMPLOYEE_INFO:position,
        EMPLOYEE_INFO:phone_numbers[0],
        EMPLOYEE_INFO:phone_numbers[1],
        EMPLOYEE_INFO:address
FROM EMPLOYEE_DATA_JSON;


SELECT         
        EMPLOYEE_INFO:employee_id,
        EMPLOYEE_INFO:employee_name,
        EMPLOYEE_INFO:position,
        EMPLOYEE_INFO:phone_numbers[0],
        EMPLOYEE_INFO:phone_numbers[1],
        EMPLOYEE_INFO:address.street,
        EMPLOYEE_INFO:address.city,
        EMPLOYEE_INFO:address.state,
        EMPLOYEE_INFO:address.zip_code
FROM EMPLOYEE_DATA_JSON;


SELECT         
        EMPLOYEE_INFO:employee_id,
         EMPLOYEE_INFO:skills,
        EMPLOYEE_INFO:employee_name,
        EMPLOYEE_INFO:position,
        EMPLOYEE_INFO:phone_numbers[0],
        EMPLOYEE_INFO:phone_numbers[1],
        EMPLOYEE_INFO:address.street,
        EMPLOYEE_INFO:address.city,
        EMPLOYEE_INFO:address.state,
        EMPLOYEE_INFO:address.zip_code
       
FROM EMPLOYEE_DATA_JSON;


SELECT         
        EMPLOYEE_INFO:employee_id,
        EMPLOYEE_INFO:employee_name,
        EMPLOYEE_INFO:position,
        EMPLOYEE_INFO:phone_numbers[0],
        EMPLOYEE_INFO:phone_numbers[1],
        EMPLOYEE_INFO:address.street,
        EMPLOYEE_INFO:address.city,
        EMPLOYEE_INFO:address.state,
        EMPLOYEE_INFO:address.zip_code,
        EMPLOYEE_INFO:skills[0],
        EMPLOYEE_INFO:skills[1],
        EMPLOYEE_INFO:skills[2]
FROM EMPLOYEE_DATA_JSON;


SELECT         
        EMPLOYEE_INFO:employee_id,
        EMPLOYEE_INFO:employee_name,
        EMPLOYEE_INFO:position,
        EMPLOYEE_INFO:phone_numbers[0],
        EMPLOYEE_INFO:phone_numbers[1],
        EMPLOYEE_INFO:address.street,
        EMPLOYEE_INFO:address.city,
        EMPLOYEE_INFO:address.state,
        EMPLOYEE_INFO:address.zip_code,
        EMPLOYEE_INFO:skills[0].skill_name,
        EMPLOYEE_INFO:skills[0].proficiency_level,
        EMPLOYEE_INFO:skills[1].skill_name,        
        EMPLOYEE_INFO:skills[1].proficiency_level,
        EMPLOYEE_INFO:skills[2].skill_name,
        EMPLOYEE_INFO:skills[2].proficiency_level
FROM EMPLOYEE_DATA_JSON;




SELECT         
        EMPLOYEE_INFO:employee_id::STRING                             AS EMPLOYEE_ID,
        EMPLOYEE_INFO:employee_name::STRING                           AS EMPLOYEE_NAME,        
        EMPLOYEE_INFO:position::STRING                                AS JOB_RANK,       
        EMPLOYEE_INFO:phone_numbers[0]::STRING                        AS PRIMARY_PHONE_NUMBER,
        EMPLOYEE_INFO:phone_numbers[1]::STRING                        AS SECONDARY_PHONE_NUMBER,
        EMPLOYEE_INFO:address.street::STRING                          AS STREET_ADDRESS, 
        EMPLOYEE_INFO:address.city::STRING                            AS CITY,
        EMPLOYEE_INFO:address.state::STRING                           AS STATE,
        EMPLOYEE_INFO:address.zip_code::STRING                        AS ZIP_CODE,
        EMPLOYEE_INFO:skills[0].skill_name::STRING                    AS SKILL_NAME_1,
        EMPLOYEE_INFO:skills[0].proficiency_level::STRING             AS SKILL1_PROFICIENCY_LEVEL,                    
        EMPLOYEE_INFO:skills[1].skill_name::STRING                    AS SKILL_NAME_2,
        EMPLOYEE_INFO:skills[1].proficiency_level::STRING             AS SKILL2_PROFICIENCY_LEVEL,  
        EMPLOYEE_INFO:skills[2].skill_name::STRING                    AS SKILL_NAME_3,        
        EMPLOYEE_INFO:skills[2].proficiency_level::STRING             AS SKILL3_PROFICIENCY_LEVEL
FROM EMPLOYEE_DATA_JSON;


CREATE OR REPLACE TABLE JSON_EXTRACTED_EMPLOYEES
AS
SELECT         
        EMPLOYEE_INFO:employee_id::STRING                             AS EMPLOYEE_ID,
        EMPLOYEE_INFO:employee_name::STRING                           AS EMPLOYEE_NAME,        
        EMPLOYEE_INFO:position::STRING                                AS JOB_RANK,       COMMISSION_PCT
        EMPLOYEE_INFO:phone_numbers[0]::STRING                        AS PRIMARY_PHONE_NUMBER,
        EMPLOYEE_INFO:phone_numbers[1]::STRING                        AS SECONDARY_PHONE_NUMBER,
        EMPLOYEE_INFO:address.street::STRING                          AS STREET_ADDRESS, 
        EMPLOYEE_INFO:address.city::STRING                            AS CITY,
        EMPLOYEE_INFO:address.state::STRING                           AS STATE,
        EMPLOYEE_INFO:address.zip_code::STRING                        AS ZIP_CODE,
        EMPLOYEE_INFO:skills[0].skill_name::STRING                    AS SKILL_NAME_1,
        EMPLOYEE_INFO:skills[0].proficiency_level::STRING             AS SKILL1_PROFICIENCY_LEVEL,                    
        EMPLOYEE_INFO:skills[1].skill_name::STRING                    AS SKILL_NAME_2,
        EMPLOYEE_INFO:skills[1].proficiency_level::STRING             AS SKILL2_PROFICIENCY_LEVEL,  
        EMPLOYEE_INFO:skills[2].skill_name::STRING                    AS SKILL_NAME_3,        
        EMPLOYEE_INFO:skills[2].proficiency_level::STRING             AS SKILL3_PROFICIENCY_LEVEL
FROM EMPLOYEE_DATA_JSON;

SELECT * FROM JSON_EXTRACTED_EMPLOYEES;



--LATERAL FLATTEN

CREATE OR REPLACE TABLE EMPLOYEE_DATA_JSON_FLATTEN
(
EMPLOYEE_INFO VARIANT
);



COPY INTO EMPLOYEE_DATA_JSON_FLATTEN FROM  @AWS_ETL_JSON_STAGE
FILES =('employee_E105.json');


SELECT * FROM EMPLOYEE_DATA_JSON_FLATTEN;


SELECT         
        EMPLOYEE_INFO:employee_id::STRING                             AS EMPLOYEE_ID,
        EMPLOYEE_INFO:employee_name::STRING                           AS EMPLOYEE_NAME,        
        EMPLOYEE_INFO:position::STRING                                AS JOB_TITLE     
FROM EMPLOYEE_DATA_JSON_FLATTEN ;

SELECT         
        EMPLOYEE_INFO:employee_id::STRING                             AS EMPLOYEE_ID,
        EMPLOYEE_INFO:employee_name::STRING                           AS EMPLOYEE_NAME,        
        EMPLOYEE_INFO:position::STRING                                AS JOB_TITLE,                    
        f.*
FROM EMPLOYEE_DATA_JSON_FLATTEN ,LATERAL FLATTEN (EMPLOYEE_INFO:phone_numbers) f;

select *  FROM EMPLOYEE_DATA_JSON_FLATTEN;


SELECT         
        EMPLOYEE_INFO:employee_id::STRING                             AS EMPLOYEE_ID,
        EMPLOYEE_INFO:employee_name::STRING                           AS EMPLOYEE_NAME,        
        EMPLOYEE_INFO:position::STRING                                AS JOB_RANK,         
        fp.VALUE
FROM EMPLOYEE_DATA_JSON_FLATTEN ,LATERAL FLATTEN (EMPLOYEE_INFO:phone_numbers) fp;


SELECT         
        EMPLOYEE_INFO:employee_id::STRING                             AS EMPLOYEE_ID,
        EMPLOYEE_INFO:employee_name::STRING                           AS EMPLOYEE_NAME,        
        EMPLOYEE_INFO:position::STRING                                AS JOB_RANK ,        
        fs.VALUE
FROM EMPLOYEE_DATA_JSON_FLATTEN ,LATERAL FLATTEN (EMPLOYEE_INFO:skills) fs;


SELECT         
        EMPLOYEE_INFO:employee_id::STRING                             AS EMPLOYEE_ID,
        EMPLOYEE_INFO:employee_name::STRING                           AS EMPLOYEE_NAME,        
        EMPLOYEE_INFO:position::STRING                                AS JOB_RANK,        
        EMPLOYEE_INFO:position::STRING                                AS JOB_RANK,     
        fs.VALUE:proficiency_level                                    AS proficiency_level,
        fs.VALUE:skill_name                                           AS skill_name
FROM EMPLOYEE_DATA_JSON_FLATTEN ,LATERAL FLATTEN (EMPLOYEE_INFO:skills) fs;



SELECT           
        EMPLOYEE_INFO:employee_id::STRING                            AS EMPLOYEE_ID,
        EMPLOYEE_INFO:employee_name::STRING                          AS EMPLOYEE_NAME,        
        EMPLOYEE_INFO:position::STRING                               AS JOB_RANK,        
        EMPLOYEE_INFO:position::STRING                               AS JOB_RANK,     
        fs.VALUE:proficiency_level                                   AS proficiency_level,
        fs.VALUE:skill_name                                          AS skill_name,
        fp.VALUE
FROM EMPLOYEE_DATA_JSON_FLATTEN ,LATERAL FLATTEN (EMPLOYEE_INFO:skills) fs ,LATERAL FLATTEN (EMPLOYEE_INFO:phone_numbers) fp;




--Combining Data with Union

CREATE OR REPLACE TABLE JSON_DATA_TABLE_UNION
(
EMPLOYEE_INFO VARIANT
);


COPY INTO JSON_DATA_TABLE_UNION FROM  @AWS_ETL_JSON_STAGE
FILES =('employees_multi_json_array.json');

SELECT * FROM JSON_DATA_TABLE_UNION;

SELECT EMPLOYEE_INFO[0] FROM JSON_DATA_TABLE_UNION;
SELECT EMPLOYEE_INFO[1] FROM JSON_DATA_TABLE_UNION;
SELECT EMPLOYEE_INFO[2] FROM JSON_DATA_TABLE_UNION;

SELECT         

        EMPLOYEE_INFO[0]:employee_id,
        EMPLOYEE_INFO[0]:employee_name,
        EMPLOYEE_INFO[0]:position, 
        EMPLOYEE_INFO[0]:phone_numbers[0],
        EMPLOYEE_INFO[0]:phone_numbers[1],
        EMPLOYEE_INFO[0]:address.city,        
        EMPLOYEE_INFO[0]:address.state,
        EMPLOYEE_INFO[0]:address.street,
        EMPLOYEE_INFO[0]:address.zip_code,
        EMPLOYEE_INFO[0]:skills[0].skill_name,
        EMPLOYEE_INFO[0]:skills[0].proficiency_level,
        EMPLOYEE_INFO[0]:skills[1].skill_name,
        EMPLOYEE_INFO[0]:skills[1].proficiency_level,
        EMPLOYEE_INFO[0]:skills[2].skill_name,
        EMPLOYEE_INFO[0]:skills[2].proficiency_level
FROM  JSON_DATA_TABLE_UNION;



SELECT         

        EMPLOYEE_INFO[1]:employee_id,
        EMPLOYEE_INFO[1]:employee_name,
        EMPLOYEE_INFO[1]:position, 
        EMPLOYEE_INFO[1]:phone_numbers[0],
        EMPLOYEE_INFO[1]:phone_numbers[1],
        EMPLOYEE_INFO[1]:address.city,        
        EMPLOYEE_INFO[1]:address.state,
        EMPLOYEE_INFO[1]:address.street,
        EMPLOYEE_INFO[1]:address.zip_code,
        EMPLOYEE_INFO[1]:skills[0].skill_name,
        EMPLOYEE_INFO[1]:skills[0].proficiency_level,
        EMPLOYEE_INFO[1]:skills[1].skill_name,
        EMPLOYEE_INFO[1]:skills[1].proficiency_level,
        EMPLOYEE_INFO[1]:skills[2].skill_name,
        EMPLOYEE_INFO[1]:skills[2].proficiency_level
FROM  JSON_DATA_TABLE_UNION;





SELECT         

        EMPLOYEE_INFO[0]:employee_id,
        EMPLOYEE_INFO[0]:employee_name,
        EMPLOYEE_INFO[0]:position, 
        EMPLOYEE_INFO[0]:phone_numbers[0],
        EMPLOYEE_INFO[0]:phone_numbers[1],
        EMPLOYEE_INFO[0]:address.city,        
        EMPLOYEE_INFO[0]:address.state,
        EMPLOYEE_INFO[0]:address.street,
        EMPLOYEE_INFO[0]:address.zip_code,
        EMPLOYEE_INFO[0]:skills[0].skill_name,
        EMPLOYEE_INFO[0]:skills[0].proficiency_level,
        EMPLOYEE_INFO[0]:skills[1].skill_name,
        EMPLOYEE_INFO[0]:skills[1].proficiency_level,
        EMPLOYEE_INFO[0]:skills[2].skill_name,
        EMPLOYEE_INFO[0]:skills[2].proficiency_level
FROM  JSON_DATA_TABLE_UNION
UNION ALL
SELECT         

        EMPLOYEE_INFO[1]:employee_id,
        EMPLOYEE_INFO[1]:employee_name,
        EMPLOYEE_INFO[1]:position, 
        EMPLOYEE_INFO[1]:phone_numbers[0],
        EMPLOYEE_INFO[1]:phone_numbers[1],
        EMPLOYEE_INFO[1]:address.city,        
        EMPLOYEE_INFO[1]:address.state,
        EMPLOYEE_INFO[1]:address.street,
        EMPLOYEE_INFO[1]:address.zip_code,
        EMPLOYEE_INFO[1]:skills[0].skill_name,
        EMPLOYEE_INFO[1]:skills[0].proficiency_level,
        EMPLOYEE_INFO[1]:skills[1].skill_name,
        EMPLOYEE_INFO[1]:skills[1].proficiency_level,
        EMPLOYEE_INFO[1]:skills[2].skill_name,
        EMPLOYEE_INFO[1]:skills[2].proficiency_level
FROM  JSON_DATA_TABLE_UNION
UNION ALL
SELECT         

        EMPLOYEE_INFO[2]:employee_id,
        EMPLOYEE_INFO[2]:employee_name,
        EMPLOYEE_INFO[2]:position, 
        EMPLOYEE_INFO[2]:phone_numbers[0],
        EMPLOYEE_INFO[2]:phone_numbers[1],
        EMPLOYEE_INFO[2]:address.city,        
        EMPLOYEE_INFO[2]:address.state,
        EMPLOYEE_INFO[2]:address.street,
        EMPLOYEE_INFO[2]:address.zip_code,
        EMPLOYEE_INFO[2]:skills[0].skill_name,
        EMPLOYEE_INFO[2]:skills[0].proficiency_level,
        EMPLOYEE_INFO[2]:skills[1].skill_name,
        EMPLOYEE_INFO[2]:skills[1].proficiency_level,
        EMPLOYEE_INFO[2]:skills[2].skill_name,
        EMPLOYEE_INFO[2]:skills[2].proficiency_level
FROM  JSON_DATA_TABLE_UNION
UNION ALL
SELECT         

        EMPLOYEE_INFO[3]:employee_id,
        EMPLOYEE_INFO[3]:employee_name,
        EMPLOYEE_INFO[3]:position, 
        EMPLOYEE_INFO[3]:phone_numbers[0],
        EMPLOYEE_INFO[3]:phone_numbers[1],
        EMPLOYEE_INFO[3]:address.city,        
        EMPLOYEE_INFO[3]:address.state,
        EMPLOYEE_INFO[3]:address.street,
        EMPLOYEE_INFO[3]:address.zip_code,
        EMPLOYEE_INFO[3]:skills[0].skill_name,
        EMPLOYEE_INFO[3]:skills[0].proficiency_level,
        EMPLOYEE_INFO[3]:skills[1].skill_name,
        EMPLOYEE_INFO[3]:skills[1].proficiency_level,
        EMPLOYEE_INFO[3]:skills[2].skill_name,
        EMPLOYEE_INFO[3]:skills[2].proficiency_level
FROM  JSON_DATA_TABLE_UNION
UNION ALL
SELECT         

        EMPLOYEE_INFO[4]:employee_id,
        EMPLOYEE_INFO[4]:employee_name,
        EMPLOYEE_INFO[4]:position, 
        EMPLOYEE_INFO[4]:phone_numbers[0],
        EMPLOYEE_INFO[4]:phone_numbers[1],
        EMPLOYEE_INFO[4]:address.city,        
        EMPLOYEE_INFO[4]:address.state,
        EMPLOYEE_INFO[4]:address.street,
        EMPLOYEE_INFO[4]:address.zip_code,
        EMPLOYEE_INFO[4]:skills[0].skill_name,
        EMPLOYEE_INFO[4]:skills[0].proficiency_level,
        EMPLOYEE_INFO[4]:skills[1].skill_name,
        EMPLOYEE_INFO[4]:skills[1].proficiency_level,
        EMPLOYEE_INFO[4]:skills[2].skill_name,
        EMPLOYEE_INFO[4]:skills[2].proficiency_level
FROM  JSON_DATA_TABLE_UNION;



SELECT         
        EMPLOYEE_INFO,
        fp.VALUE
FROM  JSON_DATA_TABLE_UNION , LATERAL FLATTEN (EMPLOYEE_INFO) fp ;


SELECT         
        VALUE:employee_id::STRING                             AS EMPLOYEE_ID,
        VALUE:employee_name::STRING                           AS EMPLOYEE_NAME,        
        VALUE:position::STRING                                AS JOB_RANK,       
        VALUE:phone_numbers[0]::STRING                        AS PRIMARY_PHONE_NUMBER,
        VALUE:phone_numbers[1]::STRING                        AS SECONDARY_PHONE_NUMBER,
        VALUE:address.street::STRING                          AS STREET_ADDRESS, 
        VALUE:address.city::STRING                            AS CITY,
        VALUE:address.state::STRING                           AS STATE,
        VALUE:address.zip_code::STRING                        AS ZIP_CODE,
        VALUE:skills[0].skill_name::STRING                    AS SKILL_NAME_1,
        VALUE:skills[0].proficiency_level::STRING             AS SKILL1_PROFICIENCY_LEVEL,                    
        VALUE:skills[1].skill_name::STRING                    AS SKILL_NAME_2,
        VALUE:skills[1].proficiency_level::STRING             AS SKILL2_PROFICIENCY_LEVEL,  
        VALUE:skills[2].skill_name::STRING                    AS SKILL_NAME_3,        
        VALUE:skills[2].proficiency_level::STRING             AS SKILL3_PROFICIENCY_LEVEL
from JSON_DATA_TABLE_UNION , LATERAL FLATTEN (EMPLOYEE_INFO) fp ;







SELECT         
        EMPLOYEE_INFO:employee_id,
        EMPLOYEE_INFO:employee_name,
        EMPLOYEE_INFO:position,
        EMPLOYEE_INFO:phone_numbers[0],
        EMPLOYEE_INFO:phone_numbers[1],
        EMPLOYEE_INFO:skills[0],
        EMPLOYEE_INFO:skills[1],
        EMPLOYEE_INFO:skills[2]
FROM EMPLOYEE_DATA_JSON;


SELECT         -- There is only 1 row in EMPLOYEE_DATA_JSON so query will return only 1 row
        EMPLOYEE_INFO:employee_id::STRING  employee_id,
        EMPLOYEE_INFO:employee_name::STRING  employee_name,
        EMPLOYEE_INFO:position::STRING  position,
        EMPLOYEE_INFO:phone_numbers[0]::STRING phone_number_1,
        EMPLOYEE_INFO:phone_numbers[1]::STRING phone_number_2
FROM EMPLOYEE_DATA_JSON;


SELECT -- This query will return multiple rows, one for each employee in the multi JSON array
    emp.value:employee_id::STRING   AS employee_id,
    emp.value:employee_name::STRING AS employee_name,
    emp.value:position::STRING      AS position,
    emp.value:phone_numbers[0]::STRING AS phone_number_1,
    emp.value:phone_numbers[1]::STRING AS phone_number_2
FROM JSON_DATA_TABLE_UNION,
     LATERAL FLATTEN(input => JSON_DATA_TABLE_UNION.EMPLOYEE_INFO) emp;
