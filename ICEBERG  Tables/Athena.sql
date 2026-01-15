-----------------------------------
-- Context
-----------------------------------
SELECT current_user; -- I needed this in order to get permissions correct

-----------------------------------
-- Create athena EMPLOYEES table
-----------------------------------
CREATE EXTERNAL TABLE hrms.aws_athena_employees (
  employee_id int,
  firstname string,
  last_name string,
  email string,
  phone_number string,
  hire_date string,
  salary double,
  commission_pct double,
  manager_id int,
  department_id int
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://vdw-dev-iceberg/employees/';

-- Explore Athena table data
SELECT * FROM "hrms"."aws_athena_employees" limit 10;

-----------------------------------
--  Create an AWS ICEBERG table and load from the Athena EMPLOYEES table
-----------------------------------
CREATE TABLE hrms.aws_athena_employees_iceberg (
  employee_id int,
  firstname string,
  last_name string,
  email string,
  phone_number string,
  hire_date string,
  salary double,
  commission_pct double,
  manager_id int,
  department_id int
)
LOCATION 's3://vdw-dev-iceberg/employees_iceberg/'
TBLPROPERTIES (
  'table_type'='ICEBERG'
);
--  Load data from the employees athena table
INSERT INTO hrms.aws_athena_employees_iceberg
SELECT * FROM HRMS.AWS_ATHENA_EMPLOYEES; 

-- update employe 104 in the iceberg table_type
select * from hrms.aws_athena_employees_iceberg where employee_id = 104;
UPDATE hrms.aws_athena_employees_iceberG 
SET FIRSTNAME = 'BRUCE'
WHERE employee_id = 104;
