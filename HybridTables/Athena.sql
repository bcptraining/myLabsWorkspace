-- Context
SELECT current_user;

----- Create EMPLOYEES table
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

-- Explore data
SELECT * FROM "hrms"."aws_athena_employees" limit 10;


-- Create an iceberg table for this same data (specify different output folder)
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

