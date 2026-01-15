SELECT system$generate_external_volume_credentials('ICEBERG_VOLUME_UNMANAGED');
select system$generate_snowflake_access_token();

USE DATABASE ICEBERG;

CREATE OR REPLACE ICEBERG TABLE HR.AWS_ATHENA_EMPLOYEES_ICEBERG (
  employee_id INT,
  firstname STRING,
  last_name STRING,
  email STRING,
  phone_number STRING,
  hire_date STRING,
  salary DOUBLE,
  commission_pct DOUBLE,
  manager_id INT,
  department_id INT
)
EXTERNAL_VOLUME = ICEBERG_VOLUME_UNMANAGED
CATALOG = 'SNOWFLAKE';
select * from HR.AWS_ATHENA_EMPLOYEES_ICEBERG;

CREATE OR REPLACE ICEBERG TABLE HR.TEST_ICEBERG
EXTERNAL_VOLUME = ICEBERG_VOLUME_UNMANAGED
CATALOG = 'SNOWFLAKE'
AS SELECT 1 AS x;

select * from HR.TEST_ICEBERG;
