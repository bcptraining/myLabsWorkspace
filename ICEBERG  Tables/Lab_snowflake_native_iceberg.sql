-----------------------------------
-- Try Snowflake-native Iceberg
-----------------------------------
-- Context
CREATE DATABASE IF NOT EXISTS ICEBERG_LAB;
CREATE SCHEMA IF NOT EXISTS ICEBERG_LAB.DEMO;
USE SCHEMA ICEBERG_LAB.DEMO;
-- DDL: Create iceberg table managed bySnowflake
CREATE OR REPLACE ICEBERG TABLE ICEBERG_LAB.DEMO.employees_native
(
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
CATALOG = SNOWFLAKE
-- BASE_LOCATION = 's3://vdw-dev-iceberg/native_employees/';  -- <-- This location does not exist
BASE_LOCATION = 's3://vdw-dev-iceberg/snowflake_employees_iceberg/'; -- This location does exist

-- DML
INSERT INTO ICEBERG_LAB.DEMO.employees_native VALUES
    (101, 'JOHN',   'SMITH',     'JSMITH',   '555-1001', '2019-03-12', 75000, 0.10, 200, 10),
    (102, 'SARA',   'JONES',     'SJONES',   '555-1002', '2020-07-01', 68000, 0.05, 201, 20),
    (103, 'MICHAEL','BROWN',     'MBROWN',   '555-1003', '2018-11-20', 82000, 0.12, 200, 30),
    (104, 'BRUCE',  'ERNST',     'BERNST',   '555-1004', '2020-01-01', 60000, 0.10, 100, 60),
    (105, 'DAVID',  'AUSTIN',    'DAUSTIN',  '555-1005', '2021-02-01', 70000, 0.15, 101, 60),
    (106, 'NANCY',  'GREEN',     'NGREEN',   '555-1006', '2017-05-14', 90000, 0.20, 202, 40),
    (107, 'KEVIN',  'LEE',       'KLEE',     '555-1007', '2022-03-10', 54000, 0.00, 203, 50),
    (108, 'AMY',    'WILSON',    'AWILSON',  '555-1008', '2019-09-09', 72000, 0.08, 200, 20),
    (109, 'ROBERT', 'TAYLOR',    'RTAYLOR',  '555-1009', '2016-12-01', 88000, 0.18, 202, 30),
    (110, 'LINDA',  'MARTIN',    'LMARTIN',  '555-1010', '2020-04-22', 64000, 0.05, 201, 10),
    (111, 'JAMES',  'HARRIS',    'JHARRIS',  '555-1011', '2018-08-15', 76000, 0.10, 200, 40),
    (112, 'KAREN',  'CLARK',     'KCLARK',   '555-1012', '2021-06-30', 58000, 0.03, 203, 50),
    (113, 'BRIAN',  'LEWIS',     'BLEWIS',   '555-1013', '2017-10-05', 81000, 0.12, 202, 30),
    (114, 'MEGAN',  'YOUNG',     'MYOUNG',   '555-1014', '2019-01-19', 69000, 0.07, 201, 20),
    (115, 'CHRIS',  'KING',      'CKING',    '555-1015', '2022-02-11', 53000, 0.00, 203, 50),
    (116, 'JULIA',  'SCOTT',     'JSCOTT',   '555-1016', '2018-04-03', 84000, 0.15, 202, 40),
    (117, 'PETER',  'ADAMS',     'PADAMS',   '555-1017', '2016-07-27', 91000, 0.20, 200, 30),
    (118, 'RACHEL', 'BAKER',     'RBAKER',   '555-1018', '2020-10-10', 62000, 0.05, 201, 10),
    (119, 'TOM',    'CAMPBELL',  'TCAMP',    '555-1019', '2021-03-03', 57000, 0.02, 203, 50),
    (120, 'EMILY',  'PARKER',    'EPARKER',  '555-1020', '2019-12-12', 73000, 0.09, 200, 20);

-- 🧊 1. Query the table normally
select * from ICEBERG_LAB.DEMO.employees_native order by firstname;
-- 2. Perform updates — this triggers Iceberg snapshot creation
UPDATE ICEBERG_LAB.DEMO.employees_native
SET salary = salary + 5000
WHERE employee_id IN (104, 105);

DELETE FROM ICEBERG_LAB.DEMO.employees_native
WHERE employee_id = 120;

INSERT INTO ICEBERG_LAB.DEMO.employees_native VALUES
(121, 'ALICE', 'MORRIS', 'AMORRIS', '555-1021', '2023-01-01', 65000, 0.05, 200, 20);

-- 3. Inspect Iceberg snapshots
-- CALL SYSTEM$LIST_ICEBERG_SNAPSHOTS('ICEBERG_LAB.DEMO.employees_native');
-- error: SQL compilation error: Unknown function SYSTEM$LIST_ICEBERG_SNAPSHOTS
