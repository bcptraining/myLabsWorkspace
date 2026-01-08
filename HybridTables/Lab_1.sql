---------------------------------
-- MC-AWS-HYBRID-TABLES-VIDEO-LAB: Hybrid tables are currently not available to trial accounts.
---------------------------------
-- Establish/set context
CREATE OR REPLACE DATABASE UNISTORE; 
CREATE SCHEMA HYBRID; 

-- DDL: Create first hybrid table
CREATE HYBRID TABLE DEPARTMENTS ( 
DEPARTMENT_ID    NUMBER(4)    PRIMARY KEY, 
DEPARTMENT_NAME  VARCHAR2(30) , 
MANAGER_ID       NUMBER(6), 
LOCATION_ID      NUMBER(4) 
); 

-- Investigate
SHOW HYBRID TABLES ; 
SHOW TABLES ; 

SELECT * FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA <> 'INFORMATION_SCHEMA' ;

-- Insert example data
INSERT INTO DEPARTMENTS (DEPARTMENT_ID, DEPARTMENT_NAME, MANAGER_ID, 
LOCATION_ID) VALUES ( 10, 'ADMINISTRATION', 200, 1700); 
INSERT INTO DEPARTMENTS (DEPARTMENT_ID, DEPARTMENT_NAME, MANAGER_ID, 
LOCATION_ID) VALUES ( 20, 'MARKETING', 201, 1800); 
INSERT INTO DEPARTMENTS (DEPARTMENT_ID, DEPARTMENT_NAME, MANAGER_ID, 
LOCATION_ID) VALUES ( 30, 'PURCHASING', 114, 1700); 
INSERT INTO DEPARTMENTS (DEPARTMENT_ID, DEPARTMENT_NAME, MANAGER_ID, 
LOCATION_ID) VALUES ( 40, 'HUMAN RESOURCES', 203, 2400); 
INSERT INTO DEPARTMENTS (DEPARTMENT_ID, DEPARTMENT_NAME, MANAGER_ID, 
LOCATION_ID) VALUES ( 50, 'SHIPPING', 121, 1500); 
INSERT INTO DEPARTMENTS (DEPARTMENT_ID, DEPARTMENT_NAME, MANAGER_ID, 
LOCATION_ID) VALUES ( 60, 'IT', 103, 1400); 
INSERT INTO DEPARTMENTS (DEPARTMENT_ID, DEPARTMENT_NAME, MANAGER_ID, 
LOCATION_ID) VALUES ( 70, 'PUBLIC RELATIONS', 204, 2700); 
INSERT INTO DEPARTMENTS (DEPARTMENT_ID, DEPARTMENT_NAME, MANAGER_ID, 
LOCATION_ID) VALUES ( 80, 'SALES', 145, 2500); 
INSERT INTO DEPARTMENTS (DEPARTMENT_ID, DEPARTMENT_NAME, MANAGER_ID, 
LOCATION_ID) VALUES ( 90, 'EXECUTIVE', 100, 1700); 

-- Check on the data loaded
select * from DEPARTMENTS; 

-- Create 2nd hybrid table
CREATE OR REPLACE HYBRID TABLE EMPLOYEES 
( 
EMPLOYEE_ID         
FIRST_NAME          
LAST_NAME           
EMAIL               
phone_number        
HIRE_DATE           
JOB_ID              
SALARY              
NUMBER(6)   PRIMARY KEY, 
VARCHAR(20), 
VARCHAR(25), 
VARCHAR(25)  UNIQUE, 
VARCHAR(20), 
DATE, 
VARCHAR(10), 
NUMBER(8,2) , 
COMMISSION_PCT      NUMBER(2,2), 
MANAGER_ID          
NUMBER(6) , 
DEPARTMENT_ID       
NUMBER(4)   FOREIGN KEY REFERENCES 
DEPARTMENTS(DEPARTMENT_ID), 
INDEX index_hire_date(HIRE_DATE) 
); 

-- Insert data into 2nd hybrid table
INSERT INTO employees (employee_id, first_name, last_name, email, phone_number, 
hire_date, job_id, salary, commission_pct, manager_id, department_id) VALUES 
( 100, 'Steven', 'King', 'SKING', '515.123.4567', TO_DATE('17-06-2003', 'dd-MM-yyyy'), 
'AD_PRES', 24000, NULL, NULL, 90); 
INSERT INTO employees (employee_id, first_name, last_name, email, phone_number, 
hire_date, job_id, salary, commission_pct, manager_id, department_id) VALUES 
( 101, 'Neena', 'Kochhar', 'NKOCHHAR', '515.123.4568', TO_DATE('21-09-2005', 'dd-MM-yyyy'), 
'AD_VP', 17000, NULL, 100, 90); 
INSERT INTO employees (employee_id, first_name, last_name, email, phone_number, 
hire_date, job_id, salary, commission_pct, manager_id, department_id) VALUES 
( 102, 'Lex', 'De Haan', 'LDEHAAN', '515.123.4569', TO_DATE('13-01-2001', 'dd-MM-yyyy'), 
'AD_VP', 17000, NULL, 100, 90); 
SELECT * FROM EMPLOYEES; 
INSERT INTO employees (employee_id, first_name, last_name, email, phone_number, 
hire_date, job_id, salary, commission_pct, manager_id, department_id) VALUES 
( 103, 'Alexander', 'Hunold', 'AHUNOLD', '590.423.4567', TO_DATE('03-01-2006', 
'dd-MM-yyyy'), 'IT_PROG', 9000, NULL, 102, 60); 
INSERT INTO employees (employee_id, first_name, last_name, email, phone_number, 
hire_date, job_id, salary, commission_pct, manager_id, department_id) VALUES 
( 104, 'Bruce', 'Ernst', 'BERNST', '590.423.4568', TO_DATE('21-05-2007', 'dd-MM-yyyy'), 
'IT_PROG', 6000, NULL, 103, 90); 
INSERT INTO employees (employee_id, first_name, last_name, email, phone_number, 
hire_date, job_id, salary, commission_pct, manager_id, department_id) VALUES 
( 105, 'David', 'Austin', 'DAUSTIN', '590.423.4569', TO_DATE('25-06-2005', 'dd-MM-yyyy'), 
'IT_PROG', 4800, NULL, 103, 100); 

-- Indexes
SHOW INDEXES; 
CREATE OR REPLACE INDEX INDEX_PHONE_NUMBER (PHONE_NUMBER) ON 
EMPLOYEES; 
DROP INDEX EMPLOYEES.INDEX_PHONE_NUMBER; 
CREATE OR REPLACE INDEX INDEX_FULL_NAME (FIRST_NAME,LAST_NAME) ON 
EMPLOYEES; 
SELECT * FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS; 
ALTER TABLE EMPLOYEES DROP CONSTRAINT 
"SYS_CONSTRAINT_00272ee2-4b19-457c-9031-a9b7b5260ee5"; 

SELECT * FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS;