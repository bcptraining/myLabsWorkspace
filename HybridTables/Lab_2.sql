--------------------
-- MC-AWS-HYBRID TABLES ASSIGNMENT PDF --Q1 --Create a new Database with any name of your choice .Create  a new schema in the database you just created 
--------------------
CREATE OR REPLACE DATABASE NEW_UNISTORE ; 
CREATE OR REPLACE SCHEMA NEW_HR; --Q2 --Get the table structure of the CUSTOMER and ORDERS table from Snowflake 
SNOWFLAKE_SAMPLE_DATA.TPCH_SF1 schema --by using DESCRIBE TABLE <TableName> 
DESCRIBE TABLE SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER; 
DESCRIBE TABLE SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS; --Q3 --Create the  CUSTOMER table  as a HYBRID table and set the column C_CUSTKEY as the 
PK,C_PHONE as the Unique Key 
CREATE OR REPLACE HYBRID TABLE CUSTOMER ( 
C_CUSTKEY      NUMBER(38,0)    PRIMARY KEY, 
C_NAME         
VARCHAR(25), 
C_ADDRESS      VARCHAR(40), 
C_NATIONKEY    NUMBER(38,0), 
C_PHONE        
VARCHAR(15)      UNIQUE, 
C_ACCTBAL      NUMBER(12,2), 
C_MKTSEGMENT   VARCHAR(10), 
C_COMMENT      VARCHAR(117) 
); 
--Q4 --Create the ORDERS table as a HYBRID table and set the column O_ORDERKEY as the PK 
and the O_CUST_KEY as FK to the CUSTOMERS table C_CUSTKEY. --When creating ORDERS Create an Index on O_ORDERDATE 
CREATE OR REPLACE HYBRID TABLE ORDERS ( 
O_ORDERKEY         
NUMBER(38,0) PRIMARY KEY, 
O_CUSTKEY          
NUMBER(38,0) FOREIGN KEY REFERENCES 
CUSTOMER(C_CUSTKEY), 
O_ORDERSTATUS      VARCHAR(1), 
O_TOTALPRICE       
NUMBER(12,2), 
O_ORDERDATE        
DATE, 
O_ORDERPRIORITY    VARCHAR(15), 
O_CLERK            
VARCHAR(15), 
O_SHIPPRIORITY     NUMBER(38,0), 
O_COMMENT          
VARCHAR(79), 
INDEX INDEX_ORDER_DATE(O_ORDERDATE) 
); --Q5 --Insert 5 rows in the CUSTOMERS table and ensure that the PK and Unique Key is not violated 
INSERT INTO CUSTOMER VALUES 
(135936,'Customer#000135936','qcfTTfM2uTIaf',11,'21-482-295-1589',4777.54,'HOUSEHOLD',' 
fully above the slyly even decoys. fluffily bold packages impress. final accounts sleep. furiously 
un'); 
INSERT INTO CUSTOMER VALUES 
(135937,'Customer#000135937','g6gmDfzotGIaaqMceZzcN7ZDVvgIhtnN04lDqAF',21,'31-176-8 
82-3362',5069.65,'BUILDING','lyly regular deposits. regular, even frets  requests along t'); 
INSERT INTO CUSTOMER VALUES( 
135938,'Customer#000135938','VnWcN3 
2XwOc55SNi',19,'29-653-849-5482',8675.25,'HOUSEHOLD','en accounts sleep furiously. ironic 
requests haggle'); 
INSERT INTO CUSTOMER VALUES 
(135939,'Customer#000135939','6S0TgG 
0FAaytXVKAP',4,'14-524-594-3734',9024.47,'MACHINERY','beans are above the final 
instructions. ideas about the slyly ironic deposits affix furious'); 
INSERT INTO CUSTOMER VALUES 
(135940,'Customer#000135940','AU5DoNiiA8ujZBS5qFOrw9z5sxAghQ',17,'27-874-905-1997',4 
315.85,'BUILDING','deposits.  quickly around'); 
INSERT INTO CUSTOMER VALUES 
(135941,'Customer#000135941','53jrg9M9CLWi7RU2PSQM',14,'24-716-197-8674',555.78,'HO 
USEHOLD','kages boost  slyly busy packages boost'); --Q6 --Insert 5 rows in the ORDERS table and ensure that the PK and FK are not violated 
INSERT INTO ORDERS 
VALUES(2400001,135936,'P',80291.26,'1995-04-03','3-MEDIUM','Clerk#000000085',1,'en 
accounts sleep furiously. ironic requests haggle'); 
INSERT INTO ORDERS 
VALUES(2400002,135937,'F',119166.38,'1994-08-25','2-HIGH','Clerk#000000698',2,'en 
instructions sleep furiously. ironic requests haggle'); 
INSERT INTO ORDERS 
VALUES(2400003,135938,'O',271227.3,'1996-07-09','1-URGENT','Clerk#000000557',3,'en 
accounts beans furiously. ironic requests haggle'); 
INSERT INTO ORDERS 
VALUES(2400004,135940,'O',150853.32,'1996-09-30','1-URGENT','Clerk#000000938',4,'en 
quickly sleep furiously. ironic requests haggle'); 
INSERT INTO ORDERS 
VALUES(2400005,135941,'F',96488.6,'1994-05-03','2-HIGH','Clerk#000000844',5,'en around 
ironic furiously. ironic requests haggle'); --Q7 --Attempt to re-insert a duplicate C_CUSTKEY in the CUSTOMER TABLE and check if the the 
Hybrid table allows insert. 
INSERT INTO CUSTOMER VALUES 
(135936,'Customer#000135936','qcfTTfM2uTIaf',11,'21-482-295-1589',4777.54,'HOUSEHOLD',' 
fully above the slyly even decoys. fluffily bold packages impress. final accounts sleep. furiously 
un'); --Q8 --Attempt to insert a row in ORDERS table with a CUSTKEY that is not in CUSTOMER and see 
if the FK error is encountered. 
INSERT INTO ORDERS 
VALUES(3400006,135949,'F',96488.6,'1994-05-03','2-HIGH','Clerk#000000844',5,'en around 
ironic furiously. ironic requests haggle'); --Q9 
--Select data from the Information_schema view that provides constraint information about 
referential integrity and once the constraint is located please drop the constraint 
SELECT * FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS; 
ALTER TABLE ORDERS DROP CONSTRAINT 
"SYS_CONSTRAINT_5d5f33fa-d988-400a-9ccf-b977126139e5" --Q10 --Once the referential integrity constraint is dropped, select data from 
INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS and verify that the constraint has 
been dropped, Re-insert row into orders table --with a CUST_KEY which is not in the CUSTOMER table 
INSERT INTO ORDERS 
VALUES(3400006,135949,'F',96488.6,'1994-05-03','2-HIGH','Clerk#000000844',5,'en around 
ironic furiously. ironic requests haggle');