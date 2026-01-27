-------------------------------------
-- Create a brand new external volume (prior volume "ICEBERG_VOLUME_UNMANAGED" seems to have entered a zombie state)
-------------------------------------

CREATE EXTERNAL VOLUME ICEBERG_VOLUME_EMPLOYEES_2
  STORAGE_LOCATIONS = (
    (
      NAME = 'HR DATA 2',
      STORAGE_PROVIDER = 's3',
      STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::904233092605:role/iceberg_unmanaged_si',
      STORAGE_BASE_URL = 's3://vdw-dev-iceberg/employees_iceberg/'
    )
  );
GRANT USAGE ON EXTERNAL VOLUME ICEBERG_VOLUME_EMPLOYEES_2 TO ROLE ACCOUNTADMIN;
desc external volume ICEBERG_VOLUME_EMPLOYEES_2;
-- STORAGE_LOCATION_1 = {"NAME":"HR DATA 2","STORAGE_PROVIDER":"S3","STORAGE_BASE_URL":"s3://vdw-dev-iceberg/employees_iceberg/","STORAGE_ALLOWED_LOCATIONS":["s3://vdw-dev-iceberg/employees_iceberg/*"],"STORAGE_REGION":"us-west-2","STORAGE_AWS_ROLE_ARN":"arn:aws:iam::904233092605:role/iceberg_unmanaged_si","STORAGE_AWS_IAM_USER_ARN":"arn:aws:iam::935542360084:user/6nrd1000-s","STORAGE_AWS_EXTERNAL_ID":"EYB38761_SFCRole=3_TLJAHOSUZvTEhPq1FAyz9lyk6D0=","ENCRYPTION_TYPE":"NONE","ENCRYPTION_KMS_KEY_ID":""}
-- ACTIVE = HR DATA 2

CREATE OR REPLACE ICEBERG TABLE "hr.test_visibility"
  CATALOG = CAT_INT_GLUE
  EXTERNAL_VOLUME = ICEBERG_VOLUME_EMPLOYEES_2
  CATALOG_TABLE_NAME = 'hrms.aws_athena_employees_iceberg';
-- AWS Glue service assume role failed for catalog CAT_INT_GLUE with message: User: arn:aws:iam::935542360084:user/6nrd1000-s is not authorized to perform: sts:AssumeRole on resource: arn:aws:iam::904233092605:role/iceberg_unmanaged_si (Service: Sts, Status Code: 403, Request ID: df823bd5-4dd3-40e6-a027-69d83b68acd4) (SDK Attempt Count: 1).
----------------------------------------------
-- iam role info: arn:aws:iam::904233092605:role/iceberg_unmanaged_si
----------------------------------------------
-- Note: The role works for regiular storage integration but not for what I am trying to do with iceberg 

-- Trust policy
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowSnowflakeStorageIntegration",
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::935542360084:user/6nrd1000-s"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": [
            "EYB38761_SFCRole=3_Eorb/6bhHX6xhHAORaKJ95d6wXI=",
            "EYB38761_SFCRole=3_TLJAHOSUZvTEhPq1FAyz9lyk6D0="
          ]
        }
      }
    }
  ]
}
-- Permissions Policy
{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Sid": "StorageListBucketScoped",
			"Effect": "Allow",
			"Action": "s3:ListBucket",
			"Resource": "arn:aws:s3:::vdw-dev-iceberg",
			"Condition": {
				"StringLike": {
					"s3:prefix": [
						"employees/",
						"employees/*",
						"employees_iceberg/",
						"employees_iceberg/*",
						"snowflake_employees_iceberg/",
						"snowflake_employees_iceberg/*"
					]
				}
			}
		},
		{
			"Sid": "StorageReadWriteEmployeesIceberg",
			"Effect": "Allow",
			"Action": [
				"s3:GetObject",
				"s3:GetObjectVersion",
				"s3:PutObject",
				"s3:DeleteObject"
			],
			"Resource": "arn:aws:s3:::vdw-dev-iceberg/employees_iceberg/*"
		},
		{
			"Sid": "StorageReadWriteSnowflakeIceberg",
			"Effect": "Allow",
			"Action": [
				"s3:GetObject",
				"s3:GetObjectVersion",
				"s3:PutObject",
				"s3:DeleteObject"
			],
			"Resource": "arn:aws:s3:::vdw-dev-iceberg/snowflake_employees_iceberg/*"
		},
		{
			"Sid": "StorageReadRawEmployeesParquet",
			"Effect": "Allow",
			"Action": [
				"s3:GetObject",
				"s3:GetObjectVersion"
			],
			"Resource": "arn:aws:s3:::vdw-dev-iceberg/employees/*"
		},
		{
			"Sid": "GlueCatalogReadMetadata",
			"Effect": "Allow",
			"Action": [
				"glue:GetTable",
				"glue:GetTables",
				"glue:GetDatabase",
				"glue:GetDatabases"
			],
			"Resource": "*"
		}
	]
}


-------------------------------------------------------------------
-- Side issue: My prior external volume seems to have entered a "zombie" state.
------------------------------------------------------------------
CREATE OR REPLACE EXTERNAL VOLUME ICEBERG_VOLUME_UNMANAGED -- External volume ICEBERG_VOLUME_UNMANAGED cannot be replaced because it has active table(s) using it.
STORAGE_LOCATIONS =
(
    (
       NAME = 'HR DATA',
       STORAGE_PROVIDER = 's3',
       STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::904233092605:role/iceberg_unmanaged_si',
       STORAGE_BASE_URL = 's3://vdw-dev-iceberg/employees_iceberg/'
    )
);

-- So I deleted the 2 iceberg tables I could see
--. SNOWFLAKE.ACCOUNT_USAGE.ICEBERG_TABLES
DROP TABLE ICEBERG.HR.AWS_ATHENA_EMPLOYEES_ICEBERG;
DROP TABLE ICEBERG.HR.TEST_ICEBERG;

SHOW ICEBERG TABLES -- Query produced no results

-- ISSUE: When I again try to create ICEBERG_VOLUME_UNMANAGED using above statement I still get "External volume ICEBERG_VOLUME_UNMANAGED cannot be replaced because it has active table(s) using it."

