-- Question 1
use database LoadingData;
use schema LOAD;

create or replace file format my_json  -- Create a JSON file format to process OBJECT,ARRAY and JSON data
type = json;

CREATE or replace STAGE AWS_ETL_JSON_STAGE
URL = 's3://vdw-dev-ingest/loadingdatalabs/JSON/A4/'
STORAGE_INTEGRATION = udemy_MC_A1_SI
FILE_FORMAT = (TYPE = JSON);

create table laptop_json_data (  -- Create a table to load JSON data
    laptop_info variant
);
copy into laptop_json_data from @AWS_ETL_JSON_STAGE/laptop_json.json;

list @AWS_ETL_JSON_STAGE;

-- Question 2

create table laptop_multi_json_data (  -- Create a table to load JSON data
    laptop_info variant
);
copy into laptop_multi_json_data from @AWS_ETL_JSON_STAGE/laptop_multi_json_array.json;

select 
ls.value:brand::STRING      AS brand,
ls.value:model::STRING      AS model,
fkeys.key::STRING           AS feature_name,
fkeys.value::STRING         AS feature_value,
port.value::STRING          AS port_name,
CAST(ls.value:reviews.averageRating AS NUMBER(10,1)) AS average_rating,
ls.value:reviews.numberOfReviews::NUMBER  AS number_of_reviews
from laptop_multi_json_data, 
lateral flatten (input => laptop_info:laptopSales) ls, -- This gives you a row per laptop object
LATERAL FLATTEN(input => ls.value:features) ff, -- This gives you a row per feature object
LATERAL FLATTEN(input => ff.value) fkeys, -- This enables you to extract key value pairs from the features object
LATERAL FLATTEN(input => ls.value:ports) port;

-- Question 3

create OR REPLACE table laptop_multi_json_data_flattened
(  
    BRAND VARCHAR(40),
    MODEL VARCHAR(40),  
    FEATURE_NAME VARCHAR(100),
    FEATURE_VALUE VARCHAR(100), 
    PORTS VARCHAR(1000),
    AVERAGE_RATING NUMBER(10,1),
    NUMBER_OF_REVIEWS NUMBER
);

INSERT INTO laptop_multi_json_data_flattened
(
    BRAND,
    MODEL,
    FEATURE_NAME,
    FEATURE_VALUE,
    PORTS,
    AVERAGE_RATING,
    NUMBER_OF_REVIEWS
)
SELECT 
    ls.value:brand::STRING      AS brand,
    ls.value:model::STRING      AS model,
    fkeys.key::STRING           AS feature_name,
    fkeys.value::STRING         AS feature_value,
    port.value::STRING          AS port_name,
    CAST(ls.value:reviews.averageRating AS NUMBER(10,1)) AS average_rating,
    ls.value:reviews.numberOfReviews::NUMBER            AS number_of_reviews
FROM laptop_multi_json_data,
     LATERAL FLATTEN(input => laptop_info:laptopSales) ls,
     LATERAL FLATTEN(input => ls.value:features) ff,
     LATERAL FLATTEN(input => ff.value) fkeys,
     LATERAL FLATTEN(input => ls.value:ports) port;

select * from laptop_multi_json_data_flattened;