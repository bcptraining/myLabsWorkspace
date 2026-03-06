-- Enable cortex functions needed to extract text from pdf files

GRANT DATABASE ROLE SNOWFLAKE.DOCUMENT_INTELLIGENCE_CREATOR
    TO ROLE accountadmin;
SHOW FUNCTIONS LIKE 'AI_EXTRACT%';
