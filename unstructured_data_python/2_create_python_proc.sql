/*--------------------------
Set context
--------------------------*/
use database unstructured_data;
use schema pdf;


CREATE OR REPLACE PROCEDURE PROCESS_PDFS(
      pdf_stage_name STRING DEFAULT '@UNSTRUCTURED_DATA.PDF.PDF_STAGE',
      pdf_extract_table_name STRING DEFAULT 'PDF_TEXT_OUTPUT'
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
"""
================================================================================
PROCESS_PDFS Stored Procedure
--------------------------------------------------------------------------------
Purpose:
    - Reads all PDF files from a specified Snowflake stage.
    - Extracts text using the existing extract_pdf_from_bytes() UDF.
    - Writes results into a target table (default: PDF_TEXT_OUTPUT).
    - Ensures the target table exists and has the correct schema.
    - Uses a transient staging table to guarantee atomic refresh behavior.

Parameters:
    pdf_stage_name (STRING)
        - Name of the stage containing PDF files.
        - May be passed with or without '@'.
        - Defaults to @UNSTRUCTURED_DATA.PDF.PDF_STAGE.

    pdf_extract_table_name (STRING)
        - Name of the final output table.
        - Defaults to PDF_TEXT_OUTPUT.

Behavior:
    1. Normalize and validate input parameters.
    2. Ensure the output table exists and matches expected schema.
       - If missing or mismatched → recreate it.
       - If valid → truncate it.
    3. Create a transient staging table (PDF_TEXT_TEMP).
    4. List all files in the stage and process only *.pdf files.
    5. Extract text via UDF and insert into staging table.
    6. If any PDFs were processed → load staging table into final table.
    7. Return a summary message.

Notes for Future Developers:
    - This procedure is idempotent: running it repeatedly produces consistent results.
    - Only PDF files are processed; other file types are ignored.
    - Errors during PDF extraction are captured and stored as text rows.
    - The transient staging table is recreated on every run.
================================================================================
"""

from snowflake.snowpark import Session
from snowflake.snowpark.functions import call_udf, current_timestamp, col
import traceback

# Expected schema for the final output table
EXPECTED_SCHEMA = [
    ("FILE_NAME", "STRING"),
    ("EXTRACTED_TEXT", "STRING"),
    ("PROCESSED_AT", "TIMESTAMP")
]

def table_exists(session, table_name):
    """Return True if the table exists in the current database/schema."""
    result = session.sql(f"SHOW TABLES LIKE '{table_name}'").collect()
    return len(result) > 0

def schema_matches(session, table_name):
    """
    Validate that the table's first three columns match EXPECTED_SCHEMA.
    Column names and types are compared case-insensitively.
    """
    desc = session.sql(f"DESC TABLE {table_name}").collect()
    cols = [(row['name'].upper(), row['type'].upper()) for row in desc]

    for (exp_name, exp_type), (act_name, act_type) in zip(EXPECTED_SCHEMA, cols):
        if exp_name != act_name or exp_type not in act_type:
            return False
    return True

def create_output_table(session, table_name):
    """Create the output table with the expected schema."""
    session.sql(f"""
        CREATE OR REPLACE TRANSIENT TABLE {table_name} (
            file_name STRING,
            extracted_text STRING,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
    """).collect()

def run(session: Session, pdf_stage_name: str, pdf_extract_table_name: str):

    # Normalize stage name (ensure it begins with '@')
    pdf_stage_name = pdf_stage_name.strip()
    if not pdf_stage_name.startswith("@"):
        pdf_stage_name = "@" + pdf_stage_name

    # Normalize table name
    pdf_extract_table_name = pdf_extract_table_name.strip()

    # -------------------------------------------------------------------------
    # Validate or create the output table
    # -------------------------------------------------------------------------
    if not table_exists(session, pdf_extract_table_name):
        create_output_table(session, pdf_extract_table_name)
    else:
        if not schema_matches(session, pdf_extract_table_name):
            # Schema mismatch → recreate table
            create_output_table(session, pdf_extract_table_name)
        else:
            # Schema matches → truncate for fresh load
            session.sql(f"TRUNCATE TABLE {pdf_extract_table_name}").collect()

    # -------------------------------------------------------------------------
    # Create transient staging table for atomic refresh
    # -------------------------------------------------------------------------
    session.sql("""
        CREATE OR REPLACE TRANSIENT TABLE PDF_TEXT_TEMP (
            file_name STRING,
            extracted_text STRING,
            processed_at TIMESTAMP
        )
    """).collect()

    # -------------------------------------------------------------------------
    # List files in the stage
    # -------------------------------------------------------------------------
    files = session.sql(f"LIST {pdf_stage_name}").collect()

    processed_count = 0

    # -------------------------------------------------------------------------
    # Process each file in the stage
    # -------------------------------------------------------------------------
    for f in files:
        full_name = f['name']              # e.g. "pdf_stage/Report2025.pdf"
        file_name = full_name.split('/')[-1]

        # Only process PDF files
        if not file_name.lower().endswith(".pdf"):
            continue

        try:
            # Read file bytes from stage
            with session.file.get_stream(f"{pdf_stage_name}/{file_name}") as stream:
                pdf_bytes = stream.read()

            # Extract text using the UDF
            df = session.create_dataframe([(pdf_bytes,)], schema=["pdf_bytes"])
            extracted = df.select(
                call_udf("extract_pdf_from_bytes", col("pdf_bytes"))
            ).collect()[0][0]

            # Insert successful extraction into staging table
            session.create_dataframe(
                [(file_name, extracted)],
                schema=["file_name", "extracted_text"]
            ).with_column("processed_at", current_timestamp()) \
             .write.mode("append").save_as_table("PDF_TEXT_TEMP")

            processed_count += 1

        except Exception:
            # Capture error details instead of failing the entire run
            err = f"ERROR processing {file_name}: {traceback.format_exc()}"
            session.create_dataframe(
                [(file_name, err)],
                schema=["file_name", "extracted_text"]
            ).with_column("processed_at", current_timestamp()) \
             .write.mode("append").save_as_table("PDF_TEXT_TEMP")

    # -------------------------------------------------------------------------
    # Move staging → final output table (only if we processed PDFs)
    # -------------------------------------------------------------------------
    if processed_count > 0:
        session.sql(f"""
            INSERT INTO {pdf_extract_table_name}
            SELECT file_name, extracted_text, processed_at
            FROM PDF_TEXT_TEMP
        """).collect()

    return f"Processed {processed_count} PDF files from stage {pdf_stage_name} into table {pdf_extract_table_name}."
$$;


/*--------------------------
Create python sproc to process all pdfs in stage: Function PROCESS_PDFS successfully created.
--------------------------*/
-- SHOW PROCEDURES LIKE 'PROCESS_PDFS';
-- DROP PROCEDURE PROCESS_PDFS();
-- DROP PROCEDURE IF EXISTS PROCESS_PDFS(STRING);
-- CREATE OR REPLACE PROCEDURE PROCESS_PDFS(
--       pdf_stage_name STRING DEFAULT '@UNSTRUCTURED_DATA.PDF.PDF_STAGE',
--       pdf_extract_table_name STRING DEFAULT 'PDF_TEXT_OUTPUT'
-- )
-- RETURNS STRING
-- LANGUAGE PYTHON
-- RUNTIME_VERSION = '3.10'
-- PACKAGES = ('snowflake-snowpark-python')
-- HANDLER = 'run'
-- AS
-- $$
-- from snowflake.snowpark import Session
-- from snowflake.snowpark.functions import call_udf, current_timestamp, col
-- import traceback

-- EXPECTED_SCHEMA = [
--     ("FILE_NAME", "STRING"),
--     ("EXTRACTED_TEXT", "STRING"),
--     ("PROCESSED_AT", "TIMESTAMP")
-- ]

-- def table_exists(session, table_name):
--     result = session.sql(f"SHOW TABLES LIKE '{table_name}'").collect()
--     return len(result) > 0

-- def schema_matches(session, table_name):
--     desc = session.sql(f"DESC TABLE {table_name}").collect()
--     cols = [(row['name'].upper(), row['type'].upper()) for row in desc]

--     # Compare only the first 3 columns (ignore defaults)
--     for (exp_name, exp_type), (act_name, act_type) in zip(EXPECTED_SCHEMA, cols):
--         if exp_name != act_name or exp_type not in act_type:
--             return False
--     return True

-- def create_output_table(session, table_name):
--     session.sql(f"""
--         CREATE OR REPLACE TRANSIENT TABLE {table_name} (
--             file_name STRING,
--             extracted_text STRING,
--             processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
--         )
--     """).collect()

-- def run(session: Session, pdf_stage_name: str, pdf_extract_table_name: str):

--     # Normalize stage name
--     pdf_stage_name = pdf_stage_name.strip()
--     if not pdf_stage_name.startswith("@"):
--         pdf_stage_name = "@" + pdf_stage_name

--     # Normalize table name
--     pdf_extract_table_name = pdf_extract_table_name.strip()

--     # --- Validate or create output table ---
--     if not table_exists(session, pdf_extract_table_name):
--         create_output_table(session, pdf_extract_table_name)
--     else:
--         if not schema_matches(session, pdf_extract_table_name):
--             create_output_table(session, pdf_extract_table_name)
--         else:
--             # Table exists and schema matches → truncate it
--             session.sql(f"TRUNCATE TABLE {pdf_extract_table_name}").collect()

--     # --- Create transient staging table ---
--     session.sql("""
--         CREATE OR REPLACE TRANSIENT TABLE PDF_TEXT_TEMP (
--             file_name STRING,
--             extracted_text STRING,
--             processed_at TIMESTAMP
--         )
--     """).collect()

--     # --- List files in stage ---
--     files = session.sql(f"LIST {pdf_stage_name}").collect()

--     processed_count = 0

--     for f in files:
--         full_name = f['name']
--         file_name = full_name.split('/')[-1]

--         if not file_name.lower().endswith(".pdf"):
--             continue

--         try:
--             # Read file bytes
--             with session.file.get_stream(f"{pdf_stage_name}/{file_name}") as stream:
--                 pdf_bytes = stream.read()

--             # Call your existing UDF
--             df = session.create_dataframe([(pdf_bytes,)], schema=["pdf_bytes"])
--             extracted = df.select(
--                 call_udf("extract_pdf_from_bytes", col("pdf_bytes"))
--             ).collect()[0][0]

--             # Insert into staging table
--             session.create_dataframe(
--                 [(file_name, extracted)],
--                 schema=["file_name", "extracted_text"]
--             ).with_column("processed_at", current_timestamp()) \
--              .write.mode("append").save_as_table("PDF_TEXT_TEMP")

--             processed_count += 1

--         except Exception:
--             err = f"ERROR processing {file_name}: {traceback.format_exc()}"
--             session.create_dataframe(
--                 [(file_name, err)],
--                 schema=["file_name", "extracted_text"]
--             ).with_column("processed_at", current_timestamp()) \
--              .write.mode("append").save_as_table("PDF_TEXT_TEMP")

--     # --- Move staging → final output table ---
--     if processed_count > 0:
--         session.sql(f"""
--             INSERT INTO {pdf_extract_table_name}
--             SELECT file_name, extracted_text, processed_at
--             FROM PDF_TEXT_TEMP
--         """).collect()

--     return f"Processed {processed_count} PDF files from stage {pdf_stage_name} into table {pdf_extract_table_name}."
-- $$;


