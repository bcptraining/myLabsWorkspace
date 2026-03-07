/*--------------------------
Set context
--------------------------*/
use database unstructured_data;
use schema pdf;

/*--------------------------
Create python sproc to process all pdfs in stage: Function PROCESS_PDFS successfully created.
--------------------------*/
CREATE OR REPLACE PROCEDURE PROCESS_PDFS(pdf_stage_name STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python', 'PyPDF2')
HANDLER = 'run'
AS
$$
from snowflake.snowpark import Session
from snowflake.snowpark.functions import call_udf, current_timestamp, col
import traceback

def run(session: Session, pdf_stage_name: str):

    # Normalize stage name (ensure it starts with @)
    pdf_stage_name = pdf_stage_name.strip()
    if not pdf_stage_name.startswith("@"):
        pdf_stage_name = "@" + pdf_stage_name

    # 1. Create transient staging table
    session.sql("""
        CREATE OR REPLACE TRANSIENT TABLE PDF_TEXT_TEMP (
            file_name STRING,
            extracted_text STRING,
            processed_at TIMESTAMP
        )
    """).collect()

    # List files in stage
    files = session.sql(f"LIST {pdf_stage_name}").collect()

    for f in files:
        full_name = f['name']  # e.g. "pdf_stage/CollabriaAug2025.pdf"
        file_name = full_name.split('/')[-1]

        # 2. Only process .pdf files
        if not file_name.lower().endswith(".pdf"):
            continue

        try:
            # Read file bytes
            with session.file.get_stream(f"{pdf_stage_name}/{file_name}") as stream:
                pdf_bytes = stream.read()

            # Call your existing UDF
            df = session.create_dataframe([(pdf_bytes,)], schema=["pdf_bytes"])
            extracted = df.select(
                call_udf("extract_pdf_from_bytes", col("pdf_bytes"))
            ).collect()[0][0]

            # Insert into staging table
            session.create_dataframe(
                [(file_name, extracted)],
                schema=["file_name", "extracted_text"]
            ).with_column("processed_at", current_timestamp()) \
             .write.mode("append").save_as_table("PDF_TEXT_TEMP")

        except Exception:
            err = f"ERROR processing {file_name}: {traceback.format_exc()}"
            session.create_dataframe(
                [(file_name, err)],
                schema=["file_name", "extracted_text"]
            ).with_column("processed_at", current_timestamp()) \
             .write.mode("append").save_as_table("PDF_TEXT_TEMP")

    # 3. If staging table has rows → truncate + reload real table
    count = session.sql("SELECT COUNT(*) FROM PDF_TEXT_TEMP").collect()[0][0]

    if count > 0:
        session.sql("TRUNCATE TABLE PDF_TEXT_OUTPUT").collect()
        session.sql("""
            INSERT INTO PDF_TEXT_OUTPUT
            SELECT file_name, extracted_text, processed_at
            FROM PDF_TEXT_TEMP
        """).collect()

    return f"Processed {count} PDF files from stage {pdf_stage_name}."
$$;

-- CREATE OR REPLACE PROCEDURE PROCESS_PDFS()
-- RETURNS STRING
-- LANGUAGE PYTHON
-- RUNTIME_VERSION = '3.10'
-- PACKAGES = ('snowflake-snowpark-python', 'PyPDF2')
-- HANDLER = 'run'
-- AS
-- $$
-- from snowflake.snowpark import Session
-- from snowflake.snowpark.functions import call_udf, current_timestamp, col
-- import traceback

-- def run(session: Session):

--     stage_name = '@UNSTRUCTURED_DATA.PDF.PDF_STAGE'

--     # 1. Create a transient staging table (TEMP TABLE not allowed)
--     session.sql("""
--         CREATE OR REPLACE TRANSIENT TABLE PDF_TEXT_TEMP (
--             file_name STRING,
--             extracted_text STRING,
--             processed_at TIMESTAMP
--         )
--     """).collect()

--     # List files in stage
--     files = session.sql(f"LIST {stage_name}").collect()

--     for f in files:
--         full_name = f['name']
--         file_name = full_name.split('/')[-1]

--         # 2. Only process .pdf files
--         if not file_name.lower().endswith(".pdf"):
--             continue

--         try:
--             # Read file bytes
--             with session.file.get_stream(f"{stage_name}/{file_name}") as stream:
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

--         except Exception:
--             err = f"ERROR processing {file_name}: {traceback.format_exc()}"
--             session.create_dataframe(
--                 [(file_name, err)],
--                 schema=["file_name", "extracted_text"]
--             ).with_column("processed_at", current_timestamp()) \
--              .write.mode("append").save_as_table("PDF_TEXT_TEMP")

--     # 3. If staging table has rows → truncate + reload real table
--     count = session.sql("SELECT COUNT(*) FROM PDF_TEXT_TEMP").collect()[0][0]

--     if count > 0:
--         session.sql("TRUNCATE TABLE PDF_TEXT_OUTPUT").collect()
--         session.sql("""
--             INSERT INTO PDF_TEXT_OUTPUT
--             SELECT file_name, extracted_text, processed_at
--             FROM PDF_TEXT_TEMP
--         """).collect()

--     return f"Processed {count} PDF files."
-- $$;



-- CREATE OR REPLACE PROCEDURE PROCESS_PDFS()
-- RETURNS STRING
-- LANGUAGE PYTHON
-- RUNTIME_VERSION = '3.10'
-- PACKAGES = ('snowflake-snowpark-python', 'PyPDF2')
-- HANDLER = 'run'
-- AS
-- $$
-- from snowflake.snowpark import Session
-- from snowflake.snowpark.functions import call_udf, current_timestamp
-- import traceback

-- def run(session: Session):

--     stage_name = '@UNSTRUCTURED_DATA.PDF.PDF_STAGE'

--     # List all files in the stage
--     files = session.sql(f"LIST {stage_name}").collect()

--     results = []

--     for f in files:
--         # LIST returns paths like "pdf_stage/filename.pdf"
--         # Extract only the filename
--         full_name = f['name']
--         file_name = full_name.split('/')[-1]

--         try:
--             # Read file bytes
--             with session.file.get_stream(f"{stage_name}/{file_name}") as stream:
--                 pdf_bytes = stream.read()

--             # Call your existing UDF
--             df = session.create_dataframe([(pdf_bytes,)], schema=["pdf_bytes"])
--             extracted = df.select(
--                 call_udf("extract_pdf_from_bytes", df["pdf_bytes"])
--             ).collect()[0][0]

--             results.append((file_name, extracted))

--         except Exception:
--             err = f"ERROR processing {file_name}: {traceback.format_exc()}"
--             results.append((file_name, err))

--     # Write results to table (3 columns)
--     if results:
--         out_df = session.create_dataframe(
--             results,
--             schema=["file_name", "extracted_text"]
--         ).with_column("processed_at", current_timestamp())

--         out_df.write.mode("append").save_as_table("PDF_TEXT_OUTPUT")

--     return f"Processed {len(results)} files."
-- $$;
