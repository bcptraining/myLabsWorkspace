/*--------------------------
Set context
--------------------------*/
use database unstructured_data;
use schema pdf;
/*--------------------------
Confirm iternal stage PDF_STAGE was created
--------------------------*/
show stages;

/*--------------------------
Create streamit app to load files into internal stage 
--------------------------*/

import streamlit as st
from snowflake.snowpark.context import get_active_session
import io

session = get_active_session()

st.title("Upload PDFs to @pdf_stage")

uploaded = st.file_uploader("Choose a PDF", type=["pdf"])

if uploaded:
    st.write(f"Uploading: {uploaded.name}")

    # Convert Streamlit UploadedFile → real binary stream
    file_bytes = uploaded.read()
    file_stream = io.BytesIO(file_bytes)

    session.file.put_stream(
        file_stream,
        f"@unstructured_data.pdf.pdf_stage/{uploaded.name}",
        auto_compress=False,
        overwrite=True
    )

    st.success(f"Uploaded {uploaded.name} to @pdf_stage")

    st.subheader("Files currently in the stage:")
    results = session.sql("LIST @unstructured_data.pdf.pdf_stage").collect()
    for r in results:
        st.write(r["name"])


