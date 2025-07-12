import streamlit as st
import pandas as pd
from datetime import datetime
from PyPDF2 import PdfWriter, PdfReader
import s3fs
from io import StringIO
import os

# ---- S3 Connection ----
s3 = s3fs.S3FileSystem(
    anon=False,
    key=st.secrets["S3_KEY"],
    secret=st.secrets["S3_SECRET"]
)

# ---- S3 Paths (use forward slashes!) ----
base_path = f's3://{st.secrets["S3_BUCKET_NAME"]}'
save_path = f"{base_path}/Data"  # use / instead of os.path.join for S3
df_path = f"{base_path}/doc_df.csv"

# Ensure Data prefix exists (optional, but safe)
if not s3.exists(save_path):
    s3.makedirs(save_path)

# ---- Upload Form ----
with st.form(key='my_form'):
    st.write("📄 **Upload a PDF file**")
    uploaded_file = st.file_uploader("Choose a file", type=['pdf'])

    option = st.selectbox(
        'What kind of file is this?',
        ('Shipping', 'Experiment Metadata', 'Other')
    )

    notes = st.text_area("Notes", "")

    submit_button = st.form_submit_button(label='Submit')

# ---- Process Upload ----
if submit_button:
    if uploaded_file is not None:
        input_pdf = PdfReader(uploaded_file)
        num_pages = len(input_pdf.pages)
        st.write(f"Number of pages in the PDF: **{num_pages}**")

        p_bar = st.progress(0, text="Starting upload...")

        for i, page in enumerate(input_pdf.pages):
            # Update progress
            p_bar.progress((i+1)/num_pages, text=f"Uploading page {i+1}/{num_pages}")

            # Create single-page PDF
            output = PdfWriter()
            output.add_page(page)

            save_page_path = f"{save_path}/{uploaded_file.name.split('.')[0]}_{i}.pdf"

            with s3.open(save_page_path, 'wb') as f:
                output.write(f)

            # Metadata
            time_now = pd.to_datetime(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

            temp_df = pd.DataFrame({
                'file_name': [uploaded_file.name],
                'page_number': [i],
                'file_path': [save_page_path],
                'file_type': [option],
                'notes': [notes],
                'upload_time': [time_now],
                'words': [[]],
                'OCR_attempted': [False],
            })

            # Append or create metadata CSV on S3
            if s3.exists(df_path):
                with s3.open(df_path, 'rb') as f:
                    df = pd.read_csv(f)
                    df = df.drop_duplicates(subset='file_path', keep='first')
                    df = pd.concat([df, temp_df], ignore_index=True)
                with s3.open(df_path, 'wb') as f:
                    df.to_csv(f, index=False)
            else:
                with s3.open(df_path, 'wb') as f:
                    temp_df.to_csv(f, index=False)

        st.success("✅ Upload completed and metadata updated!")
    else:
        st.warning("⚠️ Please upload a PDF file.")
