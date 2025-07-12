import streamlit as st
import pandas as pd
import s3fs
from doctr.models import ocr_predictor
from doctr.io import DocumentFile
from io import BytesIO
import json

# ---- SETUP ----

# Load S3 connection
s3 = s3fs.S3FileSystem(
        anon=False,
        key = st.secrets["S3_KEY"], 
        secret = st.secrets["S3_SECRET"] 
        )
# base_path = 's3://ocr-database-s3'
base_path = f's3://{st.secrets["S3_BUCKET_NAME"]}'
df_path = f'{base_path}/doc_df.csv'

# Load metadata CSV
if s3.exists(df_path):
    with s3.open(df_path, 'rb') as f:
        df = pd.read_csv(f)
else:
    st.error("Metadata CSV does not exist yet. Please upload files first.")
    st.stop()

# ---- UI ----

st.title("📝 Document OCR Processor")
st.write("Press the button below to run OCR on pages that have not been processed yet.")

process_button = st.button("🚀 Process Pages")

# ---- Doctr Model ----

model = ocr_predictor(pretrained=True, detect_orientation=True)

# ---- Processing ----

if process_button:
    to_process = df[df['OCR_attempted'] == False]

    if len(to_process) == 0:
        st.success("✅ All pages already processed! Nothing to do.")
    else:
        progress_bar = st.progress(0, text="Starting OCR...")

        for idx, row in to_process.iterrows():
            file_path = row['file_path']

            # Download PDF page from S3
            with s3.open(file_path, 'rb') as f:
                pdf_data = BytesIO(f.read())

            # Run Doctr OCR
            pdf_doc = DocumentFile.from_pdf(pdf_data)
            result = model(pdf_doc)
            json_output = result.export()

            # Extract bag of words
            bag_of_words = []
            for this_page in json_output['pages']:
                for this_block in this_page['blocks']:
                    for this_line in this_block['lines']:
                        words = [w['value'] for w in this_line['words']]
                        bag_of_words.extend(words)

            # Save words as JSON string
            df.at[idx, 'words'] = json.dumps(bag_of_words)

            # Mark as processed
            df.at[idx, 'OCR_attempted'] = True

            # Update progress
            progress = (idx + 1) / len(to_process)
            progress_bar.progress(progress, text=f"Processing {idx + 1}/{len(to_process)}")

        # Save updated CSV back to S3
        with s3.open(df_path, 'wb') as f:
            df.to_csv(f, index=False)

        st.success("🎉 All pages processed and metadata updated!")
