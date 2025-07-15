import streamlit as st
import os
os.environ['USE_TORCH'] = '1'
os.environ['TORCH_HOME'] = '/tmp/torch_cache'

# ---- Fallback secrets ----
S3_KEY = os.getenv('S3_KEY') or st.secrets.get("S3_KEY", "")
S3_SECRET = os.getenv('S3_SECRET') or st.secrets.get("S3_SECRET", "")

# ---- Page config ----
st.set_page_config(page_title="OCR Document Manager", layout="wide")

# ---- Sidebar ----
st.sidebar.title("Document Manager")
page = st.sidebar.radio("Navigate", ["Upload", "Process", "Search"])

# ---- Upload Page ----
if page == "Upload":
    import pandas as pd
    from datetime import datetime
    from PyPDF2 import PdfWriter, PdfReader
    from PIL import Image
    import s3fs

    s3 = s3fs.S3FileSystem(anon=False, key=S3_KEY, secret=S3_SECRET)

    base_path = f's3://{st.secrets["S3_BUCKET_NAME"]}'
    save_path = f"{base_path}/Data"
    df_path = f"{base_path}/doc_df.csv"

    if not s3.exists(save_path):
        s3.makedirs(save_path)

    with st.form(key='my_form'):
        st.write("**Upload a PDF, image, or Word document**")
        uploaded_file = st.file_uploader(
            "Choose a file",
            type=['pdf', 'png', 'jpeg', 'jpg', 'doc', 'docx']
        )

        option = st.selectbox(
            'What kind of file is this?',
            ('Shipping', 'Experiment Metadata', 'Other')
        )

        notes = st.text_area("Notes", "")

        submit_button = st.form_submit_button(label='Submit')

    if submit_button:
        if uploaded_file is not None:
            file_ext = uploaded_file.name.split('.')[-1].lower()
            p_bar = st.progress(0, text="Starting upload...")

            if file_ext == 'pdf':
                input_pdf = PdfReader(uploaded_file)
                num_pages = len(input_pdf.pages)
                st.write(f"Number of pages in the PDF: **{num_pages}**")

                for i, page in enumerate(input_pdf.pages):
                    p_bar.progress((i+1)/num_pages, text=f"Uploading page {i+1}/{num_pages}")

                    output = PdfWriter()
                    output.add_page(page)

                    save_page_path = f"{save_path}/{uploaded_file.name.split('.')[0]}_{i}.pdf"

                    with s3.open(save_page_path, 'wb') as f:
                        output.write(f)

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

            elif file_ext in ['png', 'jpg', 'jpeg']:
                p_bar.progress(0.5, text="Processing image...")

                image = Image.open(uploaded_file)
                save_image_path = f"{save_path}/{uploaded_file.name}"

                with s3.open(save_image_path, 'wb') as f:
                    image.save(f, format=image.format)

                time_now = pd.to_datetime(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

                temp_df = pd.DataFrame({
                    'file_name': [uploaded_file.name],
                    'page_number': [0],
                    'file_path': [save_image_path],
                    'file_type': [option],
                    'notes': [notes],
                    'upload_time': [time_now],
                    'words': [[]],
                    'OCR_attempted': [False],
                })

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

                p_bar.progress(1.0, text="Image uploaded!")

            elif file_ext in ['doc', 'docx']:
                p_bar.progress(0.5, text="Uploading Word document...")

                save_doc_path = f"{save_path}/{uploaded_file.name}"

                with s3.open(save_doc_path, 'wb') as f:
                    f.write(uploaded_file.read())

                time_now = pd.to_datetime(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

                temp_df = pd.DataFrame({
                    'file_name': [uploaded_file.name],
                    'page_number': [0],
                    'file_path': [save_doc_path],
                    'file_type': [option],
                    'notes': [notes],
                    'upload_time': [time_now],
                    'words': [[]],
                    'OCR_attempted': [False],
                })

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

                p_bar.progress(1.0, text="Word document uploaded!")

            else:
                st.warning("Unsupported file type. Please upload a PDF, image, or Word document.")
                p_bar.empty()
                st.stop()

            st.success("Upload completed and metadata updated!")

        else:
            st.warning("Please upload a file.")

# ---- Process Page ----
elif page == "Process":
    import pandas as pd
    import s3fs
    from doctr.models import ocr_predictor
    from doctr.io import DocumentFile
    from io import BytesIO
    import json

    from docx import Document as DocxDocument  # New: for Word files!

    s3 = s3fs.S3FileSystem(anon=False, key=S3_KEY, secret=S3_SECRET)
    base_path = f's3://{st.secrets["S3_BUCKET_NAME"]}'
    df_path = f'{base_path}/doc_df.csv'

    if s3.exists(df_path):
        with s3.open(df_path, 'rb') as f:
            df = pd.read_csv(f)
    else:
        st.error("Metadata CSV does not exist yet. Please upload files first.")
        st.stop()

    st.title("Document OCR Processor")

    to_process = df[df['OCR_attempted'] == False]

    if len(to_process) == 0:
        st.success("All pages already processed! Nothing to do.")
    else:
        st.info(f"**{len(to_process)} pages need OCR/text extraction**")

        with st.expander("Click to preview pending pages"):
            st.dataframe(to_process[['file_name', 'page_number', 'file_type', 'notes', 'upload_time']].reset_index(drop=True))

        st.write("Press the button below to run OCR/text extraction:")

        process_button = st.button("Process Pages")

        if process_button:
            model = ocr_predictor(pretrained=True, detect_orientation=True)
            progress_bar = st.progress(0.0, text="Starting processing...")

            for count, (idx, row) in enumerate(to_process.iterrows()):
                file_path = row['file_path']
                ext = file_path.split('.')[-1].lower()

                bag_of_words = []

                try:
                    if ext == 'pdf':
                        with s3.open(file_path, 'rb') as f:
                            pdf_data = BytesIO(f.read())
                        doc = DocumentFile.from_pdf(pdf_data)

                        result = model(doc)
                        json_output = result.export()

                        for this_page in json_output['pages']:
                            for this_block in this_page['blocks']:
                                for this_line in this_block['lines']:
                                    words = [w['value'] for w in this_line['words']]
                                    bag_of_words.extend(words)

                    elif ext in ['png', 'jpg', 'jpeg']:
                        with s3.open(file_path, 'rb') as f:
                            img_data = BytesIO(f.read())
                        doc = DocumentFile.from_images(img_data)

                        result = model(doc)
                        json_output = result.export()

                        for this_page in json_output['pages']:
                            for this_block in this_page['blocks']:
                                for this_line in this_block['lines']:
                                    words = [w['value'] for w in this_line['words']]
                                    bag_of_words.extend(words)

                    elif ext in ['doc', 'docx']:
                        with s3.open(file_path, 'rb') as f:
                            file_content = f.read()
                        docx_doc = DocxDocument(BytesIO(file_content))

                        for para in docx_doc.paragraphs:
                            bag_of_words.extend(para.text.split())

                    else:
                        st.warning(f"Skipping unsupported file type for OCR: {ext}")
                        continue

                    df.at[idx, 'words'] = json.dumps(bag_of_words)
                    df.at[idx, 'OCR_attempted'] = True

                except Exception as e:
                    st.error(f"Error processing file {file_path}: {str(e)}")
                    continue

                progress = (count + 1) / len(to_process)
                progress_bar.progress(progress, text=f"Processing {count + 1}/{len(to_process)}")

            with s3.open(df_path, 'wb') as f:
                df.to_csv(f, index=False)

            st.success("All files processed and metadata updated!")

# ---- Search Page ----
elif page == "Search":
    import os
    import pandas as pd
    from thefuzz import fuzz, process as fuzz_process
    import numpy as np
    from ast import literal_eval
    import json
    import s3fs
    from streamlit_pdf_viewer import pdf_viewer

    s3 = s3fs.S3FileSystem(anon=False, key=S3_KEY, secret=S3_SECRET)

    base_path = f's3://{st.secrets["S3_BUCKET_NAME"]}'
    data_path = f"{base_path}/Data"
    df_path = f"{base_path}/doc_df.csv"

    if s3.exists(df_path):
        with s3.open(df_path, 'rb') as f:
            df = pd.read_csv(f)
        st.success("Document database loaded successfully.")

        # Optional: let user pick top-N docs to show
        n = st.slider("Number of top documents to show:", min_value=1, max_value=20, value=5)

        with st.form(key='search_form'):
            st.subheader("Search for a word or phrase")
            search_term = st.text_input("Enter search term:")
            submit_button = st.form_submit_button(label='Search')

        if submit_button and search_term.strip():
            st.write(f"Searching for: **{search_term}**")
            search_results = []

            for i, row in df.iterrows():
                raw_words = row['words']
                word_list = literal_eval(raw_words) if isinstance(raw_words, str) else raw_words
                words_str = ' '.join(word_list)
                match_value = fuzz.partial_token_set_ratio(words_str, search_term)
                search_results.append(match_value)

            top_n_indices = np.argsort(search_results)[-n:][::-1]

            result_rows = []
            for idx in top_n_indices:
                word_list = literal_eval(df.iloc[idx]['words'])
                best_matches = fuzz_process.extract(search_term, word_list, limit=5)
                best_matches_str = ', '.join([f"{match[0]} ({match[1]})" for match in best_matches])

                basename = os.path.basename(df.iloc[idx]['file_path'])
                result_rows.append({
                    'file_name': basename,
                    'file_type': df.iloc[idx]['file_type'],
                    'notes': df.iloc[idx]['notes'],
                    'best_matches': best_matches_str
                })

            if result_rows:
                result_df = pd.DataFrame(result_rows)
                st.dataframe(result_df)
            else:
                st.info("No matches found.")

        with st.form(key='view_form'):
            st.subheader("View file")
            filename = st.text_input("Enter filename to view (e.g., file.pdf):")
            view_button = st.form_submit_button(label='View')

        if view_button and filename.strip():
            fin_path = f"{data_path}/{filename}"
            if s3.exists(fin_path):
                ext = filename.split('.')[-1].lower()
                file_content = s3.open(fin_path, 'rb').read()
                if ext == 'pdf':
                    pdf_viewer(file_content)
                else:
                    st.download_button("Download", file_content, filename)
                st.success(f"{filename} ready.")
            else:
                st.error(f"File {filename} not found.")

        with st.form(key='download_form'):
            st.subheader("Download file")
            filename_dl = st.text_input("Enter filename to download (e.g., file.pdf):")
            dl_button = st.form_submit_button(label='Download')

        if dl_button and filename_dl.strip():
            fin_path = f"{data_path}/{filename_dl}"
            if s3.exists(fin_path):
                file_content = s3.open(fin_path, 'rb').read()
                st.download_button(
                    label="Download file",
                    data=file_content,
                    file_name=filename_dl,
                    mime='application/octet-stream'
                )
                st.success(f"{filename_dl} ready for download.")
            else:
                st.error(f"File {filename_dl} not found.")
    else:
        st.warning("No OCR'd documents found yet. Please upload & process files first.")
