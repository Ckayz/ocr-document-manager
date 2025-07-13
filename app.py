import streamlit as st

# ---- Page setup ----
st.set_page_config(page_title="OCR Document Manager", layout="wide")

# ---- Sidebar ----
st.sidebar.title("Document Manager")
page = st.sidebar.radio("Navigate", ["Upload", "Process", "Search"])

# ---- Upload Page ----
if page == "Upload":
    import pandas as pd
    from datetime import datetime
    from PyPDF2 import PdfWriter, PdfReader
    import s3fs

    # ---- S3 Connection ----
    s3 = s3fs.S3FileSystem(
        anon=False,
        key=st.secrets["S3_KEY"],
        secret=st.secrets["S3_SECRET"]
    )

    base_path = f's3://{st.secrets["S3_BUCKET_NAME"]}'
    save_path = f"{base_path}/Data"
    df_path = f"{base_path}/doc_df.csv"

    if not s3.exists(save_path):
        s3.makedirs(save_path)

    with st.form(key='my_form'):
        st.write("**Upload a PDF file**")
        uploaded_file = st.file_uploader("Choose a file", type=['pdf'])

        option = st.selectbox(
            'What kind of file is this?',
            ('Shipping', 'Experiment Metadata', 'Other')
        )

        notes = st.text_area("Notes", "")

        submit_button = st.form_submit_button(label='Submit')

    if submit_button:
        if uploaded_file is not None:
            input_pdf = PdfReader(uploaded_file)
            num_pages = len(input_pdf.pages)
            st.write(f"Number of pages in the PDF: **{num_pages}**")

            p_bar = st.progress(0, text="Starting upload...")

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

            st.success("Upload completed and metadata updated!")
        else:
            st.warning("Please upload a PDF file.")

# ---- Process Page ----
elif page == "Process":
    import pandas as pd
    import s3fs
    from doctr.models import ocr_predictor
    from doctr.io import DocumentFile
    from io import BytesIO
    import json

    # ---- S3 connection ----
    s3 = s3fs.S3FileSystem(
        anon=False,
        key=st.secrets["S3_KEY"],
        secret=st.secrets["S3_SECRET"]
    )
    base_path = f's3://{st.secrets["S3_BUCKET_NAME"]}'
    df_path = f'{base_path}/doc_df.csv'

    if s3.exists(df_path):
        with s3.open(df_path, 'rb') as f:
            df = pd.read_csv(f)
    else:
        st.error("Metadata CSV does not exist yet. Please upload files first.")
        st.stop()

    st.title("Document OCR Processor")

    # ---- Show pending pages ----
    to_process = df[df['OCR_attempted'] == False]

    if len(to_process) == 0:
        st.success("All pages already processed! Nothing to do.")
    else:
        st.info(f"**{len(to_process)} pages need OCR**")

        with st.expander("Click to preview pending pages"):
            st.dataframe(
                to_process[[
                    'file_name', 'page_number', 'file_type', 'notes', 'upload_time'
                ]].reset_index(drop=True)
            )

        st.write("Press the button below to run OCR on these pages:")

        process_button = st.button("Process Pages")

        if process_button:
            model = ocr_predictor(pretrained=True, detect_orientation=True)

            progress_bar = st.progress(0.0, text="Starting OCR...")

            for count, (idx, row) in enumerate(to_process.iterrows()):
                file_path = row['file_path']

                with s3.open(file_path, 'rb') as f:
                    pdf_data = BytesIO(f.read())

                pdf_doc = DocumentFile.from_pdf(pdf_data)
                result = model(pdf_doc)
                json_output = result.export()

                bag_of_words = []
                for this_page in json_output['pages']:
                    for this_block in this_page['blocks']:
                        for this_line in this_block['lines']:
                            words = [w['value'] for w in this_line['words']]
                            bag_of_words.extend(words)

                df.at[idx, 'words'] = json.dumps(bag_of_words)
                df.at[idx, 'OCR_attempted'] = True

                progress = (count + 1) / len(to_process)
                progress_bar.progress(progress, text=f"Processing {count + 1}/{len(to_process)}")

            with s3.open(df_path, 'wb') as f:
                df.to_csv(f, index=False)

            st.success("All pages processed and metadata updated!")

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

    s3 = s3fs.S3FileSystem(
        anon=False,
        key=st.secrets["S3_KEY"],
        secret=st.secrets["S3_SECRET"]
    )

    base_path = f's3://{st.secrets["S3_BUCKET_NAME"]}'
    data_path = f"{base_path}/Data"
    df_path = f"{base_path}/doc_df.csv"
    processed_path = f"{base_path}/Processed"

    if s3.exists(df_path):
        with s3.open(df_path, 'rb') as f:
            df = pd.read_csv(f)
        st.success("Document database loaded successfully.")

        if s3.exists(processed_path):
            processed_files = [x for x in s3.ls(processed_path) if x.endswith('.json')]
            if processed_files:
                st.info(f"Found {len(processed_files)} processed files. Updating database...")
                p_bar = st.progress(0)

                for i, file in enumerate(processed_files):
                    with s3.open(file, 'rb') as f:
                        entry = pd.read_json(f)

                    entry_file_name = entry['file_name'].values[0]
                    entry_file_path = f"{base_path}/{entry_file_name}"
                    entry_words = list(entry['bag_of_words'])

                    wanted_index = df[df['file_path'] == entry_file_path].index[0]
                    df.at[wanted_index, 'words'] = json.dumps(entry_words)
                    df.at[wanted_index, 'OCR_attempted'] = True

                    s3.rm(file)

                    p_bar.progress((i+1)/len(processed_files), text=f"Processed {i+1}/{len(processed_files)}")

                with s3.open(df_path, 'wb') as f:
                    df.to_csv(f, index=False)

                st.success("Processed files loaded & database updated.")
            else:
                st.info("No new processed files found.")

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

            n = 5
            top_n_indices = np.argsort(search_results)[-n:][::-1]

            result_rows = []
            for idx in top_n_indices:
                word_list = literal_eval(df.iloc[idx]['words'])
                best_matches = fuzz_process.extract(search_term, word_list, limit=5)
                basename = os.path.basename(df.iloc[idx]['file_path'])
                result_rows.append({
                    'file_name': basename,
                    'file_type': df.iloc[idx]['file_type'],
                    'notes': df.iloc[idx]['notes'],
                    'best_matches': str(best_matches)
                })

            if result_rows:
                result_df = pd.DataFrame(result_rows)
                st.dataframe(result_df)
            else:
                st.info("No matches found.")

        with st.form(key='view_form'):
            st.subheader("View PDF")
            filename = st.text_input("Enter filename to view (e.g., file_0.pdf):")
            view_button = st.form_submit_button(label='View')

        if view_button and filename.strip():
            fin_path = f"{data_path}/{filename}"
            if s3.exists(fin_path):
                pdf_content = s3.open(fin_path, 'rb').read()
                pdf_viewer(pdf_content)
                st.success(f"{filename} loaded successfully.")
            else:
                st.error(f"File {filename} not found.")

        with st.form(key='download_form'):
            st.subheader("Download PDF")
            filename_dl = st.text_input("Enter filename to download (e.g., file_0.pdf):")
            dl_button = st.form_submit_button(label='Download')

        if dl_button and filename_dl.strip():
            fin_path = f"{data_path}/{filename_dl}"
            if s3.exists(fin_path):
                pdf_content = s3.open(fin_path, 'rb').read()
                st.download_button(
                    label="Download file",
                    data=pdf_content,
                    file_name=filename_dl,
                    mime='application/pdf'
                )
                st.success(f"{filename_dl} ready for download.")
            else:
                st.error(f"File {filename_dl} not found.")
    else:
        st.warning("No OCR'd documents found yet. Please upload & process files first.")
