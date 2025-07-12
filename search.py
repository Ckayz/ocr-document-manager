"""
Search through words extracted from OCR to find a specific word or phrase.
"""

import os
import pandas as pd
import streamlit as st
from thefuzz import fuzz, process
import numpy as np
from ast import literal_eval
import json
import s3fs
from streamlit_pdf_viewer import pdf_viewer

# ---- S3 Connection ----
s3 = s3fs.S3FileSystem(
    anon=False,
    key=st.secrets["S3_KEY"],
    secret=st.secrets["S3_SECRET"]
)

# ---- S3 Paths (always use forward slashes!) ----
base_path = f's3://{st.secrets["S3_BUCKET_NAME"]}'
data_path = f"{base_path}/Data"
df_path = f"{base_path}/doc_df.csv"
processed_path = f"{base_path}/Processed"

# ---- Load Document Database ----
if s3.exists(df_path):
    with s3.open(df_path, 'rb') as f:
        df = pd.read_csv(f)
    st.success("✅ Document database loaded successfully.")

    # ---- Check for any new processed JSONs ----
    if s3.exists(processed_path):
        processed_files = [x for x in s3.ls(processed_path) if x.endswith('.json')]
        if processed_files:
            st.info(f"🔄 Found {len(processed_files)} processed files. Updating database...")
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

                # Remove processed file
                s3.rm(file)

                p_bar.progress((i+1)/len(processed_files),
                               text=f"Processed {i+1}/{len(processed_files)}")

            # Save updated CSV back to S3
            with s3.open(df_path, 'wb') as f:
                df.to_csv(f, index=False)

            st.success("✅ Processed files loaded & document database updated.")
        else:
            st.info("📂 No new processed files found.")

    ############################################################
    # 🔍 Search
    ############################################################

    with st.form(key='search_form'):
        st.subheader("🔍 Search for a word or phrase")
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

        # Top N matches
        n = 5
        top_n_indices = np.argsort(search_results)[-n:][::-1]

        result_rows = []
        for idx in top_n_indices:
            word_list = literal_eval(df.iloc[idx]['words'])
            best_matches = process.extract(search_term, word_list, limit=5)
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

    ############################################################
    # 👀 Viewer
    ############################################################

    with st.form(key='view_form'):
        st.subheader("👀 View PDF")
        filename = st.text_input("Enter filename to view (e.g., file_0.pdf):")
        view_button = st.form_submit_button(label='View')

    if view_button and filename.strip():
        fin_path = f"{data_path}/{filename}"
        if s3.exists(fin_path):
            pdf_content = s3.open(fin_path, 'rb').read()
            pdf_viewer(pdf_content)
            st.success(f"✅ {filename} loaded successfully.")
        else:
            st.error(f"❌ File {filename} not found.")

    ############################################################
    # ⬇️ Downloader
    ############################################################

    with st.form(key='download_form'):
        st.subheader("⬇️ Download PDF")
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
            st.success(f"✅ {filename_dl} ready for download.")
        else:
            st.error(f"❌ File {filename_dl} not found.")

else:
    st.warning("⚠️ No OCR'd documents found yet. Please upload & process files first.")
