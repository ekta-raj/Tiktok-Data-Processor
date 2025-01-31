# TikTok Data Preprocessing and Analysis Toolkit

This repository provides a streamlined pipeline for processing and analyzing TikTok Direct Messages (DMs) data. The project consists of two main scripts, enabling data preprocessing, anonymization, and analytical insights.

---

### 1. **TikTok DM Preprocessor (`tiktok_preprocess.py`)**
   - **Purpose**: Extract and anonymize TikTok DM data from ZIP files and save it in a structured Parquet format.
   - **Key Features**:
     - **Anonymization**:
       - Masks sensitive personal information (e.g., names, emails, phone numbers) using Presidio.
       - Removes URLs from message content for cleaner data.
     - **Data Standardization**:
       - Normalizes timestamps to UTC format.
       - Generates consistent unique message and conversation IDs.
     - **Output**: Processes data into a Parquet file for efficient storage and analysis.
   - **Usage**:
     - Extracts "Direct Messages.txt" from the TikTok ZIP archive, processes it, and saves the anonymized data as Parquet.

   **Command**:
   ```bash
   python tiktok_preprocess.py --input <input_zip_path> --output <output_parquet_path>
   ```

---

### 2. **TikTok Data Analyzer (`tiktok-dm-parquet.ipynb`)**
   - **Purpose**: Analyze preprocessed TikTok DM data to extract meaningful insights.
   - **Key Features**:
     - **Sentiment Analysis**:
       - Classifies messages as Positive, Neutral, or Negative using sentiment analysis techniques.
     - **Behavioral Insights**:
       - Visualizes messaging patterns over time, highlighting user activity trends.
       - Explores conversational dynamics and engagement.
     - **Customizable Visualizations**:
       - Generates charts for exploratory and comparative analysis.
   - **Usage**:
     - Reads the Parquet file generated by the preprocessor and provides a notebook interface for analysis.

---

## Workflow

1. **Preprocessing**:
   - Use `tiktok_preprocess.py` to extract, anonymize, and structure TikTok DM data.

   **Example Command**:
   ```bash
   python tiktok_preprocess.py --input TikTokData.zip --output TikTokData.parquet
   ```

2. **Analysis**:
   - Open `tiktok-dm-parquet.ipynb` in a Jupyter Notebook.
   - Load the Parquet file and perform data analysis using built-in functions and visualizations.

---

## Outputs

- **Preprocessor**:
  - A Parquet file containing anonymized and structured DM data with fields:
    - `Message_ID`, `UTC_Timestamp`, `Sender_ID`, `Message`, `Conversation_ID`.
- **Analyzer**:
  - Charts and insights, including:
    - Sentiment distribution.
    - Temporal patterns in user messaging behavior.

---

## Dependencies

- **Core Libraries**:
  - `pandas`, `pyarrow`, `argparse`
- **Anonymization**:
  - `presidio-analyzer`, `presidio-anonymizer`
- **Other**:
  - `matplotlib`, `re`, `datetime`

Install dependencies via pip:
```bash
pip install pandas pyarrow presidio-analyzer presidio-anonymizer matplotlib
```
