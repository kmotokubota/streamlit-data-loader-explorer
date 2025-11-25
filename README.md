# 📊 Data Loader & Explorer

A comprehensive **Streamlit in Snowflake** application for data ingestion and exploration. Upload files in multiple formats, create tables instantly, and explore data with an intuitive visual query builder.

![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)

## ✨ Features

### 📤 Data Upload
- **Multi-format support**: CSV, TSV, JSON, JSONL, Parquet, Avro, ORC
- **Flexible configuration**: Choose database, schema, and custom table names
- **Auto-detection**: Automatic file type and schema detection
- **Data preview**: Preview data before creating tables
- **Table management**: Replace existing tables or append data

### 🔍 Data Explorer
- **Visual query builder**: Build SQL queries without writing code
- **Column selection**: Select specific columns with Select All / Deselect All buttons
- **WHERE conditions**: Add multiple filter conditions via GUI
- **SQL options**: ORDER BY, LIMIT, DISTINCT support
- **Editable SQL**: Auto-generated SQL can be manually edited
- **Export results**: Download query results in CSV, JSON, or TSV formats

## 🚀 Quick Start

### Prerequisites
- Snowflake account with Streamlit enabled
- Appropriate permissions to create tables in target schema

### Installation

1. **Open Snowsight** and navigate to **Streamlit** section
2. Click **"+ Streamlit App"**
3. Select your target **Database** and **Schema**
4. Copy the contents of `streamlit_app.py` and paste into the editor
5. Click **"Run"**

## 📖 Usage Guide

### Data Upload Mode

1. Select target **Database** and **Schema** from the sidebar
2. Upload your file (drag & drop or click to browse)
3. Configure options (delimiter, encoding for CSV/TSV)
4. Enter a **Table Name**
5. Preview the data and column information
6. Click **"Create Table"**

### Data Explorer Mode

1. Select a **Table** from the dropdown
2. Choose **Columns** to display (or leave empty for all)
3. Add **WHERE conditions** using the visual builder
4. Set **ORDER BY**, **LIMIT**, and **DISTINCT** options
5. Review the auto-generated **SQL**
6. Click **"Execute SQL"** to run the query
7. **Download** results in your preferred format

## 🔧 Supported File Formats

| Format | Extension | Notes |
|--------|-----------|-------|
| CSV | `.csv` | Configurable delimiter and encoding |
| TSV | `.tsv` | Tab-separated values |
| JSON | `.json` | Standard JSON format |
| JSON Lines | `.jsonl` | One JSON object per line |
| Parquet | `.parquet` | Columnar storage format |
| Avro | `.avro` | Schema-embedded format |
| ORC | `.orc` | Optimized Row Columnar |

## 📝 Technical Notes

### How Table Creation Works

1. **Upload**: Data is converted to Parquet format internally
2. **Stage**: Data is uploaded to a temporary Snowflake stage (`@~`)
3. **Create**: `CREATE TABLE AS SELECT` or `COPY INTO` is executed
4. **Cleanup**: Temporary stage files are automatically removed

### Limitations

- `USE DATABASE` / `USE SCHEMA` statements are not supported in Streamlit in Snowflake
- All table references use fully qualified names (`database.schema.table`)
- Excel export is not available (use TSV as an Excel-compatible alternative)
- Some Python libraries (e.g., `openpyxl`) are not available in the Streamlit environment

## 🛠️ Customization

You can customize the default database and schema by modifying the sidebar section:

```python
selected_db = st.sidebar.selectbox(
    "📁 Database",
    databases,
    index=databases.index("YOUR_DATABASE") if "YOUR_DATABASE" in databases else 0,
)
```

## 📄 License

This project is provided as-is for educational and demonstration purposes.

## 🤝 Contributing

Feel free to submit issues and pull requests for improvements.

## 📚 References

- [Streamlit in Snowflake Documentation](https://docs.snowflake.com/en/developer-guide/streamlit/about-streamlit)
- [Snowpark Python API](https://docs.snowflake.com/en/developer-guide/snowpark/python/index)
- [Frosty Friday Challenge - Week 80](https://frostyfriday.org/blog/2024/02/09/week-80-basic/)
