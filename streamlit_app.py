"""
================================================================================
  Data Loader & Explorer - Streamlit in Snowflake App
================================================================================

A comprehensive data management application for Snowflake.

Features:
- Multi-format file upload (CSV, TSV, JSON, Parquet, Avro, ORC)
- Instant table creation with auto-detected schemas
- Visual query builder for data exploration
- SQL generation and execution
- Data export in CSV, JSON, TSV formats

Usage:
1. Open Snowsight and navigate to Streamlit
2. Create a new Streamlit App
3. Paste this code and run

================================================================================
"""

import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.types import *
import pandas as pd
import io
from datetime import datetime

# =========================================================
# Page Configuration
# =========================================================
st.set_page_config(
    page_title="Data Loader & Explorer",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Get Snowflake Session
@st.cache_resource
def get_snowflake_session():
    return get_active_session()

session = get_snowflake_session()

# =========================================================
# Session State Initialization
# =========================================================
if 'loaded_table' not in st.session_state:
    st.session_state.loaded_table = None
if 'query_history' not in st.session_state:
    st.session_state.query_history = []
if 'current_mode' not in st.session_state:
    st.session_state.current_mode = 'upload'

# =========================================================
# Utility Functions
# =========================================================
@st.cache_data(ttl=300)
def get_databases():
    """Get list of databases"""
    result = session.sql("SHOW DATABASES").collect()
    return [row['name'] for row in result]

@st.cache_data(ttl=300)
def get_schemas(database):
    """Get list of schemas in a database"""
    result = session.sql(f"SHOW SCHEMAS IN DATABASE {database}").collect()
    return [row['name'] for row in result]

@st.cache_data(ttl=60)
def get_tables(database, schema):
    """Get list of tables (excluding temporary tables)"""
    result = session.sql(f"SHOW TABLES IN {database}.{schema}").collect()
    excluded_prefixes = ('SNOWPARK_TEMP', 'TEMP_', '_TEMP', 'TMP_')
    return [row['name'] for row in result 
            if not row['name'].upper().startswith(excluded_prefixes)]

def get_table_columns(database, schema, table):
    """Get column information for a table"""
    result = session.sql(f"DESCRIBE TABLE {database}.{schema}.{table}").collect()
    return [(row['name'], row['type']) for row in result]

def add_query_history(query: str, status: str, rows: int = 0):
    """Add query to history"""
    record = {
        'timestamp': datetime.now(),
        'query': query[:100] + '...' if len(query) > 100 else query,
        'status': status,
        'rows': rows
    }
    st.session_state.query_history.insert(0, record)
    st.session_state.query_history = st.session_state.query_history[:10]

def detect_file_type(uploaded_file):
    """Detect file type from extension"""
    filename = uploaded_file.name.lower()
    if filename.endswith('.csv'):
        return 'CSV'
    elif filename.endswith('.tsv'):
        return 'TSV'
    elif filename.endswith('.json') or filename.endswith('.jsonl'):
        return 'JSON'
    elif filename.endswith('.parquet'):
        return 'PARQUET'
    elif filename.endswith('.avro'):
        return 'AVRO'
    elif filename.endswith('.orc'):
        return 'ORC'
    else:
        return 'UNKNOWN'

# =========================================================
# Sidebar
# =========================================================
def render_sidebar():
    st.sidebar.header("🧭 Menu")
    
    # Mode Selection
    st.sidebar.markdown("### 📋 Features")
    
    if st.sidebar.button("📤 Data Upload", use_container_width=True, 
                         type="primary" if st.session_state.current_mode == 'upload' else "secondary"):
        st.session_state.current_mode = 'upload'
        st.rerun()
    
    if st.sidebar.button("🔍 Data Explorer", use_container_width=True,
                         type="primary" if st.session_state.current_mode == 'explore' else "secondary"):
        st.session_state.current_mode = 'explore'
        st.rerun()
    
    st.sidebar.markdown("---")
    
    # Database & Schema Selection
    st.sidebar.markdown("### ⚙️ Connection")
    
    databases = get_databases()
    selected_db = st.sidebar.selectbox(
        "📁 Database",
        databases,
        index=0,
        key="sidebar_db"
    )
    
    schemas = get_schemas(selected_db)
    selected_schema = st.sidebar.selectbox(
        "📂 Schema",
        schemas,
        index=0,
        key="sidebar_schema"
    )
    
    st.sidebar.info(f"📍 **Current Location**\n\n`{selected_db}.{selected_schema}`")
    
    st.sidebar.markdown("---")
    
    # Supported Formats
    st.sidebar.markdown("### 📁 Supported Formats")
    st.sidebar.markdown("""
    - ✅ CSV / TSV
    - ✅ JSON / JSONL
    - ✅ Parquet
    - ✅ Avro
    - ✅ ORC
    """)
    
    # Recent History
    if st.session_state.query_history:
        st.sidebar.markdown("---")
        st.sidebar.markdown("### 📝 Recent Queries")
        for i, h in enumerate(st.session_state.query_history[:3]):
            icon = "✅" if h['status'] == "Success" else "❌"
            st.sidebar.caption(f"{icon} {h['timestamp'].strftime('%H:%M')} - {h['rows']} rows")
    
    return selected_db, selected_schema

# =========================================================
# Data Upload Page
# =========================================================
def render_upload_page(selected_db, selected_schema):
    st.markdown("""
    <div style="text-align: center; padding: 1rem 0;">
        <h1>📤 Data Loader</h1>
        <p style="font-size: 1.1em; color: #666;">Upload files to create Snowflake tables</p>
    </div>
    """, unsafe_allow_html=True)
    
    st.divider()
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.subheader("1️⃣ Upload File")
        
        uploaded_file = st.file_uploader(
            "Supported: CSV, TSV, JSON, Parquet, Avro, ORC",
            type=['csv', 'tsv', 'json', 'jsonl', 'parquet', 'avro', 'orc'],
            help="Drag and drop or click to browse"
        )
        
        if uploaded_file:
            file_type = detect_file_type(uploaded_file)
            
            st.success(f"📁 **{uploaded_file.name}** ({file_type})")
            
            col_info1, col_info2 = st.columns(2)
            with col_info1:
                st.metric("Size", f"{uploaded_file.size / 1024:.1f} KB")
            with col_info2:
                st.metric("Format", file_type)
            
            with st.expander("📝 Load Options", expanded=True):
                if file_type in ['CSV', 'TSV']:
                    delimiter = st.selectbox(
                        "Delimiter",
                        [",", "\t", ";", "|"],
                        index=0 if file_type == 'CSV' else 1
                    )
                    encoding = st.selectbox(
                        "Encoding",
                        ["utf-8", "shift-jis", "cp932"],
                        index=0
                    )
                    has_header = st.checkbox("Has header row", value=True)
                elif file_type == 'JSON':
                    json_lines = st.checkbox("JSON Lines format (one record per line)", value=True)
                else:
                    st.info(f"📦 {file_type} format will be loaded automatically")
    
    with col2:
        st.subheader("2️⃣ Table Settings")
        
        table_name = st.text_input(
            "Table Name",
            value=uploaded_file.name.rsplit('.', 1)[0].upper().replace(" ", "_").replace("-", "_") if uploaded_file else "MY_TABLE",
            help="Alphanumeric characters and underscores only"
        ).upper().replace(" ", "_")
        
        if_exists = st.radio(
            "If table exists",
            ["Error", "Replace (DROP & CREATE)", "Append (INSERT)"],
            index=0,
            horizontal=True
        )
        
        if table_name:
            full_table_name = f"{selected_db}.{selected_schema}.{table_name}"
            st.info(f"📍 Target: `{full_table_name}`")
    
    st.divider()
    
    if uploaded_file:
        try:
            file_type = detect_file_type(uploaded_file)
            
            if file_type in ['CSV', 'TSV']:
                df = pd.read_csv(
                    uploaded_file,
                    delimiter=delimiter,
                    encoding=encoding,
                    header=0 if has_header else None
                )
            elif file_type == 'JSON':
                if json_lines:
                    df = pd.read_json(uploaded_file, lines=True)
                else:
                    df = pd.read_json(uploaded_file)
            elif file_type == 'PARQUET':
                df = pd.read_parquet(uploaded_file)
            elif file_type == 'AVRO':
                try:
                    import fastavro
                    records = []
                    uploaded_file.seek(0)
                    reader = fastavro.reader(uploaded_file)
                    for record in reader:
                        records.append(record)
                    df = pd.DataFrame(records)
                except ImportError:
                    st.error("❌ Avro format requires fastavro library")
                    return
            elif file_type == 'ORC':
                try:
                    import pyarrow.orc as orc
                    uploaded_file.seek(0)
                    table = orc.read_table(uploaded_file)
                    df = table.to_pandas()
                except ImportError:
                    st.error("❌ ORC format requires pyarrow library")
                    return
            else:
                st.error("❌ Unsupported file format")
                return
            
            st.subheader("3️⃣ Data Preview")
            
            col_stat1, col_stat2, col_stat3, col_stat4 = st.columns(4)
            with col_stat1:
                st.metric("Rows", f"{len(df):,}")
            with col_stat2:
                st.metric("Columns", len(df.columns))
            with col_stat3:
                st.metric("Format", file_type)
            with col_stat4:
                st.metric("Size", f"{uploaded_file.size / 1024:.1f} KB")
            
            st.dataframe(df.head(10), use_container_width=True, height=300)
            
            with st.expander("📋 Column Information"):
                col_df = pd.DataFrame({
                    'Column': df.columns,
                    'Type': df.dtypes.astype(str),
                    'Nulls': df.isnull().sum().values,
                    'Sample': [str(df[col].iloc[0])[:50] if len(df) > 0 else '' for col in df.columns]
                })
                st.dataframe(col_df, use_container_width=True)
            
            st.divider()
            
            st.subheader("4️⃣ Create Table")
            
            if st.button("🚀 Create Table", type="primary", use_container_width=True):
                with st.spinner("Creating table..."):
                    try:
                        full_table_path = f"{selected_db}.{selected_schema}.{table_name}"
                        
                        if if_exists == "Replace (DROP & CREATE)":
                            session.sql(f"DROP TABLE IF EXISTS {full_table_path}").collect()
                        
                        snowpark_df = session.create_dataframe(df)
                        
                        if if_exists == "Append (INSERT)":
                            snowpark_df.write.mode("append").save_as_table(full_table_path)
                        else:
                            snowpark_df.write.mode("errorifexists").save_as_table(full_table_path)
                        
                        st.success(f"✅ Table `{full_table_name}` created successfully!")
                        st.session_state.loaded_table = full_table_name
                        
                        add_query_history(f"CREATE TABLE {full_table_path}", "Success", len(df))
                        
                        st.code(f"SELECT * FROM {full_table_name} LIMIT 10;", language="sql")
                        
                        result_df = session.sql(f"SELECT * FROM {full_table_path} LIMIT 10").to_pandas()
                        st.dataframe(result_df, use_container_width=True)
                        
                        st.snow()
                        
                        if st.button("🔍 Explore this table"):
                            st.session_state.current_mode = 'explore'
                            st.rerun()
                        
                    except Exception as e:
                        add_query_history(f"CREATE TABLE {table_name}", "Failed", 0)
                        if "already exists" in str(e).lower():
                            st.error(f"❌ Table `{table_name}` already exists")
                        else:
                            st.error(f"❌ Error: {str(e)}")
        
        except Exception as e:
            st.error(f"❌ File read error: {str(e)}")
    else:
        st.info("👆 Please upload a file")

# =========================================================
# Data Explorer Page
# =========================================================
def render_explore_page(selected_db, selected_schema):
    st.markdown("""
    <div style="text-align: center; padding: 1rem 0;">
        <h1>🔍 Data Explorer</h1>
        <p style="font-size: 1.1em; color: #666;">Select a table to explore and download data</p>
    </div>
    """, unsafe_allow_html=True)
    
    st.divider()
    
    st.subheader("1️⃣ Select Table")
    
    tables = get_tables(selected_db, selected_schema)
    
    if not tables:
        st.warning("📭 No tables found in this schema. Please upload data first.")
        if st.button("📤 Go to Data Upload"):
            st.session_state.current_mode = 'upload'
            st.rerun()
        return
    
    default_idx = 0
    if st.session_state.loaded_table:
        loaded_table_name = st.session_state.loaded_table.split('.')[-1]
        if loaded_table_name in tables:
            default_idx = tables.index(loaded_table_name)
    
    selected_table = st.selectbox(
        "Select a table to explore",
        tables,
        index=default_idx
    )
    
    if selected_table:
        full_table = f"{selected_db}.{selected_schema}.{selected_table}"
        
        columns = get_table_columns(selected_db, selected_schema, selected_table)
        row_count = session.sql(f"SELECT COUNT(*) FROM {full_table}").collect()[0][0]
        
        col_info1, col_info2 = st.columns(2)
        with col_info1:
            st.metric("Records", f"{row_count:,}")
        with col_info2:
            st.metric("Columns", len(columns))
        
        st.divider()
        
        st.subheader("2️⃣ Select Columns")
        
        col_names = [c[0] for c in columns]
        
        table_key = f"selected_cols_{selected_table}"
        if table_key not in st.session_state:
            st.session_state[table_key] = []
        
        col_select1, col_select2 = st.columns([3, 1])
        with col_select2:
            if st.button("Select All", use_container_width=True):
                st.session_state[table_key] = col_names.copy()
                st.rerun()
            if st.button("Deselect All", use_container_width=True):
                st.session_state[table_key] = []
                st.rerun()
        
        with col_select1:
            selected_columns = st.multiselect(
                "Select columns (empty = all columns)",
                col_names,
                default=st.session_state[table_key],
                help="Multiple selection allowed",
                key=f"multiselect_{selected_table}"
            )
            st.session_state[table_key] = selected_columns
        
        with st.expander("📋 Column Information"):
            col_df = pd.DataFrame(columns, columns=['Column', 'Type'])
            st.dataframe(col_df, use_container_width=True)
        
        st.divider()
        
        st.subheader("3️⃣ Filter Conditions (WHERE)")
        
        where_conditions = []
        
        num_conditions = st.number_input("Number of conditions", min_value=0, max_value=5, value=0)
        
        for i in range(int(num_conditions)):
            st.markdown(f"**Condition {i+1}**")
            cond_col1, cond_col2, cond_col3, cond_col4 = st.columns([2, 1, 2, 1])
            
            with cond_col1:
                cond_column = st.selectbox(
                    "Column",
                    col_names,
                    key=f"cond_col_{i}"
                )
            
            with cond_col2:
                cond_operator = st.selectbox(
                    "Operator",
                    ["=", "!=", ">", ">=", "<", "<=", "LIKE", "IN", "IS NULL", "IS NOT NULL"],
                    key=f"cond_op_{i}"
                )
            
            with cond_col3:
                if cond_operator in ["IS NULL", "IS NOT NULL"]:
                    cond_value = ""
                else:
                    cond_value = st.text_input(
                        "Value",
                        key=f"cond_val_{i}",
                        help="Wrap strings in quotes: 'value'"
                    )
            
            with cond_col4:
                if i < int(num_conditions) - 1:
                    cond_logic = st.selectbox(
                        "Logic",
                        ["AND", "OR"],
                        key=f"cond_logic_{i}"
                    )
                else:
                    cond_logic = ""
            
            if cond_operator in ["IS NULL", "IS NOT NULL"]:
                where_conditions.append({
                    'column': cond_column,
                    'operator': cond_operator,
                    'value': '',
                    'logic': cond_logic
                })
            elif cond_value:
                where_conditions.append({
                    'column': cond_column,
                    'operator': cond_operator,
                    'value': cond_value,
                    'logic': cond_logic
                })
        
        custom_where = st.text_area(
            "Or enter WHERE clause directly (can be combined with above)",
            placeholder="Example: AMOUNT > 10000 AND STATUS = 'ACTIVE'",
            help="Do not include the WHERE keyword"
        )
        
        st.divider()
        
        st.subheader("4️⃣ Additional Options")
        
        opt_col1, opt_col2, opt_col3 = st.columns(3)
        
        with opt_col1:
            order_by = st.selectbox(
                "ORDER BY",
                ["None"] + col_names
            )
            if order_by != "None":
                order_dir = st.radio("Sort order", ["ASC", "DESC"], horizontal=True)
        
        with opt_col2:
            limit = st.number_input(
                "LIMIT",
                min_value=1,
                max_value=100000,
                value=100
            )
        
        with opt_col3:
            distinct = st.checkbox("DISTINCT (remove duplicates)")
        
        st.divider()
        
        st.subheader("5️⃣ Generated SQL")
        
        def quote_column(col_name):
            return f'"{col_name}"'
        
        if selected_columns:
            select_clause = ", ".join([quote_column(c) for c in selected_columns])
        else:
            select_clause = "*"
        
        if distinct:
            select_clause = f"DISTINCT {select_clause}"
        
        where_parts = []
        for cond in where_conditions:
            quoted_col = quote_column(cond['column'])
            if cond['operator'] in ["IS NULL", "IS NOT NULL"]:
                where_parts.append(f"{quoted_col} {cond['operator']}")
            else:
                where_parts.append(f"{quoted_col} {cond['operator']} {cond['value']}")
            if cond['logic']:
                where_parts.append(cond['logic'])
        
        where_clause = " ".join(where_parts)
        if custom_where:
            if where_clause:
                where_clause = f"({where_clause}) AND ({custom_where})"
            else:
                where_clause = custom_where
        
        order_clause = ""
        if order_by != "None":
            order_clause = f"ORDER BY {quote_column(order_by)} {order_dir}"
        
        sql = f"SELECT {select_clause}\nFROM {full_table}"
        if where_clause:
            sql += f"\nWHERE {where_clause}"
        if order_clause:
            sql += f"\n{order_clause}"
        sql += f"\nLIMIT {limit}"
        
        edited_sql = st.text_area(
            "SQL (editable)",
            value=sql,
            height=150
        )
        
        col_btn1, col_btn2 = st.columns(2)
        
        with col_btn1:
            execute = st.button("▶️ Execute SQL", type="primary", use_container_width=True)
        
        with col_btn2:
            copy_sql = st.button("📋 Copy SQL", use_container_width=True)
            if copy_sql:
                st.code(edited_sql, language="sql")
                st.info("👆 Copy the code above")
        
        if execute:
            st.divider()
            st.subheader("6️⃣ Results")
            
            with st.spinner("Executing query..."):
                try:
                    start_time = datetime.now()
                    result_df = session.sql(edited_sql).to_pandas()
                    execution_time = (datetime.now() - start_time).total_seconds()
                    
                    add_query_history(edited_sql, "Success", len(result_df))
                    
                    res_col1, res_col2, res_col3 = st.columns(3)
                    with res_col1:
                        st.metric("Rows", f"{len(result_df):,}")
                    with res_col2:
                        st.metric("Columns", len(result_df.columns))
                    with res_col3:
                        st.metric("Execution Time", f"{execution_time:.2f}s")
                    
                    st.dataframe(result_df, use_container_width=True, height=400)
                    
                    st.markdown("### 📥 Download Data")
                    
                    dl_col1, dl_col2, dl_col3 = st.columns(3)
                    
                    with dl_col1:
                        csv_data = result_df.to_csv(index=False)
                        st.download_button(
                            label="📄 Download CSV",
                            data=csv_data,
                            file_name=f"{selected_table}_export.csv",
                            mime="text/csv",
                            use_container_width=True
                        )
                    
                    with dl_col2:
                        json_data = result_df.to_json(orient='records', force_ascii=False, indent=2)
                        st.download_button(
                            label="📋 Download JSON",
                            data=json_data,
                            file_name=f"{selected_table}_export.json",
                            mime="application/json",
                            use_container_width=True
                        )
                    
                    with dl_col3:
                        tsv_data = result_df.to_csv(index=False, sep='\t')
                        st.download_button(
                            label="📊 Download TSV",
                            data=tsv_data,
                            file_name=f"{selected_table}_export.tsv",
                            mime="text/tab-separated-values",
                            use_container_width=True,
                            help="Tab-separated format - Opens in Excel"
                        )
                    
                    st.success("✅ Query executed successfully")
                    
                except Exception as e:
                    add_query_history(edited_sql, "Failed", 0)
                    st.error(f"❌ Query error: {str(e)}")

# =========================================================
# Main Application
# =========================================================
def main():
    selected_db, selected_schema = render_sidebar()
    
    if st.session_state.current_mode == 'upload':
        render_upload_page(selected_db, selected_schema)
    else:
        render_explore_page(selected_db, selected_schema)
    
    st.markdown("---")
    st.markdown(
        "<div style='text-align: center; color: #666; padding: 0.5rem;'>"
        "📊 Data Loader & Explorer - Streamlit in Snowflake</div>",
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()
