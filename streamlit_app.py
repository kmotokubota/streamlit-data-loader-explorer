"""
================================================================================
  Data Loader & Explorer - Streamlit in Snowflake App
  複数フォーマット対応のデータロード＆探索アプリ
================================================================================

【対応フォーマット】
- CSV / TSV（カンマ・タブ区切り）
- JSON
- Parquet
- Avro
- ORC

【機能】
- ファイルアップロード → テーブル作成
- カラム選択 & WHERE条件指定
- SQL生成 & 実行
- 結果表示 & ダウンロード

================================================================================
"""

import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.types import *
import pandas as pd
import io
import json
from datetime import datetime

# =========================================================
# ページ設定
# =========================================================
st.set_page_config(
    page_title="Data Loader & Explorer",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Snowflakeセッション取得
@st.cache_resource
def get_snowflake_session():
    return get_active_session()

session = get_snowflake_session()

# =========================================================
# セッション状態の初期化
# =========================================================
if 'loaded_table' not in st.session_state:
    st.session_state.loaded_table = None
if 'query_history' not in st.session_state:
    st.session_state.query_history = []
if 'current_mode' not in st.session_state:
    st.session_state.current_mode = 'upload'

# =========================================================
# 共通関数
# =========================================================
@st.cache_data(ttl=300)
def get_databases():
    """データベース一覧を取得"""
    result = session.sql("SHOW DATABASES").collect()
    return [row['name'] for row in result]

@st.cache_data(ttl=300)
def get_schemas(database):
    """スキーマ一覧を取得"""
    result = session.sql(f"SHOW SCHEMAS IN DATABASE {database}").collect()
    return [row['name'] for row in result]

@st.cache_data(ttl=60)
def get_tables(database, schema):
    """テーブル一覧を取得（一時テーブルを除外）"""
    result = session.sql(f"SHOW TABLES IN {database}.{schema}").collect()
    # SNOWPARK_TEMP や一時テーブルを除外
    excluded_prefixes = ('SNOWPARK_TEMP', 'TEMP_', '_TEMP', 'TMP_')
    return [row['name'] for row in result 
            if not row['name'].upper().startswith(excluded_prefixes)]

def get_table_columns(database, schema, table):
    """テーブルのカラム情報を取得"""
    result = session.sql(f"DESCRIBE TABLE {database}.{schema}.{table}").collect()
    return [(row['name'], row['type']) for row in result]

@st.cache_data(ttl=600, show_spinner=False)
def get_table_descriptions_with_ai(database: str, schema: str, table: str):
    """AI_GENERATE_TABLE_DESCを使ってテーブル・カラム説明を生成（10分キャッシュ）"""
    import re
    
    full_table_name = f"{database}.{schema}.{table}"
    
    # AI_GENERATE_TABLE_DESCを試行
    try:
        # AI_GENERATE_TABLE_DESCのクエリ（日本語で回答を要求）
        ai_query = f"""
        SELECT SNOWFLAKE.CORTEX.AI_GENERATE_TABLE_DESC(
            TABLE_NAME => '{full_table_name}',
            LANGUAGE => 'ja'
        )
        """
        ai_result = session.sql(ai_query).collect()
        
        if ai_result and ai_result[0][0]:
            ai_data = json.loads(ai_result[0][0])
            return {
                'table_description': ai_data.get('table_description', ''),
                'column_descriptions': ai_data.get('column_descriptions', {})
            }
    except Exception:
        pass
    
    # AI_GENERATE_TABLE_DESCが使えない場合、CORTEX.COMPLETEで代替実装
    try:
        # テーブル構造を取得
        describe_result = session.sql(f"DESCRIBE TABLE {full_table_name}").collect()
        
        if not describe_result:
            return None
        
        # カラム情報をまとめる
        columns_info = []
        column_names = []
        for row in describe_result:
            columns_info.append(f"{row['name']} ({row['type']})")
            column_names.append(row['name'])
        
        columns_text = "、".join(columns_info)
        column_names_json = json.dumps(column_names, ensure_ascii=False)
        
        # AI説明生成プロンプト
        prompt = f"""あなたはデータベースの専門家です。以下のテーブル情報を分析し、日本語で説明を生成してください。

テーブル名: {table}
カラム構成: {columns_text}

以下のJSON形式で回答してください。column_descriptionsには全てのカラムの説明を含めてください:
{{"table_description": "テーブルの用途を1-2文で説明", "column_descriptions": {{"カラム名1": "そのカラムの意味や用途", "カラム名2": "そのカラムの意味や用途"}}}}

カラム名一覧: {column_names_json}"""
        
        # プロンプトをエスケープ
        escaped_prompt = prompt.replace("'", "''")
        cortex_query = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-3-5-sonnet', '{escaped_prompt}')"
        cortex_result = session.sql(cortex_query).collect()
        
        if cortex_result and cortex_result[0][0]:
            response_text = cortex_result[0][0]
            # JSON部分を抽出
            json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
            
            if json_match:
                json_text = json_match.group(0)
                ai_data = json.loads(json_text)
                
                return {
                    'table_description': ai_data.get('table_description', ''),
                    'column_descriptions': ai_data.get('column_descriptions', {})
                }
    except Exception as e:
        # エラー時はNoneを返す（UIでハンドリング）
        pass
    
    return None

@st.cache_data(ttl=600, show_spinner=False)
def get_table_columns_with_descriptions(database: str, schema: str, table: str):
    """テーブルのカラム名、データ型、AI生成説明を取得（10分キャッシュ）"""
    try:
        full_table_name = f"{database}.{schema}.{table}"
        result = session.sql(f"DESCRIBE TABLE {full_table_name}").collect()
        columns_with_desc = []
        
        # AI説明を取得
        ai_descriptions = get_table_descriptions_with_ai(database, schema, table)
        
        for row in result:
            col_name = row['name']
            col_type = row['type']
            
            # サンプル値を取得
            sample_text = ""
            try:
                sample_query = f'SELECT DISTINCT "{col_name}" FROM {full_table_name} WHERE "{col_name}" IS NOT NULL LIMIT 3'
                sample_result = session.sql(sample_query).collect()
                
                if sample_result:
                    sample_values = [str(r[0])[:30] for r in sample_result]  # 各値は30文字まで
                    sample_text = "、".join(sample_values[:3])
                else:
                    sample_text = "（データなし）"
            except Exception:
                sample_text = "（取得エラー）"
            
            # AI説明を取得
            ai_desc = ""
            if ai_descriptions and ai_descriptions.get('column_descriptions', {}).get(col_name):
                ai_desc = ai_descriptions['column_descriptions'][col_name]
            
            columns_with_desc.append({
                'name': col_name,
                'type': col_type,
                'ai_description': ai_desc,
                'sample_values': sample_text
            })
        
        table_description = ai_descriptions.get('table_description', '') if ai_descriptions else ''
        return columns_with_desc, table_description
        
    except Exception as e:
        return [], ""

def add_query_history(query: str, status: str, rows: int = 0):
    """クエリ履歴に追加"""
    record = {
        'timestamp': datetime.now(),
        'query': query[:100] + '...' if len(query) > 100 else query,
        'status': status,
        'rows': rows
    }
    st.session_state.query_history.insert(0, record)
    st.session_state.query_history = st.session_state.query_history[:10]

def detect_file_type(uploaded_file):
    """ファイルタイプを検出"""
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
# サイドバー
# =========================================================
def render_sidebar():
    st.sidebar.header("🧭 メニュー")
    
    # モード選択
    st.sidebar.markdown("### 📋 機能選択")
    
    if st.sidebar.button("📤 データアップロード", use_container_width=True, 
                         type="primary" if st.session_state.current_mode == 'upload' else "secondary"):
        st.session_state.current_mode = 'upload'
        st.rerun()
    
    if st.sidebar.button("🔍 データ探索", use_container_width=True,
                         type="primary" if st.session_state.current_mode == 'explore' else "secondary"):
        st.session_state.current_mode = 'explore'
        st.rerun()
    
    st.sidebar.markdown("---")
    
    # データベース・スキーマ選択
    st.sidebar.markdown("### ⚙️ 接続設定")
    
    databases = get_databases()
    selected_db = st.sidebar.selectbox(
        "📁 データベース",
        databases,
        index=databases.index("FROSTYFRIDAY") if "FROSTYFRIDAY" in databases else 0,
        key="sidebar_db"
    )
    
    schemas = get_schemas(selected_db)
    selected_schema = st.sidebar.selectbox(
        "📂 スキーマ",
        schemas,
        index=schemas.index("WEEK80") if "WEEK80" in schemas else 0,
        key="sidebar_schema"
    )
    
    st.sidebar.info(f"📍 **現在の場所**\n\n`{selected_db}.{selected_schema}`")
    
    st.sidebar.markdown("---")
    
    # 対応フォーマット
    st.sidebar.markdown("### 📁 対応フォーマット")
    st.sidebar.markdown("""
    - ✅ CSV / TSV
    - ✅ JSON / JSONL
    - ✅ Parquet
    - ✅ Avro
    - ✅ ORC
    """)
    
    # 最近の履歴
    if st.session_state.query_history:
        st.sidebar.markdown("---")
        st.sidebar.markdown("### 📝 最近の実行")
        for i, h in enumerate(st.session_state.query_history[:3]):
            icon = "✅" if h['status'] == "成功" else "❌"
            st.sidebar.caption(f"{icon} {h['timestamp'].strftime('%H:%M')} - {h['rows']}行")
    
    return selected_db, selected_schema

# =========================================================
# Part 1: データアップロード機能
# =========================================================
def render_upload_page(selected_db, selected_schema):
    st.markdown("""
    <div style="text-align: center; padding: 1rem 0;">
        <h1>📤 Data Loader</h1>
        <p style="font-size: 1.1em; color: #666;">ファイルをアップロードしてSnowflakeテーブルを作成</p>
    </div>
    """, unsafe_allow_html=True)
    
    st.divider()
    
    # 2カラムレイアウト
    col1, col2 = st.columns([1, 1])
    
    # デフォルト値を事前に定義（変数スコープ問題を回避）
    delimiter = ","
    encoding = "utf-8"
    has_header = True
    json_lines = False
    
    with col1:
        st.subheader("1️⃣ ファイルをアップロード")
        
        uploaded_file = st.file_uploader(
            "対応形式: CSV, TSV, JSON, Parquet, Avro, ORC",
            type=['csv', 'tsv', 'json', 'jsonl', 'parquet', 'avro', 'orc'],
            help="ファイルをドラッグ&ドロップまたはクリックして選択"
        )
        
        file_type = None
        if uploaded_file is not None:
            try:
                file_type = detect_file_type(uploaded_file)
                
                # ファイル情報表示
                st.success(f"📁 **{uploaded_file.name}** ({file_type})")
                
                col_info1, col_info2 = st.columns(2)
                with col_info1:
                    st.metric("サイズ", f"{uploaded_file.size / 1024:.1f} KB")
                with col_info2:
                    st.metric("形式", file_type)
                
                # フォーマット別オプション
                with st.expander("📝 読み込みオプション", expanded=True):
                    if file_type in ['CSV', 'TSV']:
                        delimiter = st.selectbox(
                            "区切り文字",
                            [",", "\t", ";", "|"],
                            index=0 if file_type == 'CSV' else 1
                        )
                        encoding = st.selectbox(
                            "エンコーディング",
                            ["utf-8", "shift-jis", "cp932"],
                            index=0
                        )
                        has_header = st.checkbox("ヘッダー行あり", value=True)
                    elif file_type == 'JSON':
                        json_lines = st.checkbox(
                            "JSON Lines形式（1行1レコード）", 
                            value=False,
                            help="通常のJSON配列 [{},...] の場合はチェック不要。自動検出されます。"
                        )
                    else:
                        st.info(f"📦 {file_type}形式は自動的に読み込まれます")
            except Exception as upload_err:
                st.error(f"❌ ファイル情報の取得に失敗しました: {str(upload_err)}")
    
    with col2:
        st.subheader("2️⃣ テーブル設定")
        
        # デフォルトのテーブル名を安全に生成
        default_table_name = "MY_TABLE"
        if uploaded_file is not None:
            try:
                default_table_name = uploaded_file.name.rsplit('.', 1)[0].upper().replace(" ", "_").replace("-", "_")
            except:
                default_table_name = "MY_TABLE"
        
        table_name = st.text_input(
            "テーブル名",
            value=default_table_name,
            help="英数字とアンダースコアのみ使用可"
        ).upper().replace(" ", "_")
        
        if_exists = st.radio(
            "同名テーブルが存在する場合",
            ["エラー", "置換（DROP & CREATE）", "追記（INSERT）"],
            index=0,
            horizontal=True
        )
        
        if table_name:
            full_table_name = f"{selected_db}.{selected_schema}.{table_name}"
            st.info(f"📍 作成先: `{full_table_name}`")
    
    # プレビューと作成
    st.divider()
    
    if uploaded_file is not None and file_type is not None:
        try:
            # ファイルポインタをリセット
            uploaded_file.seek(0)
            
            if file_type in ['CSV', 'TSV']:
                df = pd.read_csv(
                    uploaded_file,
                    delimiter=delimiter,
                    encoding=encoding,
                    header=0 if has_header else None
                )
            elif file_type == 'JSON':
                # ファイルポインタをリセットして内容を読み込み
                uploaded_file.seek(0)
                try:
                    content = uploaded_file.read().decode('utf-8')
                except:
                    uploaded_file.seek(0)
                    content = uploaded_file.read()
                    if isinstance(content, bytes):
                        content = content.decode('utf-8')
                
                # JSON形式を自動検出して読み込み
                content_stripped = content.strip()
                
                try:
                    if content_stripped.startswith('['):
                        # JSON配列形式
                        data = json.loads(content)
                        df = pd.DataFrame(data)
                    elif content_stripped.startswith('{'):
                        # 単一オブジェクトまたはJSON Lines
                        try:
                            # まず単一オブジェクトとして試す
                            data = json.loads(content)
                            df = pd.DataFrame([data])
                        except:
                            # JSON Linesとして試す
                            lines = [json.loads(line) for line in content_stripped.split('\n') if line.strip()]
                            df = pd.DataFrame(lines)
                    else:
                        # JSON Linesとして試す
                        lines = [json.loads(line) for line in content_stripped.split('\n') if line.strip()]
                        df = pd.DataFrame(lines)
                except Exception as json_err:
                    st.error(f"❌ JSON読み込みエラー: {str(json_err)}")
                    return
            elif file_type == 'PARQUET':
                df = pd.read_parquet(uploaded_file)
            elif file_type == 'AVRO':
                # Avroはfastavroが必要
                try:
                    import fastavro
                    records = []
                    uploaded_file.seek(0)
                    reader = fastavro.reader(uploaded_file)
                    for record in reader:
                        records.append(record)
                    df = pd.DataFrame(records)
                except ImportError:
                    st.error("❌ Avro形式の読み込みにはfastavroライブラリが必要です")
                    return
            elif file_type == 'ORC':
                # ORCはpyarrowが必要
                try:
                    import pyarrow.orc as orc
                    uploaded_file.seek(0)
                    table = orc.read_table(uploaded_file)
                    df = table.to_pandas()
                except ImportError:
                    st.error("❌ ORC形式の読み込みにはpyarrowライブラリが必要です")
                    return
            else:
                st.error("❌ 未対応のファイル形式です")
                return
            
            st.subheader("3️⃣ データプレビュー")
            
            # 統計情報
            col_stat1, col_stat2, col_stat3, col_stat4 = st.columns(4)
            with col_stat1:
                st.metric("行数", f"{len(df):,}")
            with col_stat2:
                st.metric("列数", len(df.columns))
            with col_stat3:
                st.metric("ファイル形式", file_type)
            with col_stat4:
                st.metric("サイズ", f"{uploaded_file.size / 1024:.1f} KB")
            
            # データプレビュー
            st.dataframe(df.head(10), use_container_width=True, height=300)
            
            # カラム情報
            with st.expander("📋 カラム情報"):
                col_df = pd.DataFrame({
                    'カラム名': df.columns,
                    'データ型': df.dtypes.astype(str),
                    'Null数': df.isnull().sum().values,
                    'サンプル値': [str(df[col].iloc[0])[:50] if len(df) > 0 else '' for col in df.columns]
                })
                st.dataframe(col_df, use_container_width=True)
            
            st.divider()
            
            # 作成ボタン
            st.subheader("4️⃣ テーブル作成")
            
            if st.button("🚀 テーブルを作成", type="primary", use_container_width=True):
                with st.spinner("テーブルを作成中..."):
                    try:
                        # 完全修飾名を使用（USE DATABASE/SCHEMAはStreamlitでは使用不可）
                        full_table_path = f"{selected_db}.{selected_schema}.{table_name}"
                        
                        if if_exists == "置換（DROP & CREATE）":
                            session.sql(f"DROP TABLE IF EXISTS {full_table_path}").collect()
                        
                        snowpark_df = session.create_dataframe(df)
                        
                        if if_exists == "追記（INSERT）":
                            snowpark_df.write.mode("append").save_as_table(full_table_path)
                        else:
                            snowpark_df.write.mode("errorifexists").save_as_table(full_table_path)
                        
                        st.success(f"✅ テーブル `{full_table_name}` を作成しました！")
                        st.session_state.loaded_table = full_table_name
                        
                        add_query_history(f"CREATE TABLE {full_table_path}", "成功", len(df))
                        
                        # 作成後のプレビュー
                        st.code(f"SELECT * FROM {full_table_name} LIMIT 10;", language="sql")
                        
                        result_df = session.sql(f"SELECT * FROM {full_table_path} LIMIT 10").to_pandas()
                        st.dataframe(result_df, use_container_width=True)
                        
                        st.snow()
                        
                        # データ探索への誘導
                        if st.button("🔍 このテーブルを探索する"):
                            st.session_state.current_mode = 'explore'
                            st.rerun()
                        
                    except Exception as e:
                        add_query_history(f"CREATE TABLE {table_name}", "失敗", 0)
                        if "already exists" in str(e).lower():
                            st.error(f"❌ テーブル `{table_name}` は既に存在します")
                        else:
                            st.error(f"❌ エラー: {str(e)}")
        
        except Exception as e:
            st.error(f"❌ ファイル読み込みエラー: {str(e)}")
    else:
        st.info("👆 ファイルをアップロードしてください")

# =========================================================
# Part 2: データ探索機能
# =========================================================
def render_explore_page(selected_db, selected_schema):
    st.markdown("""
    <div style="text-align: center; padding: 1rem 0;">
        <h1>🔍 Data Explorer</h1>
        <p style="font-size: 1.1em; color: #666;">テーブルを選択してデータを探索・ダウンロード</p>
    </div>
    """, unsafe_allow_html=True)
    
    st.divider()
    
    # テーブル選択
    st.subheader("1️⃣ テーブル選択")
    
    tables = get_tables(selected_db, selected_schema)
    
    if not tables:
        st.warning("📭 このスキーマにはテーブルがありません。先にデータをアップロードしてください。")
        if st.button("📤 データアップロードへ"):
            st.session_state.current_mode = 'upload'
            st.rerun()
        return
    
    # 最近ロードしたテーブルがあればデフォルト選択
    default_idx = 0
    if st.session_state.loaded_table:
        loaded_table_name = st.session_state.loaded_table.split('.')[-1]
        if loaded_table_name in tables:
            default_idx = tables.index(loaded_table_name)
    
    selected_table = st.selectbox(
        "探索するテーブルを選択",
        tables,
        index=default_idx
    )
    
    if selected_table:
        full_table = f"{selected_db}.{selected_schema}.{selected_table}"
        
        # テーブル情報
        columns = get_table_columns(selected_db, selected_schema, selected_table)
        row_count = session.sql(f"SELECT COUNT(*) FROM {full_table}").collect()[0][0]
        
        col_info1, col_info2 = st.columns(2)
        with col_info1:
            st.metric("レコード数", f"{row_count:,}")
        with col_info2:
            st.metric("カラム数", len(columns))
        
        # AI説明機能のトグル
        use_ai_descriptions = st.toggle(
            "🤖 AI生成テーブル・カラム説明を表示",
            value=True,
            help="AI_GENERATE_TABLE_DESCを使ってテーブル全体の概要とカラム説明を自動生成します"
        )
        
        # AI説明の取得と表示
        if use_ai_descriptions:
            with st.spinner("🤖 AI説明を生成中..."):
                columns_with_desc, table_description = get_table_columns_with_descriptions(
                    selected_db, selected_schema, selected_table
                )
            
            # テーブル概要を表示
            if table_description:
                st.info(f"**📋 テーブル概要**: {table_description}")
            else:
                st.warning("⚠️ テーブル概要を生成できませんでした。Cortex AIの権限を確認してください。")
        
        st.divider()
        
        # カラム選択
        st.subheader("2️⃣ 表示カラムを選択")
        
        col_names = [c[0] for c in columns]
        
        # セッション状態でカラム選択を管理
        table_key = f"selected_cols_{selected_table}"
        if table_key not in st.session_state:
            st.session_state[table_key] = []
        
        col_select1, col_select2 = st.columns([3, 1])
        with col_select2:
            if st.button("全選択", use_container_width=True):
                st.session_state[table_key] = col_names.copy()
                st.rerun()
            if st.button("全解除", use_container_width=True):
                st.session_state[table_key] = []
                st.rerun()
        
        with col_select1:
            selected_columns = st.multiselect(
                "カラムを選択（空の場合は全カラム）",
                col_names,
                default=st.session_state[table_key],
                help="複数選択可能",
                key=f"multiselect_{selected_table}"
            )
            # 選択状態を保存
            st.session_state[table_key] = selected_columns
        
        # カラム情報テーブル（AI説明付き）
        with st.expander("📋 カラム情報", expanded=use_ai_descriptions):
            if use_ai_descriptions and columns_with_desc:
                # AI説明付きのカラム情報を表示
                display_data = []
                for col_info in columns_with_desc:
                    display_data.append({
                        'カラム名': col_info['name'],
                        'データ型': col_info['type'],
                        'AI説明': col_info.get('ai_description', ''),
                        'サンプル値': col_info.get('sample_values', '')
                    })
                
                col_df = pd.DataFrame(display_data)
                
                # カラム設定
                column_config = {
                    "カラム名": st.column_config.TextColumn("カラム名", width="medium"),
                    "データ型": st.column_config.TextColumn("データ型", width="small"),
                    "AI説明": st.column_config.TextColumn("AI説明", width="large", help="AI_GENERATE_TABLE_DESCで生成された説明"),
                    "サンプル値": st.column_config.TextColumn("サンプル値", width="medium", help="実際のデータサンプル")
                }
                
                st.dataframe(col_df, column_config=column_config, use_container_width=True, hide_index=True)
            else:
                # 基本的なカラム情報のみ表示
                col_df = pd.DataFrame(columns, columns=['カラム名', 'データ型'])
                st.dataframe(col_df, use_container_width=True)
        
        st.divider()
        
        # WHERE条件
        st.subheader("3️⃣ 絞り込み条件（WHERE句）")
        
        # 条件入力UI
        where_conditions = []
        
        num_conditions = st.number_input("条件数", min_value=0, max_value=5, value=0)
        
        for i in range(int(num_conditions)):
            st.markdown(f"**条件 {i+1}**")
            cond_col1, cond_col2, cond_col3, cond_col4 = st.columns([2, 1, 2, 1])
            
            with cond_col1:
                cond_column = st.selectbox(
                    "カラム",
                    col_names,
                    key=f"cond_col_{i}"
                )
            
            with cond_col2:
                cond_operator = st.selectbox(
                    "演算子",
                    ["=", "!=", ">", ">=", "<", "<=", "LIKE", "IN", "IS NULL", "IS NOT NULL"],
                    key=f"cond_op_{i}"
                )
            
            with cond_col3:
                if cond_operator in ["IS NULL", "IS NOT NULL"]:
                    cond_value = ""
                else:
                    cond_value = st.text_input(
                        "値",
                        key=f"cond_val_{i}",
                        help="文字列は 'value' のようにクォートで囲んでください"
                    )
            
            with cond_col4:
                if i < int(num_conditions) - 1:
                    cond_logic = st.selectbox(
                        "論理",
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
        
        # カスタムWHERE句
        custom_where = st.text_area(
            "または、WHERE句を直接入力（上記条件と併用可）",
            placeholder="例: AMOUNT > 10000 AND STATUS = 'ACTIVE'",
            help="WHERE キーワードは不要です"
        )
        
        st.divider()
        
        # その他のオプション
        st.subheader("4️⃣ その他のオプション")
        
        opt_col1, opt_col2, opt_col3 = st.columns(3)
        
        with opt_col1:
            order_by = st.selectbox(
                "ORDER BY",
                ["なし"] + col_names
            )
            if order_by != "なし":
                order_dir = st.radio("並び順", ["ASC", "DESC"], horizontal=True)
        
        with opt_col2:
            limit = st.number_input(
                "LIMIT",
                min_value=1,
                max_value=100000,
                value=100
            )
        
        with opt_col3:
            distinct = st.checkbox("DISTINCT（重複排除）")
        
        st.divider()
        
        # SQL生成
        st.subheader("5️⃣ 生成されたSQL")
        
        # カラム名をダブルクォートで囲む関数（大文字小文字を保持）
        def quote_column(col_name):
            return f'"{col_name}"'
        
        # SELECT句
        if selected_columns:
            select_clause = ", ".join([quote_column(c) for c in selected_columns])
        else:
            select_clause = "*"
        
        if distinct:
            select_clause = f"DISTINCT {select_clause}"
        
        # WHERE句の構築
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
        
        # ORDER BY句
        order_clause = ""
        if order_by != "なし":
            order_clause = f"ORDER BY {quote_column(order_by)} {order_dir}"
        
        # 完全なSQL
        sql = f"SELECT {select_clause}\nFROM {full_table}"
        if where_clause:
            sql += f"\nWHERE {where_clause}"
        if order_clause:
            sql += f"\n{order_clause}"
        sql += f"\nLIMIT {limit}"
        
        # SQL表示（編集可能）
        edited_sql = st.text_area(
            "SQL（編集可能）",
            value=sql,
            height=150
        )
        
        # 実行ボタン
        col_btn1, col_btn2 = st.columns(2)
        
        with col_btn1:
            execute = st.button("▶️ SQLを実行", type="primary", use_container_width=True)
        
        with col_btn2:
            copy_sql = st.button("📋 SQLをコピー", use_container_width=True)
            if copy_sql:
                st.code(edited_sql, language="sql")
                st.info("👆 上のコードをコピーしてください")
        
        # 実行結果
        if execute:
            st.divider()
            st.subheader("6️⃣ 実行結果")
            
            with st.spinner("クエリ実行中..."):
                try:
                    start_time = datetime.now()
                    result_df = session.sql(edited_sql).to_pandas()
                    execution_time = (datetime.now() - start_time).total_seconds()
                    
                    add_query_history(edited_sql, "成功", len(result_df))
                    
                    # 結果情報
                    res_col1, res_col2, res_col3 = st.columns(3)
                    with res_col1:
                        st.metric("取得行数", f"{len(result_df):,}")
                    with res_col2:
                        st.metric("カラム数", len(result_df.columns))
                    with res_col3:
                        st.metric("実行時間", f"{execution_time:.2f}秒")
                    
                    # 結果表示
                    st.dataframe(result_df, use_container_width=True, height=400)
                    
                    # ダウンロードボタン
                    st.markdown("### 📥 データダウンロード")
                    
                    dl_col1, dl_col2, dl_col3 = st.columns(3)
                    
                    with dl_col1:
                        csv_data = result_df.to_csv(index=False)
                        st.download_button(
                            label="📄 CSVでダウンロード",
                            data=csv_data,
                            file_name=f"{selected_table}_export.csv",
                            mime="text/csv",
                            use_container_width=True
                        )
                    
                    with dl_col2:
                        json_data = result_df.to_json(orient='records', force_ascii=False, indent=2)
                        st.download_button(
                            label="📋 JSONでダウンロード",
                            data=json_data,
                            file_name=f"{selected_table}_export.json",
                            mime="application/json",
                            use_container_width=True
                        )
                    
                    with dl_col3:
                        # TSV（タブ区切り）ダウンロード - Excelで開ける形式
                        tsv_data = result_df.to_csv(index=False, sep='\t')
                        st.download_button(
                            label="📊 TSVでダウンロード",
                            data=tsv_data,
                            file_name=f"{selected_table}_export.tsv",
                            mime="text/tab-separated-values",
                            use_container_width=True,
                            help="タブ区切り形式 - Excelで開けます"
                        )
                    
                    st.success("✅ クエリが正常に実行されました")
                    
                except Exception as e:
                    add_query_history(edited_sql, "失敗", 0)
                    st.error(f"❌ クエリ実行エラー: {str(e)}")

# =========================================================
# メインアプリケーション
# =========================================================
def main():
    # サイドバー
    selected_db, selected_schema = render_sidebar()
    
    # モードに応じたページ表示
    if st.session_state.current_mode == 'upload':
        render_upload_page(selected_db, selected_schema)
    else:
        render_explore_page(selected_db, selected_schema)
    
    # フッター
    st.markdown("---")
    st.markdown(
        "<div style='text-align: center; color: #666; padding: 0.5rem;'>"
        "📊 Data Loader & Explorer - Frosty Friday Week 80 Demo</div>",
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()
