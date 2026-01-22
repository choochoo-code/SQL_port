from flask import Flask, render_template, request, redirect, flash, jsonify
import pymysql
from config import Config
import os
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import re


app = Flask(__name__)
app.secret_key = "your-secret"

# Database config
DB_CONFIG = Config.DATABASE

# Timeframe mapping (minutes)
TIMEFRAMES = {
    '3min': 3,
    '5min': 5,
    '15min': 15,
    '1hr': 60
}

# ---------------------------
# DB helpers
# ---------------------------
def get_db_connection(schema=None):
    config = DB_CONFIG.copy()
    if schema:
        config['database'] = schema
    return pymysql.connect(**config)

def connect_to_sql(schema):
    config = DB_CONFIG.copy()
    return create_engine(
        f"mysql+pymysql://{config['user']}:{config['password']}@"
        f"{config['host']}:{config.get('port', 3306)}/{schema}"
    )

def get_schemas():
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute("SHOW DATABASES")
        schemas = [
            row[0] for row in cur.fetchall()
            if row[0] not in ('information_schema', 'mysql', 'performance_schema', 'sys')
        ]
    conn.close()
    return schemas

def get_tables(schema, pattern='_1min'):
    conn = get_db_connection(schema)
    with conn.cursor() as cur:
        cur.execute("SHOW TABLES")
        tables = [row[0] for row in cur.fetchall() if pattern in row[0]]
    conn.close()
    return tables

# ---------------------------
# Filename parsing (FIX)
# ---------------------------
def parse_filename(filename: str):
    """
    Expected pattern:
    ib_data_<date>_<symbol>_<call|put>_<timeframe>_<date>.csv
    Example:
    ib_data_01152026_qqq_call_1min_2026-01-23.csv
    """
    pattern = r".*_(?P<symbol>[a-zA-Z]+)_(?P<type>call|put)_(?P<tf>\d+min)_.*\.csv$"
    match = re.match(pattern, filename.lower())
    if not match:
        return None
    return match.groupdict()

# ---------------------------
# Pages
# ---------------------------
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/resample')
def resample():
    schemas = get_schemas()
    return render_template('resample.html', schemas=schemas, timeframes=TIMEFRAMES)

@app.route('/api/tables/<schema>')
def api_get_tables(schema):
    return jsonify(get_tables(schema))

# ---------------------------
# Resampling helpers
# ---------------------------
def create_dest_table(schema, dest_table):
    """Create destination table for resampled data"""
    conn = get_db_connection(schema)
    try:
        with conn.cursor() as cur:
            # Drop if exists
            cur.execute(f"DROP TABLE IF EXISTS `{schema}`.`{dest_table}`")

            # Create table
            create_sql = f"""
            CREATE TABLE `{schema}`.`{dest_table}` (
                StrikePrice INT NOT NULL,
                ContractType VARCHAR(10) NOT NULL,
                ExpiryDate DATETIME NOT NULL,
                Timestamp DATETIME NOT NULL,
                Open  DECIMAL(10,4),
                Close DECIMAL(10,4),
                High  DECIMAL(10,4),
                Low   DECIMAL(10,4),
                Volume BIGINT,
                PRIMARY KEY (StrikePrice, ContractType, ExpiryDate, Timestamp)
            )
            """
            cur.execute(create_sql)
            conn.commit()
        return True
    except Exception as e:
        print(f"Error creating table: {e}")
        return False
    finally:
        conn.close()

def execute_resample(schema, src_table, dest_table, tf_minutes):
    """Execute resampling from 1min to higher timeframe"""
    conn = get_db_connection(schema)
    try:
        with conn.cursor() as cur:
            # Execute the resampling SQL
            resample_sql = f"""
            INSERT INTO `{schema}`.`{dest_table}`
            (StrikePrice, ContractType, ExpiryDate, Timestamp, Open, Close, High, Low, Volume)

            WITH base AS (
                SELECT
                    StrikePrice,
                    ContractType,
                    ExpiryDate,
                    Timestamp,
                    Open,
                    Close,
                    High,
                    Low,
                    Volume,

                    FLOOR(
                      TIMESTAMPDIFF(
                        MINUTE,
                        CONCAT(DATE(Timestamp), ' 09:30:00'),
                        Timestamp
                      ) / {tf_minutes}
                    ) AS bucket_id

                FROM `{schema}`.`{src_table}`
                WHERE TIME(Timestamp) BETWEEN '09:30:00' AND '15:59:00'
                  AND Open IS NOT NULL
                  AND Close IS NOT NULL
                  AND High IS NOT NULL
                  AND Low IS NOT NULL
            ),

            bucketed AS (
                SELECT
                    StrikePrice,
                    ContractType,
                    ExpiryDate,

                    DATE_ADD(
                      CONCAT(DATE(Timestamp), ' 09:30:00'),
                      INTERVAL bucket_id * {tf_minutes} MINUTE
                    ) AS candle_ts,

                    Timestamp,
                    Open,
                    Close,
                    High,
                    Low,
                    Volume
                FROM base
            )

            SELECT
                StrikePrice,
                ContractType,
                ExpiryDate,
                candle_ts AS Timestamp,

                SUBSTRING_INDEX(
                  GROUP_CONCAT(Open ORDER BY Timestamp),
                  ',', 1
                ) AS Open,

                SUBSTRING_INDEX(
                  GROUP_CONCAT(Close ORDER BY Timestamp DESC),
                  ',', 1
                ) AS Close,

                MAX(High) AS High,
                MIN(Low)  AS Low,
                SUM(Volume) AS Volume

            FROM bucketed
            GROUP BY
                StrikePrice,
                ContractType,
                ExpiryDate,
                candle_ts
            ORDER BY
                StrikePrice,
                ContractType,
                ExpiryDate,
                candle_ts
            """
            cur.execute(resample_sql)
            conn.commit()
        return True
    except Exception as e:
        print(f"Error resampling: {e}")
        return False
    finally:
        conn.close()

@app.route('/api/resample', methods=['POST'])
def api_resample():
    """API endpoint to execute resampling"""
    data = request.json
    schema = data.get('schema')
    src_table = data.get('table')
    timeframes = data.get('timeframes', [])

    if not schema or not src_table or not timeframes:
        return jsonify({'success': False, 'error': 'Missing required parameters'}), 400

    results = []

    for tf_name in timeframes:
        if tf_name not in TIMEFRAMES:
            continue

        tf_minutes = TIMEFRAMES[tf_name]

        # Generate destination table name
        dest_table = src_table.replace('_1min', f'_{tf_name}')

        # Create destination table
        if not create_dest_table(schema, dest_table):
            results.append({
                'timeframe': tf_name,
                'success': False,
                'error': 'Failed to create destination table'
            })
            continue

        # Execute resampling
        if execute_resample(schema, src_table, dest_table, tf_minutes):
            results.append({
                'timeframe': tf_name,
                'table': dest_table,
                'success': True
            })
        else:
            results.append({
                'timeframe': tf_name,
                'success': False,
                'error': 'Failed to execute resampling'
            })

    return jsonify({'success': True, 'results': results})

# ---------------------------
# MERGE CSV → SQL (FIXED)
# ---------------------------
@app.route('/merge_data', methods=['GET', 'POST'])
def merge_option_data():
    if request.method == 'POST':
        schema = request.form.get('schema')
        table = request.form.get('table')
        files = request.files.getlist('csv_files')

        if not schema or not table:
            return "Missing schema or table", 400
        if not files:
            return "No files uploaded", 400

        # ---------------------------
        # VALIDATE FILE NAMES (FIXED)
        # ---------------------------
        for file in files:
            parsed = parse_filename(file.filename)
            if not parsed:
                return f"❌ Invalid filename format: {file.filename}", 400

            symbol = parsed['symbol']
            option_type = parsed['type']
            timeframe = parsed['tf']

            # Schema must start with symbol_
            if not schema.lower().startswith(f"{symbol}_"):
                return (
                    f"❌ Filename `{file.filename}` implies symbol `{symbol}`, "
                    f"but selected schema is `{schema}`"
                ), 400

            # Table must contain both option_type and timeframe
            table_lower = table.lower()
            if option_type not in table_lower or timeframe not in table_lower:
                return (
                    f"❌ Filename `{file.filename}` implies option type `{option_type}` "
                    f"and timeframe `{timeframe}`, but selected table is `{table}`"
                ), 400

        merged_options = pd.DataFrame()
        total_rows_from_csv = 0

        for file in files:
            try:
                df = pd.read_csv(file)
                total_rows_from_csv += len(df)
                merged_options = pd.concat([merged_options, df], ignore_index=True)
            except Exception as e:
                return f"Error reading {file.filename}: {e}", 500

        # Drop duplicates by unique option contract + timestamp
        merged_options.drop_duplicates(
            subset=['StrikePrice', 'ContractType', 'ExpiryDate', 'Timestamp'],
            inplace=True
        )

        try:
            engine = connect_to_sql(schema)

            # Get before count with a fresh connection (then close it)
            conn_before = get_db_connection(schema)
            with conn_before.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM `{table}`")
                before_count = cur.fetchone()[0]
            conn_before.close()

            merged_options.to_sql(
                table,
                con=engine,
                schema=schema,
                if_exists='append',
                index=False
            )

            # Get after count with a NEW connection to see committed changes
            conn_after = get_db_connection(schema)
            with conn_after.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM `{table}`")
                after_count = cur.fetchone()[0]
            conn_after.close()

            rows_inserted = after_count - before_count
            duplicates_skipped = len(merged_options) - rows_inserted

        except Exception as e:
            return f"Database error: {e}", 500

        # ---------------------------
        # LOGGING
        # ---------------------------
        log_dir = os.path.join(os.getcwd(), "log")
        os.makedirs(log_dir, exist_ok=True)
        log_path = os.path.join(log_dir, "merge_log.csv")

        log_entry = pd.DataFrame([{
            "datetime": datetime.now(),
            "schema": schema,
            "table": table,
            "rows_from_csv": total_rows_from_csv,
            "rows_merged": rows_inserted,
            "duplicates_skipped": duplicates_skipped,
            "files_uploaded": ', '.join(f.filename for f in files)
        }])

        log_entry.to_csv(
            log_path,
            mode='a',
            index=False,
            header=not os.path.exists(log_path)
        )

        return f"""
        <p style='color:green'>✅ Upload complete</p>
        <p>CSV rows: {total_rows_from_csv}</p>
        <p>Inserted: {rows_inserted}</p>
        <p>Duplicates skipped: {duplicates_skipped}</p>
        <br>
        <a href='/'>🏠 Return Home</a> |
        <a href='/merge_data'>📁 Merge More Files</a>
        """

    schemas = get_schemas()
    return render_template("merge_data.html", schemas=schemas)

# ---------------------------
# ADD ROW (unchanged)
# ---------------------------
@app.route('/add_row', methods=['GET', 'POST'])
def add_row():
    table_name = "your_table_name"
    schema_name = "your_schema_name"

    conn = get_db_connection(schema_name)
    with conn.cursor() as cur:
        cur.execute(f"DESCRIBE `{table_name}`")
        columns = [row[0] for row in cur.fetchall() if row[3] != "PRI"]
    conn.close()

    if request.method == 'POST':
        values = [request.form.get(col) for col in columns]
        placeholders = ','.join(['%s'] * len(columns))
        insert_sql = f"INSERT INTO `{table_name}` ({','.join(columns)}) VALUES ({placeholders})"

        try:
            conn = get_db_connection(schema_name)
            with conn.cursor() as cur:
                cur.execute(insert_sql, values)
                conn.commit()
            conn.close()
            flash('Row inserted successfully!', 'success')
        except Exception as e:
            flash(str(e), 'danger')

        return redirect('/add_row')

    return render_template('add_row.html', columns=columns)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
