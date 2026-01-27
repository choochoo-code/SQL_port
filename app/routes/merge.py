"""
Merge CSV to SQL routes
"""
import os
import re
import pandas as pd
from datetime import datetime
from flask import Blueprint, render_template, request, jsonify

from app.services import (
    get_db_connection,
    get_sqlalchemy_engine,
    get_schemas,
    get_base_tables_status,
    create_base_table,
    BASE_TABLES
)

merge_bp = Blueprint('merge', __name__)


def parse_filename(filename: str):
    """
    Parse CSV filename to extract symbol, type, and timeframe.

    Option pattern: ib_data_<date>_<symbol>_<call|put>_<timeframe>_<date>.csv
    Example: ib_data_01152026_qqq_call_1min_2026-01-23.csv

    Stock pattern: ib_data_<date>_<symbol>_<timeframe>.csv
    Example: ib_data_01202026_qqq_1 min.csv
    """
    # Try option pattern first (with call/put)
    option_pattern = r".*_(?P<symbol>[a-zA-Z]+)_(?P<type>call|put)_(?P<tf>\d+\s*min)_.*\.csv$"
    match = re.match(option_pattern, filename.lower())
    if match:
        result = match.groupdict()
        result['tf'] = result['tf'].replace(' ', '')  # Normalize "1 min" to "1min"
        return result

    # Try stock pattern (no call/put)
    stock_pattern = r".*_(?P<symbol>[a-zA-Z]+)_(?P<tf>\d+\s*min)\.csv$"
    match = re.match(stock_pattern, filename.lower())
    if match:
        result = match.groupdict()
        result['type'] = 'stock'  # Mark as stock type
        result['tf'] = result['tf'].replace(' ', '')  # Normalize "1 min" to "1min"
        return result

    return None


@merge_bp.route('/merge_data', methods=['GET', 'POST'])
def merge_option_data():
    if request.method == 'POST':
        schema = request.form.get('schema')
        table = request.form.get('table')
        files = request.files.getlist('csv_files')

        if not schema or not table:
            return "Missing schema or table", 400
        if not files or all(f.filename == '' for f in files):
            return "No files uploaded", 400

        # Validate table is a known base table
        if table not in BASE_TABLES:
            return f"Invalid table: {table}", 400

        # Create table if it doesn't exist
        create_base_table(schema, table)

        # Validate file names match selected table
        for file in files:
            if file.filename == '':
                continue
            parsed = parse_filename(file.filename)
            if not parsed:
                return f"Invalid filename format: {file.filename}", 400

            symbol = parsed['symbol']
            option_type = parsed['type']
            timeframe = parsed['tf']

            # Schema must start with symbol
            if not schema.lower().startswith(f"{symbol}_"):
                return (
                    f"Filename `{file.filename}` implies symbol `{symbol}`, "
                    f"but selected schema is `{schema}`"
                ), 400

            # Validate file type matches selected table
            if table == 'ib_stock_1min':
                # Stock table: only accept stock files (non-option, non-vix)
                if option_type != 'stock':
                    return (
                        f"Filename `{file.filename}` is an option file (type: {option_type}), "
                        f"but selected table is `{table}` (stock table)"
                    ), 400
                if symbol == 'vix':
                    return (
                        f"Filename `{file.filename}` is a VIX file, "
                        f"use `ib_vix_1min` table instead"
                    ), 400
            elif table == 'ib_vix_1min':
                # VIX table: only accept vix files
                if option_type != 'stock':
                    return (
                        f"Filename `{file.filename}` is an option file (type: {option_type}), "
                        f"but selected table is `{table}` (VIX table)"
                    ), 400
                if symbol != 'vix':
                    return (
                        f"Filename `{file.filename}` has symbol `{symbol}`, "
                        f"but selected table is `{table}` (requires VIX files)"
                    ), 400
            else:
                # Option table: only accept option files (call/put)
                if option_type == 'stock':
                    return (
                        f"Filename `{file.filename}` is a stock/index file, "
                        f"but selected table is `{table}` (option table)"
                    ), 400
                table_lower = table.lower()
                if option_type not in table_lower or timeframe not in table_lower:
                    return (
                        f"Filename `{file.filename}` implies option type `{option_type}` "
                        f"and timeframe `{timeframe}`, but selected table is `{table}`"
                    ), 400

        # Merge CSV files
        merged_data = pd.DataFrame()
        total_rows_from_csv = 0

        for file in files:
            if file.filename == '':
                continue
            try:
                df = pd.read_csv(file)
                total_rows_from_csv += len(df)
                merged_data = pd.concat([merged_data, df], ignore_index=True)
            except Exception as e:
                return f"Error reading {file.filename}: {e}", 500

        # Drop duplicates within CSV files first
        if table in ('ib_stock_1min', 'ib_vix_1min'):
            key_cols = ['Timestamp']
        else:
            key_cols = ['StrikePrice', 'ContractType', 'ExpiryDate', 'Timestamp']

        csv_dupes_count = len(merged_data) - len(merged_data.drop_duplicates(subset=key_cols))
        merged_data.drop_duplicates(subset=key_cols, inplace=True)

        try:
            engine = get_sqlalchemy_engine(schema)

            # Get existing keys from SQL table to check for duplicates
            conn = get_db_connection(schema)
            with conn.cursor() as cur:
                cur.execute(f"SELECT {', '.join(key_cols)} FROM `{table}`")
                existing_rows = cur.fetchall()
            conn.close()

            # Helper to normalize datetime to string for comparison
            def normalize_dt(val):
                if val is None:
                    return ''
                if hasattr(val, 'strftime'):
                    return val.strftime('%Y-%m-%d %H:%M:%S')
                # Handle pandas Timestamp
                s = str(val)
                # Remove any timezone info and normalize format
                s = s.replace('T', ' ').split('.')[0].split('+')[0]
                return s.strip()

            # Create set of existing keys for fast lookup
            existing_keys = set()
            for row in existing_rows:
                if table in ('ib_stock_1min', 'ib_vix_1min'):
                    existing_keys.add(normalize_dt(row[0]))
                else:
                    # (StrikePrice, ContractType, ExpiryDate, Timestamp)
                    key = (
                        int(row[0]),
                        str(row[1]).strip(),
                        normalize_dt(row[2]),
                        normalize_dt(row[3])
                    )
                    existing_keys.add(key)

            # Debug: print sample keys
            print(f"[DEBUG] Existing keys count: {len(existing_keys)}")
            if existing_keys:
                sample = list(existing_keys)[:2]
                print(f"[DEBUG] Sample existing keys: {sample}")

            # Filter out rows that already exist in SQL
            rows_before_filter = len(merged_data)
            skipped_rows = []

            def row_exists(row):
                if table in ('ib_stock_1min', 'ib_vix_1min'):
                    key = normalize_dt(row['Timestamp'])
                else:
                    key = (
                        int(row['StrikePrice']),
                        str(row['ContractType']).strip(),
                        normalize_dt(row['ExpiryDate']),
                        normalize_dt(row['Timestamp'])
                    )
                return key in existing_keys

            # Debug: print sample CSV keys
            if len(merged_data) > 0:
                sample_row = merged_data.iloc[0]
                if table in ('ib_stock_1min', 'ib_vix_1min'):
                    sample_key = normalize_dt(sample_row['Timestamp'])
                else:
                    sample_key = (
                        int(sample_row['StrikePrice']),
                        str(sample_row['ContractType']).strip(),
                        normalize_dt(sample_row['ExpiryDate']),
                        normalize_dt(sample_row['Timestamp'])
                    )
                print(f"[DEBUG] Sample CSV key: {sample_key}")
                print(f"[DEBUG] Key in existing: {sample_key in existing_keys}")

            # Identify duplicates and collect info
            mask = merged_data.apply(row_exists, axis=1)
            duplicate_rows = merged_data[mask]

            for _, row in duplicate_rows.iterrows():
                skipped_rows.append({
                    'StrikePrice': row.get('StrikePrice', 'N/A'),
                    'ContractType': row.get('ContractType', 'N/A'),
                    'ExpiryDate': str(row.get('ExpiryDate', 'N/A')),
                    'Timestamp': str(row.get('Timestamp', 'N/A'))
                })

            # Keep only new rows
            new_data = merged_data[~mask]
            duplicates_skipped = rows_before_filter - len(new_data)

            # Insert only new data
            rows_inserted = 0
            if len(new_data) > 0:
                new_data.to_sql(
                    table,
                    con=engine,
                    schema=schema,
                    if_exists='append',
                    index=False
                )
                rows_inserted = len(new_data)

        except Exception as e:
            return f"Database error: {e}", 500

        # Logging
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
            "files_uploaded": ', '.join(f.filename for f in files if f.filename)
        }])

        log_entry.to_csv(
            log_path,
            mode='a',
            index=False,
            header=not os.path.exists(log_path)
        )

        return render_template('merge_result.html',
                               total_rows=total_rows_from_csv,
                               rows_inserted=rows_inserted,
                               duplicates_skipped=duplicates_skipped,
                               csv_dupes=csv_dupes_count,
                               skipped_rows=skipped_rows[:100])

    schemas = get_schemas()
    return render_template("merge_data.html", schemas=schemas)


@merge_bp.route('/api/base_tables/<schema>')
def api_get_base_tables(schema):
    """Get status of base tables in a schema"""
    return jsonify(get_base_tables_status(schema))
