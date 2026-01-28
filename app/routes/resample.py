"""
Resample option data routes
"""
from flask import Blueprint, render_template, request, jsonify

from app.services import (
    get_db_connection,
    get_schemas,
    get_tables,
    create_resampled_table
)

resample_bp = Blueprint('resample', __name__)

# Timeframe mapping (minutes)
TIMEFRAMES = {
    '3min': 3,
    '5min': 5,
    '15min': 15,
    '1hr': 60
}


def execute_resample_option(schema, src_table, dest_table, tf_minutes):
    """Execute resampling from 1min to higher timeframe for option tables"""
    conn = get_db_connection(schema)
    try:
        with conn.cursor() as cur:
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
                MIN(Low) AS Low,
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
        print(f"Error resampling option: {e}")
        return False
    finally:
        conn.close()


def execute_resample_stock(schema, src_table, dest_table, tf_minutes):
    """Execute resampling from 1min to higher timeframe for stock/VIX tables"""
    conn = get_db_connection(schema)
    try:
        with conn.cursor() as cur:
            # Use MIN/MAX on concatenated timestamp+value to reliably get first/last
            # This avoids GROUP_CONCAT ordering issues
            resample_sql = f"""
            INSERT INTO `{schema}`.`{dest_table}`
            (Timestamp, Open, Close, High, Low, Volume)

            WITH bucketed AS (
                SELECT
                    DATE_ADD(
                      CONCAT(DATE(Timestamp), ' 09:30:00'),
                      INTERVAL FLOOR(
                        TIMESTAMPDIFF(
                          MINUTE,
                          CONCAT(DATE(Timestamp), ' 09:30:00'),
                          Timestamp
                        ) / {tf_minutes}
                      ) * {tf_minutes} MINUTE
                    ) AS candle_ts,
                    Timestamp AS orig_ts,
                    Open,
                    Close,
                    High,
                    Low,
                    Volume
                FROM `{schema}`.`{src_table}`
                WHERE TIME(Timestamp) BETWEEN '09:30:00' AND '15:59:00'
                  AND Open IS NOT NULL
                  AND Close IS NOT NULL
                  AND High IS NOT NULL
                  AND Low IS NOT NULL
            )

            SELECT
                candle_ts AS Timestamp,
                CAST(SUBSTRING_INDEX(
                  MIN(CONCAT(DATE_FORMAT(orig_ts, '%Y%m%d%H%i%s'), '|', Open)),
                  '|', -1
                ) AS DECIMAL(10,4)) AS Open,
                CAST(SUBSTRING_INDEX(
                  MAX(CONCAT(DATE_FORMAT(orig_ts, '%Y%m%d%H%i%s'), '|', Close)),
                  '|', -1
                ) AS DECIMAL(10,4)) AS Close,
                MAX(High) AS High,
                MIN(Low) AS Low,
                SUM(Volume) AS Volume
            FROM bucketed
            GROUP BY candle_ts
            ORDER BY candle_ts
            """
            cur.execute(resample_sql)
            conn.commit()
        return True
    except Exception as e:
        print(f"Error resampling stock: {e}")
        return False
    finally:
        conn.close()


def get_table_type(src_table):
    """Determine table type from source table name"""
    if 'stock' in src_table.lower():
        return 'stock'
    elif 'vix' in src_table.lower():
        return 'vix'
    return 'option'


@resample_bp.route('/resample')
def resample():
    schemas = get_schemas()
    return render_template('resample.html', schemas=schemas, timeframes=TIMEFRAMES)


@resample_bp.route('/api/tables/<schema>')
def api_get_tables(schema):
    """Get 1min tables in a schema"""
    return jsonify(get_tables(schema, pattern='_1min'))


@resample_bp.route('/api/resample', methods=['POST'])
def api_resample():
    """Execute resampling"""
    data = request.json
    schema = data.get('schema')
    src_table = data.get('table')
    timeframes = data.get('timeframes', [])

    if not schema or not src_table or not timeframes:
        return jsonify({'success': False, 'error': 'Missing required parameters'}), 400

    # Determine table type
    table_type = get_table_type(src_table)

    results = []

    for tf_name in timeframes:
        if tf_name not in TIMEFRAMES:
            continue

        tf_minutes = TIMEFRAMES[tf_name]
        dest_table = src_table.replace('_1min', f'_{tf_name}')

        if not create_resampled_table(schema, dest_table, table_type):
            results.append({
                'timeframe': tf_name,
                'success': False,
                'error': 'Failed to create destination table'
            })
            continue

        # Use appropriate resample function based on table type
        if table_type in ('stock', 'vix'):
            success = execute_resample_stock(schema, src_table, dest_table, tf_minutes)
        else:
            success = execute_resample_option(schema, src_table, dest_table, tf_minutes)

        if success:
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
