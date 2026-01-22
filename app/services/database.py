"""
Database connection and helper functions
"""
import pymysql
from sqlalchemy import create_engine
from config import Config

DB_CONFIG = Config.DATABASE

# Base tables that should exist in each schema
BASE_TABLES = {
    'ib_2w_call_1min': {
        'display': 'ib_2w_call_1min',
        'type': 'call'
    },
    'ib_2w_put_1min': {
        'display': 'ib_2w_put_1min',
        'type': 'put'
    },
    'ib_stock_1min': {
        'display': 'ib_stock_1min',
        'type': 'stock'
    }
}


def get_db_connection(schema=None):
    """Get a PyMySQL connection"""
    config = DB_CONFIG.copy()
    if schema:
        config['database'] = schema
    return pymysql.connect(**config)


def get_sqlalchemy_engine(schema):
    """Get a SQLAlchemy engine for pandas operations"""
    config = DB_CONFIG.copy()
    return create_engine(
        f"mysql+pymysql://{config['user']}:{config['password']}@"
        f"{config['host']}:{config.get('port', 3306)}/{schema}"
    )


def get_schemas():
    """Get list of user databases (excluding system databases)"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SHOW DATABASES")
            schemas = [
                row[0] for row in cur.fetchall()
                if row[0] not in ('information_schema', 'mysql', 'performance_schema', 'sys')
            ]
        return schemas
    finally:
        conn.close()


def get_tables(schema, pattern='_1min'):
    """Get tables in a schema matching a pattern"""
    conn = get_db_connection(schema)
    try:
        with conn.cursor() as cur:
            cur.execute("SHOW TABLES")
            tables = [row[0] for row in cur.fetchall() if pattern in row[0]]
        return tables
    finally:
        conn.close()


def get_base_tables_status(schema):
    """Check which base tables exist in a schema"""
    existing_tables = get_tables(schema, pattern='')
    status = {}
    for table_name, info in BASE_TABLES.items():
        status[table_name] = {
            'exists': table_name in existing_tables,
            'display': info['display'],
            'type': info['type']
        }
    return status


def create_base_table(schema, table_name):
    """Create a base table with the standard schema"""
    conn = get_db_connection(schema)
    try:
        with conn.cursor() as cur:
            if table_name == 'ib_stock_1min':
                # Stock table schema (simpler, no options-specific fields)
                create_sql = f"""
                CREATE TABLE IF NOT EXISTS `{table_name}` (
                    Timestamp DATETIME NOT NULL,
                    Open DECIMAL(10,4),
                    Close DECIMAL(10,4),
                    High DECIMAL(10,4),
                    Low DECIMAL(10,4),
                    Volume BIGINT,
                    PRIMARY KEY (Timestamp)
                )
                """
            else:
                # Options table schema
                create_sql = f"""
                CREATE TABLE IF NOT EXISTS `{table_name}` (
                    StrikePrice INT NOT NULL,
                    ContractType VARCHAR(10) NOT NULL,
                    ExpiryDate DATETIME NOT NULL,
                    Timestamp DATETIME NOT NULL,
                    Open DECIMAL(10,4),
                    Close DECIMAL(10,4),
                    High DECIMAL(10,4),
                    Low DECIMAL(10,4),
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


def create_resampled_table(schema, dest_table):
    """Create destination table for resampled option data"""
    conn = get_db_connection(schema)
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{schema}`.`{dest_table}`")
            create_sql = f"""
            CREATE TABLE `{schema}`.`{dest_table}` (
                StrikePrice INT NOT NULL,
                ContractType VARCHAR(10) NOT NULL,
                ExpiryDate DATETIME NOT NULL,
                Timestamp DATETIME NOT NULL,
                Open DECIMAL(10,4),
                Close DECIMAL(10,4),
                High DECIMAL(10,4),
                Low DECIMAL(10,4),
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
