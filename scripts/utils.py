import duckdb
import asyncpg
import yaml
import os
from gcsfs import GCSFileSystem

def load_config():
    with open(os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml'), 'r') as file:
        config = yaml.safe_load(file)
    return config

async def fetch_table_names(connection):
    query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
    result = await connection.fetch(query)
    return [row['table_name'] for row in result]

def export_to_iceberg(table_name, data, gcs_bucket, gcs_project):
    duckdb_conn = duckdb.connect(database=':memory:')
    duckdb_conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM data")
    gcsfs = GCSFileSystem(project=gcs_project)
    with gcsfs.open(f"{gcs_bucket}/{table_name}.parquet", 'wb') as f:
        duckdb_conn.execute(f"COPY {table_name} TO 's3://{gcs_bucket}/{table_name}.parquet' (FORMAT 'parquet')")
