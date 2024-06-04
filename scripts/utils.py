import pyarrow as pa
import pyarrow.parquet as pq
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

async def fetch_table_data(connection, table_name):
    query = f'SELECT * FROM {table_name};'
    records = await connection.fetch(query)
    return pa.Table.from_pylist([dict(record) for record in records])

def export_to_parquet(table_name, arrow_table, gcs_bucket, gcs_project):
    gcsfs = GCSFileSystem(project=gcs_project)
    parquet_path = f"{gcs_bucket}/{table_name}.parquet"
    
    with gcsfs.open(parquet_path, 'wb') as f:
        pq.write_table(arrow_table, f, compression='snappy')

