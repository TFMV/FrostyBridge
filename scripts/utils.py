import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as pcsv
import pyarrow.feather as pf
import pyarrow.json as pjson
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

def export_to_format(table_name, arrow_table, gcs_bucket, gcs_project, arrow_format):
    gcsfs = GCSFileSystem(project=gcs_project)
    file_extension = {
        'parquet': 'parquet',
        'csv': 'csv',
        'feather': 'feather',
        'json': 'json'
    }.get(arrow_format, 'parquet')
    
    file_path = f"{gcs_bucket}/{table_name}.{file_extension}"
    
    with gcsfs.open(file_path, 'wb') as f:
        if arrow_format == 'parquet':
            pq.write_table(arrow_table, f, compression='snappy')
        elif arrow_format == 'csv':
            pcsv.write_csv(arrow_table, f)
        elif arrow_format == 'feather':
            pf.write_feather(arrow_table, f)
        elif arrow_format == 'json':
            pjson.write_json(arrow_table, f)
        else:
            raise ValueError(f"Unsupported format: {arrow_format}")
