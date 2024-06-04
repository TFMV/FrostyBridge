import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as pcsv
import pyarrow.feather as pf
import pyarrow.json as pjson
import pyarrow.orc as porc
import asyncpg
import yaml
import os
from gcsfs import GCSFileSystem
from pyspark.sql import SparkSession

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
        'json': 'json',
        'orc': 'orc'
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
        elif arrow_format == 'orc':
            porc.write_table(arrow_table, f)
        else:
            raise ValueError(f"Unsupported format: {arrow_format}")

def export_to_iceberg(table_name, arrow_table, gcs_bucket, gcs_project):
    import importlib
    import sys

    # Check if the 'distutils' module is available
    if 'distutils' in sys.modules:
        from distutils.version import LooseVersion
    else:
        # If not, try to import it from the 'setuptools' package
        setuptools_version = importlib.import_module('setuptools.version')
        LooseVersion = setuptools_version.LooseVersion

    # Initialize Spark session with Iceberg support
    spark = SparkSession.builder \
        .appName("FrostyBridge") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hive") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://localhost:9083") \
        .getOrCreate()

    # Convert Arrow Table to Spark DataFrame
    df = spark.createDataFrame(arrow_table.to_pandas())

    # Write DataFrame to Iceberg table
    df.write.format("iceberg").mode("overwrite").save(f"{gcs_bucket}/{table_name}")

    spark.stop()