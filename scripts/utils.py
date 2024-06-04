import pyarrow as pa
import asyncpg
import yaml
import os
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, LongType

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

def convert_arrow_to_pyiceberg_records(arrow_table, iceberg_schema):
    records = []
    for batch in arrow_table.to_batches():
        for row in batch.to_pydict().values():
            record = {}
            for field in iceberg_schema.fields:
                record[field.name] = row.get(field.name)
            records.append(record)
    return records

def export_to_iceberg(table_name, arrow_table, gcs_bucket, gcs_project):
    # Load the Iceberg catalog
    catalog = load_catalog("default")

    # Define the schema (adjust as necessary)
    schema = Schema(
        NestedField.required(1, "id", LongType.get()),
        NestedField.optional(2, "name", StringType.get())
    )

    # Load the table from the catalog
    table = catalog.load_table(f"{gcs_bucket}.{table_name}")

    # Convert Arrow table to PyIceberg records
    records = convert_arrow_to_pyiceberg_records(arrow_table, schema)

    # Append the records to the table
    with table.new_append() as append:
        for record in records:
            append.append(record)
        append.commit()

