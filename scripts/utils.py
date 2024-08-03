import os
import logging
import yaml
import pyarrow.parquet as pq
import pyarrow.csv as pcsv
import pyarrow.feather as feather
import pyarrow.orc as orc
import pyarrow.ipc as ipc
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.table.metadata import new_table_metadata
from pyiceberg.exceptions import NoSuchNamespaceError, NamespaceAlreadyExistsError, NoSuchTableError
import s3fs
import adlfs
import gcsfs
from pyiceberg.table.sorting import SortOrder, SortField
from pyiceberg.transforms import IdentityTransform

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_config():
    with open(os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml'), 'r') as file:
        config = yaml.safe_load(file)
    return config

def ensure_directory_exists(directory_path):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)

def fetch_table_names(connection):
    query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    return [row[0] for row in result]

def fetch_table_data(connection, table_name):
    query = f'SELECT * FROM {table_name};'
    cursor = connection.cursor()
    cursor.execute(query)
    arrow_table = cursor.fetch_arrow_table()
    return arrow_table

def get_filesystem(filesystem_type, config):
    if filesystem_type == 's3':
        return s3fs.S3FileSystem(key=config['s3']['key'], secret=config['s3']['secret'], region_name=config['s3']['region'])
    elif filesystem_type == 'adl':
        return adlfs.AzureBlobFileSystem(account_name=config['adl']['account_name'], account_key=config['adl']['account_key'], container_name=config['adl']['container_name'])
    elif filesystem_type == 'gcs':
        return gcsfs.GCSFileSystem(project=config['gcs']['project'], token=config['gcs']['key_file'])
    elif filesystem_type == 'local':
        return None
    else:
        raise ValueError(f"Unsupported filesystem type: {filesystem_type}")

def get_output_base_path(filesystem_type, config):
    if 'iceberg' in config and 'warehouse' in config['iceberg']:
        base_path = config['iceberg']['warehouse']
    else:
        raise KeyError("The 'warehouse' key is missing from the 'iceberg' configuration.")

    if filesystem_type == 's3':
        return f"s3://{config['s3']['bucket']}/{base_path}"
    elif filesystem_type == 'adl':
        return f"adl://{config['adl']['account_name']}/{base_path}"
    elif filesystem_type == 'gcs':
        return f"gcs://{config['gcs']['bucket_name']}/{base_path}"
    else:  # Local
        return base_path

def get_output_path(filesystem, base_path, table_name, extension):
    if filesystem is None:
        return os.path.join(base_path, f"{table_name}.{extension}")
    else:
        return f"{base_path}/{table_name}.{extension}"

def export_to_parquet(table_name, arrow_table, filesystem, base_path):
    record_count = arrow_table.num_rows
    output_path = get_output_path(filesystem, base_path, table_name, 'parquet')
    if filesystem is None:
        pq.write_table(arrow_table, output_path)
    else:
        with filesystem.open(output_path, 'wb') as f:
            pq.write_table(arrow_table, f)
    logger.info(f"Table {table_name} ({record_count} records) exported to Parquet successfully at {output_path}")

def export_to_csv(table_name, arrow_table, filesystem, base_path):
    record_count = arrow_table.num_rows
    output_path = get_output_path(filesystem, base_path, table_name, 'csv')
    if filesystem is None:
        pcsv.write_csv(arrow_table, output_path)
    else:
        with filesystem.open(output_path, 'wb') as f:
            pcsv.write_csv(arrow_table, f)
    logger.info(f"Table {table_name} ({record_count} records) exported to CSV successfully at {output_path}")

def export_to_feather(table_name, arrow_table, filesystem, base_path):
    record_count = arrow_table.num_rows
    output_path = get_output_path(filesystem, base_path, table_name, 'feather')
    if filesystem is None:
        feather.write_feather(arrow_table, output_path)
    else:
        with filesystem.open(output_path, 'wb') as f:
            feather.write_feather(arrow_table, f)
    logger.info(f"Table {table_name} ({record_count} records) exported to Feather successfully at {output_path}")

def export_to_orc(table_name, arrow_table, filesystem, base_path):
    record_count = arrow_table.num_rows
    output_path = get_output_path(filesystem, base_path, table_name, 'orc')
    if filesystem is None:
        orc.write_table(arrow_table, output_path)
    else:
        with filesystem.open(output_path, 'wb') as f:
            orc.write_table(arrow_table, f)
    logger.info(f"Table {table_name} ({record_count} records) exported to ORC successfully at {output_path}")

def export_to_ipc(table_name, arrow_table, filesystem, base_path):
    record_count = arrow_table.num_rows
    output_path = get_output_path(filesystem, base_path, table_name, 'arrow')
    if filesystem is None:
        with pa.OSFile(output_path, 'wb') as sink:
            with ipc.new_file(sink, arrow_table.schema) as writer:
                writer.write_table(arrow_table)
    else:
        with filesystem.open(output_path, 'wb') as f:
            with ipc.new_file(f, arrow_table.schema) as writer:
                writer.write_table(arrow_table)
    logger.info(f"Table {table_name} ({record_count} records) exported to IPC successfully at {output_path}")

def export_to_iceberg(table_name, arrow_table, config):
    record_count = arrow_table.num_rows
    filesystem_type = config['output']['filesystem']
    filesystem = get_filesystem(filesystem_type, config)
    warehouse_path = get_output_base_path(filesystem_type, config)

    namespace_path = os.path.join(warehouse_path, config['iceberg'].get("namespace", "default"))
    table_directory = os.path.join(namespace_path, table_name)

    if filesystem is None:
        ensure_directory_exists(table_directory)
    else:
        filesystem.mkdirs(table_directory, exist_ok=True)

    # Ensure the directory structure for the default.db exists
    db_directory = os.path.join(namespace_path, "default.db")
    if filesystem is None:
        ensure_directory_exists(db_directory)
    else:
        filesystem.mkdirs(db_directory, exist_ok=True)

    # Ensure catalog database is initialized correctly
    catalog_db_path = config["iceberg"]["uri"].replace("sqlite:///", "")
    if os.path.exists(catalog_db_path):
        os.remove(catalog_db_path)

    # Load the Iceberg catalog
    catalog = load_catalog("default", **config['iceberg'])

    # Define the namespace and table name
    namespace = config['iceberg'].get("namespace", "default")
    full_table_name = f"{namespace}.{table_name}"

    # Ensure the namespace exists
    try:
        catalog.create_namespace(namespace)
    except NamespaceAlreadyExistsError:
        pass  # Namespace already exists

    # Check if the table exists, and create it if it doesn't
    try:
        table = catalog.load_table(full_table_name)
        logger.info(f"Table {full_table_name} already exists.")
        
        # Update schema and overwrite data
        with table.update_schema() as update_schema:
            update_schema.union_by_name(arrow_table.schema)
        
        table.overwrite(arrow_table)
        logger.info(f"Table {full_table_name} ({record_count} records) overwritten with new data.")
        
    except NoSuchTableError:
        logger.info(f"Table {full_table_name} does not exist. Creating it.")
        table = catalog.create_table(full_table_name, schema=arrow_table.schema)
        logger.info(f"Table {full_table_name} created.")
        
        table.overwrite(arrow_table)
        logger.info(f"Data from {table_name} ({record_count} records) written to new Iceberg table successfully.")
    
def export_to_format(table_name, arrow_table, export_format, config):
    filesystem_type = config['output']['filesystem']
    filesystem = get_filesystem(filesystem_type, config)
    base_path = get_output_base_path(filesystem_type, config)

    if filesystem is None:
        ensure_directory_exists(base_path)
    else:
        filesystem.mkdirs(base_path, exist_ok=True)

    if export_format == "iceberg":
        export_to_iceberg(table_name, arrow_table, config)
    elif export_format == "parquet":
        export_to_parquet(table_name, arrow_table, filesystem, base_path)
    elif export_format == "csv":
        export_to_csv(table_name, arrow_table, filesystem, base_path)
    elif export_format == "feather":
        export_to_feather(table_name, arrow_table, filesystem, base_path)
    elif export_format == "orc":
        export_to_orc(table_name, arrow_table, filesystem, base_path)
    elif export_format == "ipc":
        export_to_ipc(table_name, arrow_table, filesystem, base_path)
    else:
        raise ValueError(f"Unsupported export format: {export_format}")
