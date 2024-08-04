import os
import logging
import yaml
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as pcsv
import pyarrow.feather as feather
import pyarrow.orc as orc
import pyarrow.ipc as ipc
from pyiceberg.catalog import load_catalog
from pyiceberg.table.metadata import new_table_metadata
from pyiceberg.exceptions import NoSuchNamespaceError, NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.table.sorting import SortOrder, SortField
from pyiceberg.transforms import IdentityTransform
import s3fs
import adlfs
import gcsfs

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ConfigLoader:
    @staticmethod
    def load_config(file_path):
        with open(file_path, 'r') as file:
            return yaml.safe_load(file)

class DirectoryManager:
    @staticmethod
    def ensure_directory_exists(directory_path):
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)

class DatabaseFetcher:
    def __init__(self, connection):
        self.connection = connection

    def fetch_table_names(self):
        query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
        cursor = self.connection.cursor()
        cursor.execute(query)
        return [row[0] for row in cursor.fetchall()]

    def fetch_table_data(self, table_name):
        query = f'SELECT * FROM {table_name};'
        cursor = self.connection.cursor()
        cursor.execute(query)
        return cursor.fetch_arrow_table()

class FileSystemFactory:
    @staticmethod
    def get_filesystem(filesystem_type, config):
        if filesystem_type == 's3':
            return s3fs.S3FileSystem(
                key=config['s3']['key'],
                secret=config['s3']['secret'],
                region_name=config['s3']['region']
            )
        elif filesystem_type == 'adl':
            return adlfs.AzureBlobFileSystem(
                account_name=config['adl']['account_name'],
                account_key=config['adl']['account_key'],
                container_name=config['adl']['container_name']
            )
        elif filesystem_type == 'gcs':
            return gcsfs.GCSFileSystem(
                project=config['gcs']['project'],
                token=config['gcs']['key_file']
            )
        elif filesystem_type == 'local':
            return None
        else:
            raise ValueError(f"Unsupported filesystem type: {filesystem_type}")

class OutputPathManager:
    @staticmethod
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

    @staticmethod
    def get_output_path(filesystem, base_path, table_name, extension):
        if filesystem is None:
            return os.path.join(base_path, f"{table_name}.{extension}")
        else:
            return f"{base_path}/{table_name}.{extension}"

class Exporter:
    def __init__(self, config):
        self.config = config
        self.filesystem_type = config['output']['filesystem']
        self.filesystem = FileSystemFactory.get_filesystem(self.filesystem_type, config)
        self.base_path = OutputPathManager.get_output_base_path(self.filesystem_type, config)
        if self.filesystem is None:
            DirectoryManager.ensure_directory_exists(self.base_path)
        else:
            self.filesystem.mkdirs(self.base_path, exist_ok=True)
        self._init_catalog_and_namespace()

    def export_to_parquet(self, table_name, arrow_table):
        self._export_table(table_name, arrow_table, 'parquet', pq.write_table)

    def export_to_csv(self, table_name, arrow_table):
        self._export_table(table_name, arrow_table, 'csv', pcsv.write_csv)

    def export_to_feather(self, table_name, arrow_table):
        self._export_table(table_name, arrow_table, 'feather', feather.write_feather)

    def export_to_orc(self, table_name, arrow_table):
        self._export_table(table_name, arrow_table, 'orc', orc.write_table)

    def export_to_ipc(self, table_name, arrow_table):
        output_path = OutputPathManager.get_output_path(self.filesystem, self.base_path, table_name, 'arrow')
        if self.filesystem is None:
            with pa.OSFile(output_path, 'wb') as sink:
                with ipc.new_file(sink, arrow_table.schema) as writer:
                    writer.write_table(arrow_table)
        else:
            with self.filesystem.open(output_path, 'wb') as f:
                with ipc.new_file(f, arrow_table.schema) as writer:
                    writer.write_table(arrow_table)
        logger.info(f"Table {table_name} exported to IPC successfully at {output_path}")

    def export_to_iceberg(self, table_name, arrow_table):
        record_count = arrow_table.num_rows
        namespace = self.config['iceberg'].get("namespace", "default")
        full_table_name = f"{namespace}.{table_name}"

        try:
            table = self.catalog.load_table(full_table_name)
            logger.info(f"Table {full_table_name} already exists.")
            # Update schema and overwrite data
            with table.update_schema() as update_schema:
                update_schema.union_by_name(arrow_table.schema)
            table.overwrite(arrow_table)
            logger.info(f"Table {full_table_name} ({record_count} records) overwritten with new data.")
        except NoSuchTableError:
            logger.info(f"Table {full_table_name} does not exist. Creating it.")
            table = self.catalog.create_table(full_table_name, schema=arrow_table.schema)
            logger.info(f"Table {full_table_name} created.")
            table.overwrite(arrow_table)
            logger.info(f"Data from {table_name} ({record_count} records) written to new Iceberg table successfully.")

    def export_to_format(self, table_name, arrow_table, export_format):
        export_methods = {
            'parquet': self.export_to_parquet,
            'csv': self.export_to_csv,
            'feather': self.export_to_feather,
            'orc': self.export_to_orc,
            'ipc': self.export_to_ipc,
            'iceberg': self.export_to_iceberg
        }
        if export_format not in export_methods:
            raise ValueError(f"Unsupported export format: {export_format}")
        export_methods[export_format](table_name, arrow_table)

    def _export_table(self, table_name, arrow_table, extension, write_func):
        record_count = arrow_table.num_rows
        output_path = OutputPathManager.get_output_path(self.filesystem, self.base_path, table_name, extension)
        if self.filesystem is None:
            write_func(arrow_table, output_path)
        else:
            with self.filesystem.open(output_path, 'wb') as f:
                write_func(arrow_table, f)
        logger.info(f"Table {table_name} ({record_count} records) exported to {extension.upper()} successfully at {output_path}")

    def _init_catalog_and_namespace(self):
        catalog_db_path = self.config["iceberg"]["uri"]
        if os.path.exists(catalog_db_path):
            os.remove(catalog_db_path)
        self.catalog = load_catalog("default", **self.config['iceberg'])
        namespace = self.config['iceberg'].get("namespace", "default")
        self._ensure_namespace_exists(namespace)

    def _ensure_namespace_exists(self, namespace):
        try:
            self.catalog.create_namespace(namespace)
        except NamespaceAlreadyExistsError:
            pass  # Namespace already exists
