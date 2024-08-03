import os
import adbc_driver_postgresql.dbapi as pgdbapi
from scripts.utils import ConfigLoader

script_dir = os.path.dirname(__file__)
config_path = os.path.join(script_dir, '../config/config.yaml')
config = ConfigLoader.load_config(config_path)

def get_pg_uri(db_config):
    return f"postgresql://{db_config['user']}:{db_config['password']}@" \
           f"{db_config['host']}:{db_config['port']}/{db_config['dbname']}"

class DatabaseConnectionManager:
    def __init__(self):
        self.conn = self.connect_to_database(config['database'])

    def connect_to_database(self, db_config):
        return pgdbapi.connect(get_pg_uri(db_config))

    def get_connection(self):
        return self.conn

db_manager = DatabaseConnectionManager()
