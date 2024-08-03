import adbc_driver_postgresql.dbapi as pgdbapi
from scripts.utils import load_config

config = load_config()

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
