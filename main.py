from app.routers import export_database, ExportRequest
from scripts.utils import load_config

def main():
    config = load_config()

    connection_config = {
        'user': config['database']['user'],
        'password': config['database']['password'],
        'database': config['database']['dbname'],
        'host': config['database']['host'],
        'port': config['database']['port']
    }

    request = ExportRequest(format=config['output']['format'], connection_config=connection_config)
    export_database(request, config)

if __name__ == "__main__":
    main()
