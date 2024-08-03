from app.routers import export_database, ExportRequest
from scripts.utils import ConfigLoader

def main():
    config = ConfigLoader.load_config('config/config.yaml')

    request = ExportRequest(format=config['output']['format'])
    export_database(request)

if __name__ == "__main__":
    main()
