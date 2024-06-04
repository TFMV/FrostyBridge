import asyncio
import yaml
from app.routers import export_database, ExportRequest

async def main():
    with open('config/config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    request = ExportRequest(format=config['arrow']['format'])
    await export_database(request)

if __name__ == "__main__":
    asyncio.run(main())