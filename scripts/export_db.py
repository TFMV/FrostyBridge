import asyncio
import asyncpg
from utils import load_config, fetch_table_names, export_to_iceberg

async def export_database():
    config = load_config()
    pg_config = config['postgres']
    gcs_config = config['gcs']

    conn = await asyncpg.connect(
        user=pg_config['user'],
        password=pg_config['password'],
        database=pg_config['database'],
        host=pg_config['host'],
        port=pg_config['port']
    )

    try:
        table_names = await fetch_table_names(conn)
        for table_name in table_names:
            query = f'SELECT * FROM {table_name};'
            data = await conn.fetch(query)
            export_to_iceberg(table_name, data, gcs_config['bucket_name'], gcs_config['project'])
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(export_database())
