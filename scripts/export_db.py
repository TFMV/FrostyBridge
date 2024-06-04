import asyncio
import asyncpg
from utils import load_config, fetch_table_names, fetch_table_data, export_to_parquet

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
            arrow_table = await fetch_table_data(conn, table_name)
            export_to_parquet(table_name, arrow_table, gcs_config['bucket_name'], gcs_config['project'])
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(export_database())
