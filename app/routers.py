from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import asyncio
import asyncpg
from scripts.utils import load_config, fetch_table_names, fetch_table_data, export_to_format, export_to_iceberg

router = APIRouter()

class ExportRequest(BaseModel):
    format: str

@router.post("/export")
async def export_database(request: ExportRequest):
    config = load_config()
    pg_config = config['postgres']
    gcs_config = config['gcs']
    arrow_format = request.format

    if arrow_format not in ['parquet', 'csv', 'feather', 'json', 'orc', 'iceberg']:
        raise HTTPException(status_code=400, detail="Unsupported format")

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
            if arrow_format == 'iceberg':
                export_to_iceberg(table_name, arrow_table, gcs_config['bucket_name'], gcs_config['project'])
            else:
                export_to_format(table_name, arrow_table, gcs_config['bucket_name'], gcs_config['project'], arrow_format)
    finally:
        await conn.close()

    return {"message": "Export completed successfully"}
