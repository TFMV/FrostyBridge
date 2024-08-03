import logging
import time
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from scripts.db_manager import db_manager
from scripts.utils import fetch_table_names, fetch_table_data, export_to_format, load_config

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter()

class ExportRequest(BaseModel):
    format: str

@router.post("/export")
def export_database(request: ExportRequest, config: dict):
    iceberg_config = config['iceberg']
    arrow_format = request.format

    supported_formats = ['parquet', 'csv', 'iceberg', 'feather', 'orc', 'ipc']

    if arrow_format not in supported_formats:
        raise HTTPException(status_code=400, detail="Unsupported format")

    conn = db_manager.get_connection()

    try:
        table_names = fetch_table_names(conn)
        for table_name in table_names:
            start_time = time.time()
            arrow_table = fetch_table_data(conn, table_name)
            record_count = arrow_table.num_rows
            export_to_format(table_name, arrow_table, arrow_format, config)
            end_time = time.time()
            duration = end_time - start_time
            logger.info(f"Exported table {table_name} ({record_count} records) to {arrow_format} in {duration:.2f} seconds")
    finally:
        conn.close()

    return {"message": "Export completed successfully"}
