# FrostyBridge

FrostyBridge is a Python solution that exports an entire PostgreSQL database to Google Cloud Storage (GCS) in open data formats supported by Apache Arrow.

![FrostyBridge](assets/fb.webp)

## Features

- Export all tables from a PostgreSQL database to GCS
- Each table is stored in Iceberg format
- Utilizes DuckDB for efficient data processing
- Asynchronous operations with asyncpg

## Prerequisites

- Python 3.7+
- PostgreSQL database
- Google Cloud Storage bucket
- Install the required Python packages

## Installation

### Clone the repository

```bash
git clone https://github.com/TFMV/FrostyBridge.git
```

### Install the required packages

```bash
pip install -r requirements.txt
```

Configure the `config.yaml` file with your PostgreSQL and GCS details

## Usage

```bash
bash run.sh
```
