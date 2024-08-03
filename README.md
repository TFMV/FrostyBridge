# FrostyBridge: Exporting PostgreSQL Databases to Various Storage Systems

FrostyBridge is a Python solution designed to export entire PostgreSQL databases to various storage systems in open data formats. It enables efficient and reliable data migration for analytical purposes.

FrostyBridge can be used to extract an entire Postgres database to an Iceberg data lake.

![FrostyBridge](assets/fb.webp)

## Features

* **Full Database Export:** Seamlessly export all tables from a PostgreSQL database to supported storage systems, including local, S3, ADL, and GCS.
* **Multiple Output Formats** Support for multiple output formats, including Parquet, CSV, Iceberg, Feather, ORC, and IPC.
* **Arrow Power:** Utilizes Apache Arrow for efficient data processing, providing high performance and memory efficiency.

## Prerequisites

* Python 3.7 or later
* A PostgreSQL database
* Required Python packages: Install them with `pip install -r requirements.txt`

## Installation

1. **Clone the repository:**

```bash
git clone https://github.com/TFMV/FrostyBridge.git
```

2. **Install required packages:**

```bash
pip install -r requirements.txt
```

3. **Configure `config.yaml`:**

Edit the `config/config.yaml` file with your PostgreSQL and output details

## Usage

### CLI

Run the following command to export the PostgreSQL database to GCS:

```bash
./cli.sh
```

### API

1. **Start the API server:**

```bash
./api.sh
```

2. **Send an API request to trigger the export.**

### Docker

1. **Build the Docker image:**

```bash
docker build -t frostybridge .
```

3. **Run the Docker container:**

```bash
docker run -v /path/to/config:/app/config -v /path/to/local/parquet/files:/app/parquet-files frostybridge
```

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.

## Author

Thomas F McGeehan V
