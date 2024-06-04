## FrostyBridge: Exporting PostgreSQL Databases to Google Cloud Storage

FrostyBridge is a Python solution designed to export entire PostgreSQL databases to Google Cloud Storage (GCS) in open data formats supported by Apache Arrow. It enables efficient and reliable data migration for analytical purposes.

![FrostyBridge](assets/fb.webp)

### Features

* **Full Database Export:** Export all tables from a PostgreSQL database to GCS.
* **Parquet Format:** Each table is stored in the widely-used Parquet format for optimized data storage and retrieval.
* **Arrow Power:** Utilizes Apache Arrow for efficient data processing, providing high performance and memory efficiency.
* **Asynchronous Operations:** Leverages asyncpg for asynchronous operations, ensuring faster execution and scalability.

### Prerequisites

* Python 3.7 or later
* A PostgreSQL database
* A Google Cloud Storage bucket
* Required Python packages: Install them with `pip install -r requirements.txt`

### Project Structure

```
.
├── .git
├── .gitignore
├── Dockerfile
├── LICENSE
├── README.md
├── api.sh
├── app
│ ├── __init__.py
│ ├── app.py
│ └── routers.py
├── assets
│ └── fb.webp
├── cli.sh
├── config
│ ├── config.yaml
│ └── sa.json
├── jars
│ └── gcs-connector-hadoop3-2.2.5-shaded.jar
├── main.py
├── requirements.txt
└── scripts
├── __init__.py
├── export_db.py
└── utils.py
```

### Installation

1. **Clone the repository:**

```bash
git clone https://github.com/TFMV/FrostyBridge.git
```

2. **Install required packages:**

```bash
pip install -r requirements.txt
```

3. **Configure `config.yaml`:**

Edit the `config/config.yaml` file with your PostgreSQL and GCS details:

```yaml
database_url: "postgresql://user:password@host:port/database"
gcs_bucket: "your-gcs-bucket"
gcs_project: "your-gcp-project"
```

### Usage

#### CLI

Run the following command to export the PostgreSQL database to GCS:

```bash
./cli.sh
```

#### API

1. **Start the API server:**

```bash
./api.sh
```

2. **Send an API request to trigger the export.**

#### Docker

1. **Build the Docker image:**

```bash
docker build -t frostybridge .
```

2. **Run the Docker container:**

```bash
docker run -v /path/to/config:/app/config -v /path/to/local/parquet/files:/app/parquet-files frostybridge
```

### License

This project is licensed under the MIT License. See the `LICENSE` file for details.

### Author

Thomas F McGeehan V
