# CMS Hospital Data ETL Pipeline
This project downloads and processes hospital-related datasets from the CMS (Centers for Medicare & Medicaid Services) data portal.

## Features

- Automatically downloads hospital-related datasets from CMS
- Converts column names to snake_case format
- Maintains both raw and processed data
- Tracks metadata and updates
- Runs daily to check for new/updated data
- Processes files in parallel
  
## Directory Structure
project/
├── data/
│ ├── raw/ # Original downloaded files
│ ├── processed/ # Files with transformed column names
│ └── metadata/ # Contains metadata database
├── main.py # Main entry point
├── cms_hospital_etl.py # ETL implementation
└── requirements.txt # Project dependencies

## Requirements

- Python 3.7+
- Required packages: pandas, requests, tqdm, schedule

## Usage

1. Install dependencies: pip install -r requirements.txt
2. Run the ETL pipeline: python main.py


The script will:
- Download hospital-related datasets from CMS
- Convert column names to snake_case
- Store both raw and processed versions
- Run daily at midnight to check for updates

## Data Processing

- Raw data is stored in `data/raw/`
- Processed data (with transformed column names) is stored in `data/processed/`
- Metadata about downloads is stored in `data/metadata/metadata.db`

## Scheduling

The pipeline runs daily at midnight to check for updates. You can modify the schedule in `main.py`.
