from typing import Dict, List
import pandas as pd
import sqlite3
from datetime import datetime
from pathlib import Path
import logging
import requests
import concurrent.futures
from tqdm import tqdm
import traceback
import re

class CMSHospitalETL:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        
        
        self.base_dir = Path("data")
        self.raw_dir = self.base_dir / "raw"
        self.processed_dir = self.base_dir / "processed"
        self.metadata_dir = self.base_dir / "metadata"
        
        for dir_path in [self.raw_dir, self.processed_dir, self.metadata_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
        
        self.metadata_db = self.metadata_dir / "metadata.db"
        self.cms_api_url = "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
        
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        self.logger.addHandler(handler)
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.metadata_db) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS file_metadata (
                    file_id TEXT PRIMARY KEY,
                    filename TEXT,
                    last_modified TEXT,
                    last_processed TEXT
                )
            """)

    def to_snake_case(self, name: str) -> str:
        name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()

    def process_dataset(self, dataset: Dict):
        try:
            download_url = dataset['distribution'][0]['downloadURL']
            filename = f"{dataset['identifier']}.csv"
            raw_path = self.raw_dir / filename
            processed_path = self.processed_dir / filename

            if not raw_path.exists() or self.needs_update(dataset):
                self.logger.info(f"Downloading {filename}")
                df = pd.read_csv(download_url, low_memory=False)
                df.to_csv(raw_path, index=False)
                
                df.columns = [self.to_snake_case(col) for col in df.columns]
                df.to_csv(processed_path, index=False)
                
                
                with sqlite3.connect(self.metadata_db) as conn:
                    conn.execute(
                        "INSERT OR REPLACE INTO file_metadata (file_id, filename, last_modified, last_processed) VALUES (?, ?, ?, ?)",
                        (dataset['identifier'], filename, dataset['modified'], datetime.now().strftime('%Y-%m-%d'))
                    )
                
                self.logger.info(f"Successfully processed {filename}")
            else:
                self.logger.info(f"Skipping {filename} - already up to date")
            
        except Exception as e:
            self.logger.error(f"Error processing dataset {dataset['identifier']}: {str(e)}")

    def fetch_hospital_datasets(self) -> List[Dict]:
        try:
            response = requests.get(self.cms_api_url)
            if response.status_code != 200:
                self.logger.error(f"API request failed with status code: {response.status_code}")
                return []
            
            datasets = response.json()
            hospital_datasets = [
                dataset for dataset in datasets
                if any('hospital' in theme.lower() for theme in dataset.get('theme', [])) 
                or 'hospital' in dataset.get('title', '').lower()
            ]
            
            self.logger.info(f"Found {len(hospital_datasets)} hospital-related datasets")
            return hospital_datasets
            
        except Exception as e:
            self.logger.error(f"Error fetching datasets: {str(e)}")
            return []

    def needs_update(self, dataset: Dict) -> bool:
        """Force update if no existing data, otherwise check modification date."""
        if not (self.raw_dir / f"{dataset['identifier']}.csv").exists():
            return True
            
        try:
            with sqlite3.connect(self.metadata_db) as conn:
                result = conn.execute(
                    "SELECT last_modified FROM file_metadata WHERE file_id = ?",
                    (dataset['identifier'],)
                ).fetchone()
                
                return True if not result else dataset['modified'] > result[0]
                
        except Exception as e:
            self.logger.error(f"Error checking update status: {str(e)}")
            return True

    def check_existing_data(self) -> bool:
        """Check if data directories have data and metadata DB exists."""
        try:
            # Check if directories have any CSV files
            raw_files = list(self.raw_dir.glob('*.csv'))
            processed_files = list(self.processed_dir.glob('*.csv'))
            
            if not (raw_files and processed_files):
                self.logger.info("No existing data found in data directories")
                return False
            
            # Check if metadata DB exists and has records
            if not self.metadata_db.exists():
                self.logger.info("Metadata database not found")
                return False
            
            with sqlite3.connect(self.metadata_db) as conn:
                count = conn.execute("SELECT COUNT(*) FROM file_metadata").fetchone()[0]
                if count == 0:
                    self.logger.info("No records in metadata database")
                    return False
                
                last_processed = conn.execute(
                    "SELECT MAX(last_processed) FROM file_metadata"
                ).fetchone()[0]
                
                if last_processed:
                    days_since_update = (datetime.now() - datetime.strptime(last_processed, '%Y-%m-%d')).days
                    self.logger.info(f"Data last updated {days_since_update} days ago")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error checking existing data: {str(e)}")
            return False

    def run(self):
        try:
            # Check existing data first
            if self.check_existing_data():
                self.logger.info("Checking for updates to existing data...")
            else:
                self.logger.info("Initial data download required")
            
            datasets = self.fetch_hospital_datasets()
            datasets_to_process = [d for d in datasets if self.needs_update(d)]
            
            if not datasets_to_process:
                self.logger.info("All datasets are up to date")
                return
            
            self.logger.info(f"Processing {len(datasets_to_process)} datasets")
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                list(tqdm(
                    executor.map(self.process_dataset, datasets_to_process),
                    total=len(datasets_to_process),
                    desc="Processing datasets"
                ))
                
            self.logger.info("ETL job completed successfully")
            
        except Exception as e:
            self.logger.error(f"Error in ETL job: {str(e)}")
            raise 