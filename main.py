import subprocess
import sys
import pkg_resources
import time
from pathlib import Path

def check_and_install_requirements():
    """Check if all requirements are installed and install if missing."""
    requirements_file = Path("requirements.txt")
    
    if not requirements_file.exists():
        print("Error: requirements.txt not found!")
        sys.exit(1)
        
    # Read requirements from file
    with open(requirements_file, 'r') as f:
        requirements = [line.strip() for line in f if line.strip() and not line.startswith('#')]
    
    # Check installed packages
    installed = {pkg.key for pkg in pkg_resources.working_set}
    missing = []
    
    for requirement in requirements:
        # Remove version specifiers
        package_name = requirement.split('>=')[0].split('==')[0].split('>')[0].strip()
        if package_name.lower() not in installed:
            missing.append(requirement)
    
    if missing:
        print("Installing missing requirements...")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install"] + missing)
            print("Successfully installed all requirements.")
        except subprocess.CalledProcessError as e:
            print(f"Error installing requirements: {e}")
            sys.exit(1)
    else:
        print("All requirements already installed.")

def run_etl_once():
    """Run the ETL pipeline once."""
    try:
        from cms_hospital_etl import CMSHospitalETL
        print("Running initial ETL job...")
        etl = CMSHospitalETL()
        
        # Add debug logging
        print("Fetching hospital datasets...")
        datasets = etl.fetch_hospital_datasets()
        print(f"Found {len(datasets)} datasets")
        
        if not datasets:
            print("No datasets found! Check API response...")
            return
            
        etl.run()
        print("Initial ETL job completed.")
    except Exception as e:
        print(f"Error running ETL job: {str(e)}")
        import traceback
        print(traceback.format_exc())  # Print full stack trace
        sys.exit(1)

def setup_schedule():
    """Set up the scheduled job."""
    import schedule
    from cms_hospital_etl import CMSHospitalETL
    
    def scheduled_job():
        etl = CMSHospitalETL()
        etl.run()
    
    # Schedule job to run daily at midnight
    schedule.every().day.at("00:00").do(scheduled_job)
    print("ETL job scheduled to run daily at midnight.")
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)
    except KeyboardInterrupt:
        print("\nScheduler stopped by user.")
        sys.exit(0)

def main():
    print("Starting CMS Hospital ETL Pipeline Setup...")
    
    # Check and install requirements
    check_and_install_requirements()
    
    # Run ETL once
    run_etl_once()
    
    # Set up scheduled job
    print("Setting up scheduled job...")
    setup_schedule()

if __name__ == "__main__":
    main() 