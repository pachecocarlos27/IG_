from cms_hospital_etl import CMSHospitalETL
import schedule
import time

def run_job():
    etl = CMSHospitalETL()
    etl.run()

if __name__ == "__main__":
    # Schedule job to run daily at midnight
    schedule.every().day.at("00:00").do(run_job)
    
    while True:
        schedule.run_pending()
        time.sleep(60) 