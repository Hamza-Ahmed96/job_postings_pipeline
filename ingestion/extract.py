import math
import sys
import time
import json
import os
import requests
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.my_exception import CustomException
from dotenv import load_dotenv
from loguru import logger



load_dotenv()
# Extract the API Keys and API ID from .env 
app_id = os.getenv("ADZUNA_APP_ID")
app_key = os.getenv("ADZUNA_APP_KEY")
countries = ['gb']
job_titles = ['data engineer', 'data scientist', 'data analyst', 'software engineer']
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def main_request(country: str, job_title: str, page = 1, max_results = 50) -> dict:
    ''' 
    Makes initial requests, 
    '''
    url = f"https://api.adzuna.com/v1/api/jobs/{country}/search/{page}"
    params = {
    "app_id" : app_id,
    "app_key" : app_key,
    "what" : job_title,
    "results_per_page" : max_results
    }
    try:
        r = requests.get(url, params=params)
        if r.status_code != 200:
            logger.error(f"Error encounterd {r.status_code}: {r.text}")
            return {"count" : 0, "results" :[]}
    except Exception as e:
        raise CustomException(e, sys)
    return r.json()


def get_total_pages(response, page_cap = 10) -> int:
    
    total = response['count']
    number_of_pages = math.ceil(total/50)
    # Adzuna caps a seary search for the first 10 pages, 
    cap = min(number_of_pages, page_cap)
    return cap


def fetch_jobs(country : str, job_title : str) -> dict:
    
    jobslist = []
    
    initial_data = main_request(country=country, job_title=job_title)
    total_pages = get_total_pages(initial_data)
    for page in range(1, total_pages + 1):
        results = main_request(country=country, job_title=job_title, page=page)
        logger.info(f"Fetching Page {page}/{total_pages} for {job_title} in {country}")
        jobslist.extend(results["results"])
        time.sleep(1)
    
    return jobslist


def save_raw(jobs, country : str, job_title: str):
    
    job_title = job_title.rstrip()
    country = country.strip()
    
    with open(os.path.join(BASE_DIR, "raw", f"{job_title.replace(" ", "")}_{country}.json"), mode="w", encoding="utf-8") as f:
        payload = {
            "job_title_short" : job_title,
            "country" : country,
            "jobs" : jobs
        }
        json.dump(payload, f, indent=4)

def run_extraction(countries : list, job_titles : list):
    
    for country in countries:
        for job_title in job_titles:
            jobs = fetch_jobs(country=country, job_title=job_title)
            save_raw(jobs, country=country, job_title=job_title)
            logger.info(f'Saved {len(jobs)} jobs for {job_title} in {country}')

    
    
    
    


if __name__ == "__main__":
   run_extraction(countries=countries, job_titles=job_titles)
   
    
    