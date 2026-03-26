import json
import re
import os
import sys
from loguru import logger
from pathlib import Path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.my_exception import CustomException
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
folder_path = Path(BASE_DIR + "/raw")


def get_jobs() -> list:
    ''' retruns list of all jobs in the raw folder and appends them to a single list '''
    jobs = []
    try:
        for file_path in folder_path.glob('*.json'):
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                for job in data["jobs"]:
                    job["job_title_short"] = data["job_title_short"]
                    jobs.append(job)
    except Exception as e:
        raise CustomException(e, sys)
    logger.info(f"Files Extract from raw folder, containing {len(jobs)} jobs")
    return jobs

def calc_salary_avg(salary_min, salary_max) -> float:
    ''' Calculates average salary from minimum and maximum salary '''
    if (salary_min is None and salary_max is None):
        return None
    elif salary_min is None:
        salary_min = salary_max
    elif salary_max is None:
        salary_max = salary_min
    salary_year_avg = (salary_min + salary_max) / 2
    return salary_year_avg

def is_remote(description: str) -> bool:
    ''' Returns a boolean for if the job description contains remote, hybrid or work form home key words else False '''
    if description is None:
        return None
    elif re.search(r"remote|hybrid|work from home", description, re.IGNORECASE):
        return True
    else:
        return False

def transform_jobs_raw(jobs : list) ->list:
    ''' Returns a list of transformed fields from the raw jobs list in JSON files '''
    job_ids = []
    job_titles = []
    company_names = []
    job_locations = []
    salary_year_avges = []
    job_posted_dates = []
    job_schedule_types = []
    job_title_short = []
    job_work_from_homes = []
    
    try:
        for job in jobs:
            job_ids.append(job.get("id"))
            job_titles.append(job.get("title"))
            company_names.append(job.get("company", {}).get("display_name"))
            job_locations.append(job.get('location', {}).get('display_name'))
            job_title_short.append(job.get('job_title_short'))
            job_posted_dates.append(job.get('created'))
            job_schedule_types.append(job.get('contract_time'))
            job_work_from_homes.append(is_remote(job.get('description')))
            salary_year_avges.append(calc_salary_avg(salary_min=job.get('salary_min'), salary_max=job.get('salary_max')))
    except Exception as e:
        raise CustomException(e, sys)
    logger.info(f"Sucessfully extracted job details: {len(job_ids), len(job_titles), len(company_names), len(job_locations), len(salary_year_avges)}")
         
    return [
        {
        "job_id" : job_id,
        "job_title" : job_title,
        "job_posted_date" : job_posted_date,
        "company_name" : company_name,
        "job_location" : job_location,
        "salary_year_avg" : salary_year_avg,
        "job_schedule_type" : job_schedule_type,
        "job_work_from_home" : job_work_from_home
        }
        for job_id, job_title, job_posted_date, company_name, job_location, salary_year_avg, job_schedule_type, job_work_from_home
        in zip(job_ids, job_titles, job_posted_dates, company_names, job_locations, salary_year_avges, job_schedule_types, job_work_from_homes)
        ]
        

if __name__ == "__main__":
    jobs = get_jobs()
    transformed_jobs = transform_jobs_raw(jobs=jobs)
    transformed_jobs_df = pd.DataFrame(transformed_jobs)
    print(transformed_jobs_df.head(3))
