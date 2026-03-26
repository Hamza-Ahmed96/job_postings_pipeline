from pathlib import Path
import snowflake.connector
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
import os
import json
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
import sys
sys.path.append(BASE_DIR)
from utils.useful_functions import handle_exceptions
folder_path = Path(BASE_DIR + "/raw")
from dotenv import load_dotenv
from loguru import logger
from datetime import datetime


@handle_exceptions
def get_jobs() -> list:

    jobs_list = []
    for file_path in folder_path.glob('*.json'):
        with open(file_path, mode="r", encoding='utf-8') as job_file:
            data = json.load(job_file)
            for job in data["jobs"]:
                job["job_title_short"] = data["job_title_short"]
                jobs_list.append(job)
    logger.info("Sucessfully extracted jobs from JSON files")
    return jobs_list

@handle_exceptions
def extract_job_fileds_from_raw(jobs : list) -> list:

    ids = []
    titles = []
    short_titles = []
    company_names = []
    locations = []
    posted_dates = []
    contract_types = []
    salaries = []
    descriptions = []

    for job in jobs:
        ids.append(job.get("id"))
        titles.append(job.get("title"))
        short_titles.append(job.get("job_title_short"))
        company_names.append(job.get("company", {}).get("display_name"))
        locations.append(job.get("location", {}).get("display_name"))
        posted_dates.append(job.get("created"))
        contract_types.append(job.get("contract_time"))
        salaries.append((job.get("salary_min"), job.get("salary_max")))
        descriptions.append(job.get("description"))

    logger.info("Sucessfully extracted job fields")
    return [
        {
        "id" : id,
        "title" : title,
        "job_title_short" : job_short_title,
        "company_name" : company_name,
        "location" : location,
        "job_posted_date" : posted_date,
        "contract_type" : contract_type,
        "salary_min" : salary[0],
        "salary_max" : salary[1],
        "description" : description,
        "load_date" : datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
    }
    for id, title, job_short_title, company_name, location, posted_date, contract_type, salary, description
    in zip(ids, titles, short_titles, company_names, locations, posted_dates, contract_types, salaries, descriptions)
    ]

@handle_exceptions
def get_connection() -> snowflake.connector.SnowflakeConnection:
    load_dotenv()
    conn = snowflake.connector.connect(
        user = os.getenv('user'),
        account = os.getenv('account'),
        password = os.getenv('password'),
        warehouse = os.getenv('warehouse'),
        database = os.getenv('database'),
        schema = os.getenv('schema')
    )
    logger.info("Sucessfully established Snowflake connection")
    return conn

@handle_exceptions
def create_raw_job_postings_table(conn : snowflake.connector.SnowflakeConnection):
    curr = conn.cursor()
    curr.execute(
        """
        CREATE TABLE IF NOT EXISTS RAW.JOB_POSTINGS(
            id VARCHAR PRIMARY KEY,
            job_title_short VARCHAR,
            title VARCHAR,
            job_posted_date DATETIME,
            company_name VARCHAR,
            location VARCHAR,
            salary_min FLOAT,
            salary_max FLOAT,
            contract_type VARCHAR,
            description VARCHAR,
            load_date DATETIME
        );
        """
    )
    logger.info("Sucessfully created raw.job_postings table")

@handle_exceptions
def insert_into_raw_job_postings(conn: snowflake.connector.SnowflakeConnection, jobs):
    curr = conn.cursor()
    curr.execute("TRUNCATE TABLE IF EXISTS RAW.JOB_POSTINGS")
    df = pd.DataFrame(jobs).drop_duplicates(subset="id").reset_index(drop=True)
    df["id"] = df["id"].astype(str)
    write_pandas(conn, df, table_name="JOB_POSTINGS", schema="RAW", quote_identifiers=False)
    logger.info("Sucessfully inserted job fields into raw.job_postings table")
    curr.close()

def tests(data, conn):
    curr = conn.cursor()
    # Test : number of rows from RAW.JOB_POSTINGS == lenght of data uploaded
    curr.execute("SELECT COUNT(*) FROM RAW.JOB_POSTINGS")
    rows = curr.fetchone()
    number_of_rows = rows[0]
    assert len(data) == number_of_rows
    logger.info("Tests ran sucessfully")

def run_load():
    conn = get_connection()
    jobs = get_jobs()
    data = extract_job_fileds_from_raw(jobs)
    create_raw_job_postings_table(conn)
    insert_into_raw_job_postings(conn, data)
    deduped_data = pd.DataFrame(data).drop_duplicates(subset="id").to_dict("records")
    tests(data=deduped_data, conn=conn)
    logger.info("All Load Processes Ran Sucessfully")

if __name__ == "__main__":
    run_load()
