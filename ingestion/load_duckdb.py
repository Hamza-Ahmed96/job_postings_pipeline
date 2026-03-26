from pathlib import Path
import duckdb
import pandas as pd
import os
import json
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
import sys
sys.path.append(BASE_DIR)
from utils.useful_functions import handle_exceptions
folder_path = Path(BASE_DIR + "/raw")
from dotenv import load_dotenv
from loguru import logger

@handle_exceptions
def get_connection() -> duckdb.DuckDBPyConnection:
    load_dotenv()
    conn = duckdb.connect("md:", config={"motherduck_token" : os.getenv("MOTHERDUCK_TOKEN")})
    logger.info("Sucessfully established Duckdb connection")
    return conn

@handle_exceptions
def get_jobs() -> list:
    jobs_list = []
    for file_path in folder_path.glob("*.json"):
        with open(file_path, mode='r', encoding='utf-8') as job_file:
            data = json.load(job_file)
            for job in data['jobs']:
                job['job_title_short'] = data['job_title_short']
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
        "posted_date" : posted_date,
        "contract_type" : contract_type,
        "salary_min" : salary[0],
        "salary_max" : salary[1],
        "description" : description
    }
    for id, title, job_short_title, company_name, location, posted_date, contract_type, salary, description
    in zip(ids, titles, short_titles, company_names, locations, posted_dates, contract_types, salaries, descriptions)
    ]

@handle_exceptions
def create_database(conn : duckdb.DuckDBPyConnection):
    conn.sql(" CREATE DATABASE IF NOT EXISTS data_jobs_adzuna; ")
    conn.sql("USE data_jobs_adzuna;")
    logger.info("Succesfully created data_jobs_adzuna database")

@handle_exceptions
def create_schema(conn : duckdb.DuckDBPyConnection):
    conn.sql(" CREATE SCHEMA IF NOT EXISTS data_jobs_adzuna.raw; ")
    logger.info("Succesfully created raw schema")

@handle_exceptions
def create_raw_job_postings_table(conn : duckdb.DuckDBPyConnection):
    conn.sql("""
             CREATE TABLE IF NOT EXISTS raw.job_postings(
                id VARCHAR PRIMARY KEY,
                job_title_short VARCHAR,
                title VARCHAR,
                job_posted_date TIMESTAMP,
                company_name VARCHAR,
                location VARCHAR,
                salary_min FLOAT,
                salary_max FLOAT,
                contract_type VARCHAR,
                description VARCHAR
             );
             """)
    logger.info("Sucesfully created raw jobs postings table ")
    
@handle_exceptions
def insert_into_raw_job_postings_table(conn : duckdb.DuckDBPyConnection, jobs : list):
    df = pd.DataFrame(jobs).drop_duplicates(subset="id")
    conn.register("jobs_df", df)
    conn.sql("TRUNCATE TABLE raw.job_postings")
    conn.sql("INSERT INTO raw.job_postings SELECT id, job_title_short, title, posted_date, company_name, location, salary_min, salary_max, contract_type, description FROM jobs_df")
    logger.info("Sucesfully inserted data into raw job postings table")

def tests(conn : duckdb.DuckDBPyConnection, data):
    number_of_rows = conn.sql("SELECT COUNT(*) FROM raw.job_postings;").fetchone()[0]
    assert number_of_rows == len(data)
    logger.info("Tests ran sucessfully")


def run_load():
    conn = get_connection()
    create_database(conn)
    create_schema(conn)
    create_raw_job_postings_table(conn)
    jobs = get_jobs()
    data = extract_job_fileds_from_raw(jobs=jobs)
    insert_into_raw_job_postings_table(conn, data)
    tests(conn=conn, data=data)
    conn.close()
    logger.info("Load ran succesfully")
    

if __name__ == "__main__":
    run_load()

    