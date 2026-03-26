import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mcp.server.fastmcp import FastMCP

from ingestion.extract import run_extraction
import ingestion.load_duckdb
import ingestion.load_snowflake

mcp = FastMCP("adzuna-pipeline")

@mcp.tool()
def fetch_jobs(countries : list[str], job_titles : list[str]) -> str:
    """  
    Fetch job postings from the Adzuna API and save them as raw JSON files. 
    Use this when you want to extract fresh job data before loading to the database
    """
    run_extraction(countries=countries, job_titles=job_titles)
    return f"Fetched jobs for {job_titles} in countries {countries}"

@mcp.tool()
def load_to_motherduck()->str:
    """ 
    Load raw job postings JSON files inot MotherDuck database
    Use this after fetching jobs to persist the data into database.
    """
    ingestion.load_duckdb.run_load()
    return "Successfully loaded job postings into MotherDuck."

@mcp.tool()
def load_to_snowflake()->str:
    """ 
    Load raw job postings JSON files into Snowflake database
    Use this after fetching jobs to persist the data into database
    """
    ingestion.load_snowflake.run_load()
    return "Successfully loaded job postings into SnowFlake"
    

@mcp.tool()
def run_full_pipeline(countries: list[str], job_titles: list[str]) -> str:
    """
    Run the full ELT pipeline: fetch jobs from Adzuna and load them into MotherDuck.
    Use this when you want to do both steps in one go. Loads to MotherDuck only.
    Use fetch_jobs + load_to_snowflake separately if you want to load to Snowflake.
    """
    run_extraction(countries=countries, job_titles=job_titles)
    ingestion.load_duckdb.run_load()
    return f"Pipeline complete. Fetched and loaded jobs for {job_titles} in {countries}."

if __name__ == "__main__":
    mcp.run(transport='stdio')