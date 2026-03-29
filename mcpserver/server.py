import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from mcp.server.fastmcp import FastMCP
from dbt.cli.main import dbtRunner, dbtRunnerResult
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
def run_dbt_models(model: str = "stg_raw__job_postings") -> str:
    """
    Run dbt transformation models on Snowflake.
    Optionally pass a model name to run a specific model and all downstream models.
    Leave model empty to run all models.
    Example: model='stg_raw__job_postings' runs staging and everything downstream.
    """
    os.chdir(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "dbt_project"))

    if model:
        cmd_params = ["run", "--select", f"{model}+"]
    else:
        cmd_params = ["run"]

    dbt = dbtRunner()
    res: dbtRunnerResult = dbt.invoke(cmd_params)

    if res.success:
        return "dbt run completed successfully"
    else:
        return f"dbt run failed: {res.exception}"


@mcp.tool()
def run_full_pipeline_motherduck(countries: list[str], job_titles: list[str]) -> str:
    """
    Run the full ELT pipeline: fetch jobs from Adzuna and load them into MotherDuck.
    Use this when you want to do both steps in one go. Loads to MotherDuck only.
    Use fetch_jobs + load_to_motherduck separately if you want to load to Snowflake.
    """
    run_extraction(countries=countries, job_titles=job_titles)
    ingestion.load_duckdb.run_load()
    return f"Pipeline complete. Fetched and loaded jobs for {job_titles} in {countries}."

@mcp.tool()
def run_full_pipeline_snowflake(countries : list[str], job_titles: list[str]) -> str:
    """ 
    Run the full ELT pipeline : fetch jobs from Adzuna and load them to Snowflake.
    Use this when you want to do both steps in one go. Loads to Snowflake only.
    Use fetch_jobs+ load_to_snowflake.
    Use run_dbt_models() to run the dbt transformations on snowflake
    """
    run_extraction(countries=countries, job_titles=job_titles)
    ingestion.load_snowflake.run_load()
    run_dbt_models()
    return f"Pipeline completed. Fetched and loaded jobs for {job_titles} in {countries}. Transformed the raw data in snowflake with dbt models"




if __name__ == "__main__":
    mcp.run(transport='stdio')