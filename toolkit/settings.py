import json
import logging

from databricks_cli.jobs.api import JobsApi
from databricks_cli.sdk.api_client import ApiClient
from databricks.sdk.runtime import dbutils, spark

# Logging configuration
logging.basicConfig(filename='log_file.log', level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Function to get the Jobs API
def get_jobs_api():
    try:
        # Get the URL from Spark configuration
        url = spark.conf.get("spark.databricks.workspaceUrl")
        host = f"https://{url}/"

        # Get the API token
        token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
        return JobsApi(ApiClient(host=host, token=token))
    except Exception as e:
        logging.error(f"Error obtaining the Jobs API: {e}")
        return None

# Function to get the job configuration
def get_job_config(job_name):
    try:
        # Open the JSON configuration file
        with open(f'resources/{job_name}.json', 'r') as config_file:
            job_config = json.load(config_file)
        return job_config
    except FileNotFoundError:
        logging.error("File not found.")
    except (IOError, Exception) as e:
        logging.error(f"An unexpected error occurred: {e}")
    return None

# Function to find the job ID
def find_job_id(job_api, job_name):
    try:
        jobs = job_api.list_jobs()
        for _, items in jobs.items():
            for job in items:
                if job['settings']['name'] == job_name:
                    return job['job_id']
    except Exception as e:
        logging.error(f"An error occurred while finding the job ID: {e}")
    return None

# Function to create or replace the job
def create_or_replace_job(job_name):
    job_api = get_jobs_api()
    job_config = get_job_config(job_name)

    if job_api is not None and job_config is not None:
        job_id = find_job_id(job_api, job_name)

        if job_id is not None:
            new_settings = job_config.get('new_settings', job_config)
            request_body = {
                'job_id': job_id,
                'new_settings': new_settings
            }
            logging.info(f"JOB_ID {job_id} has been updated")
            job_api.reset_job(request_body)
        else:
            job_api.create_job(job_config)
            logging.info('Job created')
    else:
        logging.error('Unable to create or update the job due to previous errors.')