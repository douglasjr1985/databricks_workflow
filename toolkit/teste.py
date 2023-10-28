import logging
import json
from databricks_job_manager.databricks_job_manager  import DatabricksJobManager

#from job_config_reader import get_job_config

def get_job_config(job_name):
    try:
        with open(f'resources/{job_name}.json', 'r') as config_file:
            job_config = json.load(config_file)
        return job_config
    except FileNotFoundError:
        logging.error("File not found.")
    except (IOError, Exception) as e:
        logging.error(f"An unexpected error occurred: {e}")
    return None

def main():
    # Logging configuration
    logging.basicConfig(filename='log_file.log', level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    workspace_url = "dock-tech-hml.cloud.databricks.com"
    api_token = "dapif67b502316790060ca5c6de62f0e13b2"
    job_name = "job_teste2"  # Replace with your job name

    job_config = get_job_config(job_name)

    if job_config:
        job_manager = DatabricksJobManager(workspace_url, api_token)
        job_manager.create_or_replace_job(job_name, job_config)
    else:
        logging.error('Unable to create or update the job due to previous errors.')

if __name__ == '__main__':
    main()