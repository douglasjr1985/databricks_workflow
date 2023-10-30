import argparse
import logging
import json
from toolkit.databricks_job.job_manager  import DatabricksJobManager


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

    # Argument configuration
    parser = argparse.ArgumentParser(description='Process modified Jobs.')
    parser.add_argument('--workspace_url', type=str, help='Workspace URL')
    parser.add_argument('--client_secret', type=str, help='Client Secret')
    parser.add_argument('--filename', type=str, help='File Name')
    args = parser.parse_args()

    print(f'Processing job: {args.filename}') 

    workspace_url = args.workspace_url
    client_secret = args.client_secret
    job_name = args.filename  

    job_config = get_job_config(job_name)

    if job_config:
        job_manager = DatabricksJobManager(workspace_url, client_secret)
        job_manager.create_or_replace_job(job_name, job_config)
    else:
        logging.error('Unable to create or update the job due to previous errors.')


if __name__ == '__main__':
    main()