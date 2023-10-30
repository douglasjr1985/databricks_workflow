import json
import logging
from databricks_cli.jobs.api import JobsApi
from databricks_cli.sdk.api_client import ApiClient

class DatabricksJobManager:

    def __init__(self, workspace_url, api_token):
        self.host = f"https://{workspace_url}/"
        self.token = api_token
        self.jobs_api = JobsApi(ApiClient(host=self.host, token=self.token))
        

    def _find_job_id(self, job_name):
        try:
            jobs = self.jobs_api.list_jobs()
            for _, items in jobs.items():
                for job in items:
                    if job['settings']['name'] == job_name:
                        return job['job_id']

        except Exception as e:
            logging.error(f"An error occurred while finding the job ID: {e}")
        return None

    def create_or_replace_job(self, job_name, job_config):
        job_id = self._find_job_id(job_name)

        if job_id is not None:
            new_settings = job_config.get('new_settings', job_config)
            request_body = {
                'job_id': job_id,
                'new_settings': new_settings
            }
            logging.info(f"JOB_ID {job_id} has been updated")
            self.jobs_api.reset_job(request_body)
        else:
            self.jobs_api.create_job(job_config)
            logging.info('Job created')
