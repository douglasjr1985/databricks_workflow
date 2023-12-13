import logging
import sys

from pyspark.sql import SparkSession
from config.vacuum import VacuumJob


def configure_logging():
    """Configures the logging settings."""
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, 
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

if __name__ == "__main__":

    # Configure logging
    configure_logging()
    
    spark = SparkSession.builder.appName("DeltaTableMaintenance").getOrCreate()
    spark.conf.set('spark.databricks.delta.vacuum.parallelDelete.enabled', 'true')
 
    # Assuming the same config file can be used for both optimize and vacuum jobs
    vacuum_job = VacuumJob(spark, "config/param.json")

    # Then run vacuum
    vacuum_job.run_parallel_vacuum()
