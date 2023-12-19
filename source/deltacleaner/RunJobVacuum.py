
from pyspark.sql import SparkSession
from cleaner.vacuum import VacuumJob
from configure_logging import LoggingConfigurator

if __name__ == "__main__":
    # Configure logging
    logger = LoggingConfigurator()
    logger.configure_logging()

    # Initialize SparkSession
    spark = SparkSession.builder.appName("DeltaTableCleanerVacuum").getOrCreate()
    # Set configuration for Delta vacuum job
    spark.conf.set('spark.databricks.delta.vacuum.parallelDelete.enabled', 'true')
 
    # Initialize and run the VacuumJob
    # Assuming the same config file can be used for both optimize and vacuum jobs
    vacuum_job = VacuumJob(spark)
    # Execute the vacuum job
    vacuum_job.run_parallel_vacuum()