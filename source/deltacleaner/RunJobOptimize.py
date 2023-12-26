from cleaner.optmize import OptimizeJob
from configure_logging import LoggingConfigurator
from pyspark.sql import SparkSession

if __name__ == "__main__":
    # Configure logging
    logger = LoggingConfigurator()
    logger.configure_logging()
    
    # Initialize SparkSession
    spark = SparkSession.builder.appName("DeltaTableCleanerOptimize").getOrCreate()
 
    # Initialize and run the OptimizeJob
    # Assuming the same config file can be used for both optimize and vacuum jobs 
    optimize_job = OptimizeJob(spark)
    # Execute the optimize job
    optimize_job.run_parallel_optimize()
