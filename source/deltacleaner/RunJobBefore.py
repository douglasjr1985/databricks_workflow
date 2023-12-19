
from pyspark.sql import SparkSession
from metrics.before import DeltaTableMetricsCollectorBefore
from configure_logging import LoggingConfigurator

if __name__ == "__main__":
    # Configure logging settings
    logger = LoggingConfigurator()
    logger.configure_logging()

    # Initialize SparkSession for the DeltaTableMetricsCollectorBefore
    spark = SparkSession.builder.appName("DeltaTableMetricsCollectorBefore").getOrCreate()
    
    config_file_path = "config/param.json"

    # Create an instance of DeltaTableMetricsCollectorBefore and execute the metric collection
    collector = DeltaTableMetricsCollectorBefore(spark,config_file_path)
    collector.collect_metrics()
