from pyspark.sql import SparkSession
from metrics.after import DeltaTableMetricsCollectorAfter
from configure_logging import LoggingConfigurator

if __name__ == "__main__":
    # Configure logging settings
    logger = LoggingConfigurator()
    logger.configure_logging()

    # Initialize SparkSession for the DeltaTableMetricsCollectorAfter
    spark = SparkSession.builder.appName("DeltaTableMetricsCollectorAfter").getOrCreate()
    
    # Create an instance of DeltaTableMetricsCollectorAfter and execute the metric collection
    collector = DeltaTableMetricsCollectorAfter(spark)
    collector.collect_metrics()
