from pyspark.sql import SparkSession
import time

from utilities.log import get_logger

logger = get_logger(__name__)

class SparkManager:
    """Centralized Spark session management with monitoring capabilities"""
    
    def __init__(self, app_name="RetailAnalytics"):
        self.app_name = app_name
        self.spark = None
    
    def create_session(self, **configs):
        """Create Spark session with optional configurations"""
        builder = SparkSession.builder.appName(self.app_name)
        
        # Apply any additional configurations
        for key, value in configs.items():
            builder = builder.config(key, value)
        
        self.spark = builder.getOrCreate()
        self._print_configuration()
        return self.spark
    
    def _print_configuration(self):
        """Print current Spark configuration and executor status"""
        logger.info("=== Current Spark Configuration ===")
        logger.info(f"Application Name: {self.spark.sparkContext.appName}")
        logger.info(f"Executor instances: {self.spark.conf.get('spark.executor.instances')}")
        logger.info(f"Executor cores: {self.spark.conf.get('spark.executor.cores')}")
        logger.info(f"Executor memory: {self.spark.conf.get('spark.executor.memory')}")
        logger.info(f"Driver cores: {self.spark.conf.get('spark.driver.cores')}")
        logger.info(f"Max cores allowed: {self.spark.conf.get('spark.cores.max')}")
        logger.info(f"Default parallelism: {self.spark.sparkContext.defaultParallelism}")
        logger.info(f"Dynamic allocation: {self.spark.conf.get('spark.dynamicAllocation.enabled', 'false')}")
        
        # Print Spark UI URLs
        logger.info("=== Spark UI Access ===")
        logger.info(f"Application UI: {self.spark.sparkContext.uiWebUrl}")
        logger.info(f"Master UI: http://localhost:8080")
        logger.info("Open these URLs in your browser to monitor execution")
    
    def monitor_executors(self):
        """Monitor current executor status"""
        if not self.spark:
            logger.warning("No active Spark session")
            return
        
        # getExecutorInfos is not available in Spark 3.5.1.
        # Instead, we can use the REST API or SparkContext properties for basic info.
        sc = self.spark.sparkContext
        logger.info("=== Executor Status ===")
        logger.info(f"Application ID: {sc.applicationId}")
        logger.info(f"Master: {sc.master}")
        logger.info(f"Default Parallelism: {sc.defaultParallelism}")
        logger.info(f"Executor Memory: {self.spark.conf.get('spark.executor.memory')}")
        logger.info(f"Executor Cores: {self.spark.conf.get('spark.executor.cores')}")
        logger.info(f"Executor Instances: {self.spark.conf.get('spark.executor.instances')}")
        logger.info("For detailed executor info, visit the Spark UI at: " + (sc.uiWebUrl or "N/A"))
    
    def analyze_partitions(self, df, description="DataFrame"):
        """Analyze DataFrame partition distribution"""
        logger.info(f"=== {description} Partition Analysis ===")
        logger.info(f"Number of partitions: {df.rdd.getNumPartitions()}")
        
        partition_sizes = df.rdd.mapPartitions(lambda x: [sum(1 for _ in x)]).collect()
        logger.info(f"Partition sizes: {partition_sizes}")
        logger.info(f"Min partition size: {min(partition_sizes):,}")
        logger.info(f"Max partition size: {max(partition_sizes):,}")
        logger.info(f"Avg partition size: {sum(partition_sizes) / len(partition_sizes):,.0f}")
        
        # Check for data skew
        avg_size = sum(partition_sizes) / len(partition_sizes)
        skewed_partitions = [i for i, size in enumerate(partition_sizes) if abs(size - avg_size) > avg_size * 0.5]
        if skewed_partitions:
            logger.warning(f"Skewed partitions detected: {skewed_partitions}")
        else:
            logger.info("Partitions are well balanced")
    
    def stop_session(self, keep_alive_seconds=0):
        """Stop Spark session with optional keep-alive period"""
        # if keep_alive_seconds > 0:
        #     logger.info(f"Keeping Spark session alive for {keep_alive_seconds} seconds to examine UI...")
        #     time.sleep(keep_alive_seconds)
        
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")
        
    def get_session(self):
        """Get current Spark session"""
        return self.spark

# Convenience functions for quick access
def create_spark_session(app_name="RetailAnalytics", **configs):
    """Create a new Spark session with configurations"""
    manager = SparkManager(app_name)
    return manager.create_session(**configs), manager

def get_spark_context_info(spark):
    """Get basic Spark context information"""
    return {
        'app_name': spark.sparkContext.appName,
        'master': spark.sparkContext.master,
        'default_parallelism': spark.sparkContext.defaultParallelism,
        'ui_web_url': spark.sparkContext.uiWebUrl
    }