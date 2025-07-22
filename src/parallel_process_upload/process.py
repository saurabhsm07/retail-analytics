from pyspark.sql.functions import col, rand, expr, concat, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import random
import time

# Import the Spark utilities
from utilities.spark_session import SparkManager

from utilities.log import get_logger

logger = get_logger(__name__)

def generate_dummy_dataframe(spark, num_rows=10000000):
    """Generate dummy DataFrame with specified columns"""
    logger.info(f"=== Step 1: Creating base DataFrame with {num_rows:,} rows ===")
    
    # Create base DataFrame with just IDs
    df_base = spark.range(1, num_rows + 1).toDF("id")
    logger.info(f"Base DataFrame partitions: {df_base.rdd.getNumPartitions()}")
    
    logger.info("Check Spark UI now - you should see the range() operation")
    time.sleep(5)  # Give time to check UI
    
    logger.info("=== Step 2: Adding name column ===")
    # Add name column with random names
    df_with_name = df_base.withColumn(
        "name", 
        concat(
            lit("User_"), 
            col("id").cast("string")
        )
    )
    
    logger.info("=== Step 3: Adding address column ===")
    # Add address column with random addresses
    df_with_address = df_with_name.withColumn(
        "address",
        concat(
            (rand() * 9999 + 1).cast("int").cast("string"),
            lit(" Main St, City "),
            (rand() * 100 + 1).cast("int").cast("string")
        )
    )
    
    logger.info("=== Step 4: Adding income column ===")
    # Add income column with random income values
    df_final = df_with_address.withColumn(
        "income",
        (rand() * 100000 + 30000).cast("double")
    )
    
    return df_final

def write_to_s3(df, s3_path, num_partitions=10):
    """Write DataFrame to S3 with specified number of partitions"""
    logger.info(f"=== Step 5: Repartitioning to {num_partitions} partitions ===")
    df_repartitioned = df.repartition(num_partitions)
    logger.info(f"After repartition - partitions: {df_repartitioned.rdd.getNumPartitions()}")
    
    logger.info("Check Spark UI - you should see repartition shuffle operation")
    time.sleep(3)
    
    logger.info(f"=== Step 6: Writing to S3: {s3_path} ===")
    df_repartitioned.write \
      .mode("overwrite") \
      .option("header", "true") \
      .csv(s3_path)

def main():
    # Configuration
    S3_OUTPUT_PATH = "s3a://my-bucket/test-data/"
    NUM_ROWS = 100000000
    NUM_PARTITIONS = 12
    
    # Create Spark session using the manager
    spark_manager = SparkManager("TestDataGenerator")
    spark = spark_manager.create_session()
    
    try:
        logger.info("Starting data generation process...")
        logger.info(f"Processing {NUM_ROWS:,} rows with {NUM_PARTITIONS} partitions")
        
        # Monitor initial executor state
        spark_manager.monitor_executors()
        
        # Generate dummy data
        df = generate_dummy_dataframe(spark, NUM_ROWS)
        
        logger.info("=== Step 3.5: Showing sample data (triggers computation) ===")
        logger.info("Check Spark UI - this will trigger the first job!")
        df.show(10)
        
        # Monitor after first job
        spark_manager.monitor_executors()
        
        logger.info("=== Step 4.5: Counting rows (triggers another job) ===")
        logger.info("Check Spark UI - this will be a separate job!")
        row_count = df.count()
        logger.info(f"Total rows: {row_count:,}")
        
        # Analyze partition distribution
        spark_manager.analyze_partitions(df, "Generated Data")
        
        # Repartition and write to S3
        logger.info("=== Final Step: Writing to S3 ===")
        write_to_s3(df, S3_OUTPUT_PATH, NUM_PARTITIONS)
        
        # Final monitoring
        spark_manager.monitor_executors()
        
        logger.info(f"Successfully written data to {S3_OUTPUT_PATH}")
        logger.info("Final check - look at the complete job history in Spark UI")
        
    except Exception as e:
        logger.error(f"Error occurred: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        # Use the manager to stop the session
        spark_manager.stop_session(keep_alive_seconds=30)

if __name__ == "__main__":
    main()