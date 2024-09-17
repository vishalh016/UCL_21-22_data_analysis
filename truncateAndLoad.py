from pyspark.sql import SparkSession
import psycopg2
import pandas as pd

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Load Attacking CSV to PostgreSQL") \
    .getOrCreate()

# Define CSV file path (update with your actual path)
csv_file_path = "Data/attacking.csv"

# Read CSV file using Spark (efficient for larger datasets)
attacking_df_spark = spark.read.csv(csv_file_path, header=True, inferSchema=True)

# Convert Spark DataFrame to Pandas DataFrame
pandas_df = attacking_df_spark.toPandas()
print(pandas_df)
# sys.exit(0)
# Connect to PostgreSQL database
try:
    conn = psycopg2.connect(
        host="localhost",
        database="ucl2122",
        user="postgres",
        password="Jeet@6291"
    )
    cursor = conn.cursor()

    # Truncate the existing data in the table
    cursor.execute("TRUNCATE TABLE attack_stat RESTART IDENTITY CASCADE")
    conn.commit()
    print("Data TRUNCATED successfully!")
    # Insert data into PostgreSQL attack_stat table
    
except Exception as e:
    print(f"Error loading data: {e}")

finally:
    conn.close()

spark.stop()
