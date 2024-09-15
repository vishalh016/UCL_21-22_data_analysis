from pyspark.sql import SparkSession

# Path to the JDBC driver
# jdbc_driver_path = "/lib/postgresql-42.7.4.jar"
#     .config("spark.jars", "D:/UCL_21-22/lib/postgresql-42.7.4.jar") \


# Initialize Spark session
spark = SparkSession.builder \
    .appName("Load Attacking CSV to PostgreSQL") \
    .getOrCreate()

# Define CSV file path (update with your actual path)
csv_file_path = "Data/attacking.csv"

# Read CSV file using Spark (efficient for larger datasets)
attacking_df_spark = spark.read.csv(csv_file_path, header=True, inferSchema=True)

# PostgreSQL database credentials
db_url = "jdbc:postgresql://localhost:5433/ucl21-22"  # Update with actual values
db_properties = {
    "user": "postgres",  # Update with your PostgreSQL username
    "password": "Jeet@6291",  # Update with your PostgreSQL password
    "driver": "org.postgresql.Driver"
}

# Write Spark DataFrame to PostgreSQL database
attacking_df_spark.write \
    .jdbc(url=db_url, table="public.attacking", mode="append", properties=db_properties)

# Stop Spark session
spark.stop()
