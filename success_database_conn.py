from pyspark.sql import SparkSession
import psycopg2  # Import the psycopg2 library for interacting with PostgreSQL


# Initialize Spark session
spark = SparkSession.builder \
    .appName("Load Attacking CSV to PostgreSQL") \
    .getOrCreate()

# Define CSV file path (update with your actual path)
csv_file_path = "Data/attacking.csv"

# Read CSV file using Spark (efficient for larger datasets)
attacking_df_spark = spark.read.csv(csv_file_path, header=True, inferSchema=True)

try:
    conn = psycopg2.connect(
        host="localhost", 
        database="ucl2122", 
        user="postgres", 
        password="Jeet@6291"
    )
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM attack_stat")
    results = cursor.fetchone()
    print(results)
except Exception as e:
    print(f"Error: {e}")
    print("Connection failed.")
    conn = None

finally:    
    conn.close()

    



# # PostgreSQL database credentials
# db_url = "jdbc:postgresql://localhost:5433/"  # Update with hostname/IP if needed
# db_properties = {
#     "user": "postgres",  # Update with your PostgreSQL username
#     "password": "Jeet@6291",  # Update with your PostgreSQL password
#     "driver": "org.postgresql.Driver"
# }

# # Function to check if database exists
# def check_and_create_database(db_url, db_name):
#     try:
#         conn = psycopg2.connect(db_url)
#         cursor = conn.cursor()
#         cursor.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{db_name}'")
#         exists = cursor.fetchone()
#         conn.close()
#     except Exception as e:
#         print(f"Error checking database: {e}")
#         return False
#     return exists is not None

# # Check if database exists, create it if not
# if not check_and_create_database(db_url, "ucl2122"):
#     try:
#         conn = psycopg2.connect(dbname="postgres", user=db_properties["user"], password=db_properties["password"])
#         cursor = conn.cursor()
#         cursor.execute(f"CREATE DATABASE ucl2122")
#         conn.commit()
#         conn.close()
#         print(f"Database 'ucl2122' created successfully.")
#     except Exception as e:
#         print(f"Error creating database: {e}")
#         exit(1)

# db_url=db_url + "ucl2122"
# # Write Spark DataFrame to PostgreSQL database (assuming database exists now)
# attacking_df_spark.write \
#     .jdbc(url=db_url + "/ucl2122", table="public.attacking", mode="append", properties=db_properties)

# Stop Spark session
spark.stop()
