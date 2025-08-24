import os
import sys
import psycopg2
from pyspark.sql import SparkSession

def create_spark_session():
    return SparkSession.builder \
        .appName("RidesLoad") \
        .config("spark.jars", "file:///Users/samiryadav/Desktop/postgresql-42.7.7.jar") \
        .config("spark.driver.extraClassPath", "/Users/samiryadav/Desktop/postgresql-42.7.7.jar") \
        .getOrCreate()


def create_postgres_tables():
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(
            dbname="learning_analytics", user="postgres", password="postgresql", host="localhost", port="5432"
        )
        cursor = conn.cursor()
        
        # Stage 1
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS rides_cleaned (
            booking_id VARCHAR(50) PRIMARY KEY,
            booking_timestamp TIMESTAMP,
            booking_status TEXT,
            customer_id VARCHAR(50),
            vehicle_type TEXT,
            pickup_location TEXT,
            drop_location TEXT,
            avg_vtat FLOAT,
            avg_ctat FLOAT,
            cancelled_rides_by_customer INT,
            reason_for_cancelling_by_customer TEXT,
            cancelled_rides_by_driver INT,
            driver_cancellation_reason TEXT,
            incomplete_rides INT,
            incomplete_rides_reason TEXT,
            booking_value FLOAT,
            ride_distance FLOAT,
            driver_ratings FLOAT,
            customer_rating FLOAT,
            payment_method TEXT
        );
        """)
        # Stage 2
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS aggregated_rides (
            vehicle_type TEXT PRIMARY KEY,
            total_rides INT,
            avg_booking_value FLOAT,
            avg_ride_distance FLOAT,
            avg_driver_rating FLOAT,
            avg_customer_rating FLOAT
        );
        """)
        # Stage 3
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS customer_summary (
            customer_id VARCHAR(50) PRIMARY KEY,
            total_rides INT,
            avg_booking_value FLOAT,
            avg_ride_distance FLOAT,
            avg_driver_rating FLOAT,
            total_cancellations INT
        );
        """)
        conn.commit()
        print("✅ PostgreSQL tables created successfully")
    except Exception as e:
        print(f"❌ Error creating tables: {e}")
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

def load_to_postgres(spark, input_dir):
    jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
    connection_properties = {"user":"postgres","password":"postgresql","driver":"org.postgresql.Driver"}

    # Stage 1
    df1 = spark.read.parquet(os.path.join(input_dir,"stage1","rides_cleaned"))
    print("Stage 1 rows:", df1.count())
    df1.write.mode("overwrite").jdbc(jdbc_url,"rides_cleaned",properties=connection_properties)

# Stage 2
    df2 = spark.read.parquet(os.path.join(input_dir,"stage2","customer_summary"))
    print("Stage 2 rows:", df2.count())
    df2.write.mode("overwrite").jdbc(jdbc_url,"customer_summary",properties=connection_properties)

    # Stage 3
    df3 = spark.read.parquet(os.path.join(input_dir,"stage3","aggregated_rides"))
    print("Stage 3 rows:", df3.count())
    df3.write.mode("overwrite").jdbc(jdbc_url,"aggregated_rides",properties=connection_properties)

    print("✅ Load stage completed")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python load_rides.py <transformed_data_dir>")
        sys.exit(1)

    input_dir = sys.argv[1]
    spark = create_spark_session()
    create_postgres_tables()
    load_to_postgres(spark, input_dir)
