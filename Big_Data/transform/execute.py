import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql import functions as F

def create_spark_session():
    return SparkSession.builder \
        .appName("RidesTransform") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.sql.shuffle.partitions", "16") \
        .getOrCreate()

def load_and_transform(spark, input_dir, output_dir):
    """
    Load rides CSV(s), clean, handle nulls, transform, 
    and save Stage1, Stage2, and Stage3 Parquet files.
    """

    # -----------------------------
    # 1️⃣ Define schema
    # -----------------------------
    rides_schema = T.StructType([
        T.StructField("Date", T.StringType(), True),
        T.StructField("Time", T.StringType(), True),
        T.StructField("Booking ID", T.StringType(), True),
        T.StructField("Booking Status", T.StringType(), True),
        T.StructField("Customer ID", T.StringType(), True),
        T.StructField("Vehicle Type", T.StringType(), True),
        T.StructField("Pickup Location", T.StringType(), True),
        T.StructField("Drop Location", T.StringType(), True),
        T.StructField("Avg VTAT", T.FloatType(), True),
        T.StructField("Avg CTAT", T.FloatType(), True),
        T.StructField("Cancelled Rides by Customer", T.IntegerType(), True),
        T.StructField("Reason for cancelling by Customer", T.StringType(), True),
        T.StructField("Cancelled Rides by Driver", T.IntegerType(), True),
        T.StructField("Driver Cancellation Reason", T.StringType(), True),
        T.StructField("Incomplete Rides", T.IntegerType(), True),
        T.StructField("Incomplete Rides Reason", T.StringType(), True),
        T.StructField("Booking Value", T.FloatType(), True),
        T.StructField("Ride Distance", T.FloatType(), True),
        T.StructField("Driver Ratings", T.FloatType(), True),
        T.StructField("Customer Rating", T.FloatType(), True),
        T.StructField("Payment Method", T.StringType(), True)
    ])

    # -----------------------------
    # 2️⃣ Load CSV(s)
    # -----------------------------
    csv_path = os.path.join(input_dir, "*.csv")
    df = spark.read.csv(csv_path, header=True, schema=rides_schema)

    # -----------------------------
    # 3️⃣ Clean extra quotes
    # -----------------------------
    id_cols = ["Booking ID", "Customer ID"]
    for col in id_cols:
        df = df.withColumn(col, F.regexp_replace(F.col(col), '"', ''))

    # -----------------------------
    # 4️⃣ Handle nulls
    # -----------------------------
    string_cols = [c for c, t in df.dtypes if t == 'string']
    for col in string_cols:
        df = df.withColumn(col, F.coalesce(F.col(col), F.lit("Unknown")))

    numeric_cols = [c for c, t in df.dtypes if t in ('int', 'bigint', 'double', 'float')]
    for col in numeric_cols:
        df = df.withColumn(col, F.coalesce(F.col(col), F.lit(0)))

    # -----------------------------
    # 5️⃣ Stage 1: Cleaned Data
    # -----------------------------
    stage1_path = os.path.join(output_dir, "stage1", "rides_cleaned")
    os.makedirs(os.path.dirname(stage1_path), exist_ok=True)
    df = df.repartition(8).persist()
    df.write.mode("overwrite").parquet(stage1_path)
    print(f"✅ Stage 1: Cleaned rides data saved at {stage1_path}")
    print(f"Stage 1 row count: {df.count()}")

    # -----------------------------
    # 6️⃣ Stage 2: Customer Summary
    # -----------------------------
    customer_summary = df.groupBy("Customer ID").agg(
        F.count("Booking ID").alias("total_rides"),
        F.sum("Booking Value").alias("total_spent"),
        F.avg("Driver Ratings").alias("avg_driver_rating"),
        F.avg("Customer Rating").alias("avg_customer_rating"),
        F.sum("Cancelled Rides by Customer").alias("total_cancelled"),
        F.sum("Cancelled Rides by Driver").alias("total_driver_cancelled"),
        F.sum("Incomplete Rides").alias("total_incomplete")
    )

    stage2_path = os.path.join(output_dir, "stage2", "customer_summary")
    os.makedirs(os.path.dirname(stage2_path), exist_ok=True)
    customer_summary.write.mode("overwrite").parquet(stage2_path)
    print(f"✅ Stage 2: Customer summary saved at {stage2_path}")
    print(f"Stage 2 row count: {customer_summary.count()}")

    # -----------------------------
    # 7️⃣ Stage 3: Aggregated Rides by Vehicle Type & Booking Status
    # -----------------------------
    aggregated_rides = df.groupBy("Vehicle Type", "Booking Status").agg(
        F.count("Booking ID").alias("total_rides"),
        F.sum("Booking Value").alias("total_value"),
        F.avg("Ride Distance").alias("avg_distance"),
        F.avg("Avg VTAT").alias("avg_vtat"),
        F.avg("Avg CTAT").alias("avg_ctat")
    )

    stage3_path = os.path.join(output_dir, "stage3", "aggregated_rides")
    os.makedirs(os.path.dirname(stage3_path), exist_ok=True)
    aggregated_rides.write.mode("overwrite").parquet(stage3_path)
    print(f"✅ Stage 3: Aggregated rides saved at {stage3_path}")
    print(f"Stage 3 row count: {aggregated_rides.count()}")

    return df, customer_summary, aggregated_rides

# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python execute.py <input_dir> <output_dir>")
        sys.exit(1)

    input_dir = sys.argv[1]
    output_dir = sys.argv[2]

    spark = create_spark_session()
    load_and_transform(spark, input_dir, output_dir)
    print("✅ Transform stage completed")
