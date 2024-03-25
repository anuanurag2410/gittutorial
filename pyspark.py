from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array_remove

# Create SparkSession
spark = SparkSession.builder \
    .appName("Remove Nulls from Array") \
    .getOrCreate()


#funtion feature added 


# Sample data
data = [(1, [10, None, 30, None, 50]),
        (2, [100, 200, None, 400]),
        (3, [None, 2, 3, None, 5, 6, None, 8, 9, None])]


print("hello")
# Create DataFrame
df = spark.createDataFrame(data, ["id", "num"])

# Remove null values from the "num" array column
df_no_nulls = df.withColumn("num", array_remove(col("num"), None))

# Show DataFrame
df_no_nulls.show(truncate=False)

# Stop SparkSession
spark.stop()
