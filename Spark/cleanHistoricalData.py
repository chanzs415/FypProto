from pyspark.sql.functions import udf, from_unixtime
from pyspark.sql.types import TimestampType
from pyspark.sql import Window
from pyspark.sql.functions import col, when, expr, date_format
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import mean, col, approx_count_distinct
from pyspark.sql.types import DoubleType
from pyspark.sql.types import StringType
import sys
from pyspark.sql import SparkSession

# create a SparkSession object
spark = SparkSession.builder.appName("CleanRTData").getOrCreate()

if len(sys.argv) < 2:
    print("Please provide the input file path as a command-line argument.")
    sys.exit(1)
# Get the input file path from command-line argument
input_file_path = sys.argv[1]

# Read the dataset
df = spark.read.csv(input_file_path, header=True, inferSchema=True)

# read the dataset into a DataFrame
#df = spark.read.csv("/app/data/historicalData_Malaysia_2023-03-01.csv", header=True, inferSchema=True)

# convert the 'time_epoch' column to a datetime object
#df = df.withColumn('time', from_unixtime(col('time_epoch')).cast(TimestampType()))
df = df.withColumn('time', date_format(from_unixtime(col('time_epoch')).cast(TimestampType()), 'yyyy-MM-dd HH:mm:ss'))

# drop the 'time_epoch' column
df = df.drop('time_epoch')

# filter the rows with inconsistent 'is_day' values
inconsistent_rows = df.filter(
    ((col('is_day') == 1) & ((col('hour') < 6) | (col('hour') > 18))) |
    ((col('is_day') == 0) & ((col('hour') >= 6) & (col('hour') <= 18)))
)

# update the inconsistent 'is_day' values
df = df.withColumn('is_day', when(col('is_day') == 1, 0).otherwise(1))

# Use string manipulation to extract the 'text' value from 'condition' column
clear_values = udf(lambda x: x.split("'text': '")[1].split("',")[0], StringType())
df = df.withColumn('condition', clear_values(col('condition')))

# Check the 'humidity' column for values outside the range of 0-100%, and correct any values that are out of range.
df = df.withColumn('humidity', when(col('humidity') < 0, 0).when(col('humidity') > 100, 100).otherwise(col('humidity')))

# Check the 'uv' column for values outside the range of 0-12, and correct any values that are out of range.
df = df.withColumn('uv', when(col('uv') < 0, 0).when(col('uv') > 12, 12).otherwise(col('uv')))

###################################### DUPLICATES ############################################
# Check for duplicates
print(f"Checking duplicates in the dataset...")
duplicates = df.dropDuplicates()

# Count the number of duplicates
num_duplicates = duplicates.count()

print(f"There are {num_duplicates} duplicates in the dataset.")

# Drop the duplicates
if num_duplicates > 0:
    print(f"Cleaning duplicates...")
    df = df.dropDuplicates()
    print(f"Clean.")
else:
    print(f"Done.")


###################################### MISSING ############################################
# Checking missing columns
# Checking missing columns
print("Checking missing values in the dataset...")
missing_values = df.select([mean(col(c)).alias(c) for c in df.columns if df.schema[c].dataType in [DoubleType(), "int"]]).collect()[0].asDict()
print("Calculating median for numeric columns...")
median_values = df.select([expr(f"percentile_approx(`{c}`, 0.5)").alias(c) for c in df.columns if df.schema[c].dataType in [DoubleType(), "int"]]).collect()[0].asDict()


  
# Fill missing values with the mean for numeric columns and mode for categorical columns
for col_name, value in missing_values.items():
    if isinstance(value, (float, int)):
        for col_name, value in median_values.items():
          df = df.fillna(value, subset=[col_name])
    else:
        mode_value = df.select(approx_count_distinct(col(col_name), rsd=0.01).alias(col_name)).orderBy(col(col_name)).limit(1).collect()[0][0]
        df = df.fillna(col_name, mode_value)



# Check again for missing values
missing_cols = [c for c in df.columns if df.schema[c].dataType in [DoubleType(), "int"]]
missing_data = df.select([mean(col(c)).alias(c) for c in missing_cols]).rdd.flatMap(lambda x: x).collect()
if not missing_data:
    print("No columns with missing data of types 'int' or 'double'.")
elif missing_data[0] is None:
    print("Failed to clean datasets.")
else:
    print("Successfully cleaned data.")

df = df.orderBy('time')

    # Save cleaned data to a new CSV file
df.write.mode("overwrite").csv("/usr/local/output", header=True)

spark.stop()
print("Cleaned data")