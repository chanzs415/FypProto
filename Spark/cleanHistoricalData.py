from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, approx_count_distinct
from pyspark.sql.types import DoubleType

import sys

if len(sys.argv) < 2:
    print("Please provide the input file path as a command-line argument.")
    sys.exit(1)

# Get the input file path from command-line argument
input_file_path = sys.argv[1]

# Create a SparkSession
spark = SparkSession.builder.appName("Data Cleaning").getOrCreate()

# Read the dataset
df = spark.read.csv(input_file_path, header=True, inferSchema=True)

# Dropping unrelated columns
if 'Unnamed: 0' in df.columns:
    df = df.drop('Unnamed: 0')

###################################### DUPLICATES ############################################
# Check for duplicates
print(f"Checking duplicates in the dataset...")
duplicates = df.dropDuplicates().count() - df.count()

if duplicates > 0:
    print(f"There are {duplicates} duplicates in the dataset.")
    # Drop the duplicates
    df = df.dropDuplicates()
    print(f"Cleaned duplicates.")
else:
    print(f"No duplicates found.")

###################################### MISSING ############################################
# Checking missing columns
print(f"Checking missing values in the dataset...")
missing_values = df.select([mean(col(c)).alias(c) for c in df.columns if c in ['int', 'double']]).collect()[0].asDict()

# Fill missing values with the mean for numeric columns and mode for categorical columns
for col_name, value in missing_values.items():
    if isinstance(value, (float, int)):
        df = df.withColumn(col_name, col(col_name).cast(DoubleType()).fillna(value))
    else:
        mode_value = df.select(approx_count_distinct(col(col_name), rsd=0.01).alias(col_name)).orderBy(col(col_name)).limit(1).collect()[0][0]
        df = df.fillna({col_name: mode_value})

# Check again for missing values
missing_cols = [c for c in df.columns if c in ['int', 'double']]
missing_data = df.select([mean(col(c)).alias(c) for c in missing_cols]).rdd.flatMap(lambda x: x).collect()
if not missing_data:
    print(f"No columns with missing data of types 'int' or 'double'.")
elif missing_data[0] is None:
    print(f"Failed to clean datasets.")
else:
    print(f"Successfully cleaned data.")

if input_file_path == "/app/data.csv":
    df = df.toDF(*['Location', 'Last Updated', 'Temperature (C)', 'Temperature (F)', 'Wind (km/hr)','Wind direction (in degree)', 'Wind direction (compass)', 'Pressure (millibars)','Precipitation (mm)', 'Humidity', 'Cloud Cover', 'UV Index', 'Wind gust (km/hr)'])
    # Save cleaned data to a new CSV file
    df.write.option("header", True).csv('/usr/local/output2', mode='overwrite')
else:
    # Save cleaned data to a new CSV file
    df.write.option("header", True).csv('/usr/local/output', mode='overwrite')

print(f"Cleaned data")
