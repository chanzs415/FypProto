from pyspark.sql import SparkSession
from pyspark.sql.functions import isnan, when, count, col
from pyspark.sql.functions import col

# Create a SparkSession
spark = SparkSession.builder.appName("DataCleaning").getOrCreate()

# Load data from CSV file
df = spark.read.csv('/app/data.csv', header=True)

# Drop duplicates
df = df.dropDuplicates()

# Check for missing values and fill them
for col_name in df.columns:
    # Count number of missing values
    null_count = df.filter(col("`" + col_name + "`").isNull()).count()
    
    # Fill missing values
    if null_count > 0:
        df = df.withColumn(col_name, when(col("`" + col_name + "`").isNull(), 0).otherwise(col("`" + col_name + "`")))

        
# Check for outliers and remove them
df = df.filter((df['Temperature (C)'] >= 0) & (df['Temperature (C)'] <= 50) &
               (df['Wind (km/hr)'] >= 0) & (df['Wind (km/hr)'] <= 50) &
               (df['Pressure (millibars)'] >= 900) & (df['Pressure (millibars)'] <= 1100))

df = df.toDF(*['Location', 'Last Updated', 'Temperature (C)', 'Temperature (F)', 'Wind (km/hr)','Wind direction (in degree)', 'Wind direction (compass)', 'Pressure (millibars)','Precipitation (mm)', 'Humidity', 'Cloud Cover', 'UV Index', 'Wind gust (km/hr)'])

# Write cleaned data to a new CSV file
df.write.option("header", True).csv('/usr/local/output2', mode='overwrite')

# Stop the SparkSession
spark.stop()




