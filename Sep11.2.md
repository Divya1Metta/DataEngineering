# Create a spark dataframe
data = [("John",25),("Jane",30),("Sam",22)]
df = spark.createDataFrame(data, ["Name","Age"])

display(df)
     

# Display the data as a bar chart
df.groupBy("Age").count().display()
     

# Read streaming data from a socket (simulated source)
lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

# Split the lines into words
words = lines.selectExpr("explode(split(value,' ')) as word")

# Count the number of words
wordCounts = words.groupBy("word").count()

# Start the streaming query to console
query = wordCounts.writeStream.outputMode("complete").format("console").start()

# Await termination
query.awaitTermination()
     

from pyspark.sql.functions import col
from pyspark.sql import functions as F

# Load the data set
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:/Workspace/Users/azuser2121_mml.local@techademy.com/Shared/data.csv")

df.show(10)

# Data Cleaning
df = df.dropna()
df_filtered = df.filter(col("ARR_DELAY")<=0)
df_filtered.show()

# Aggregation and Summary statistics
df_grouped = df.groupBy("CARRIER").agg(F.avg("ARR_DELAY").alias("average_delay"))
df_grouped.show()

df.select(F.min("ARR_DELAY"), F.max("ARR_DELAY"), F.mean("ARR_DELAY")).show()

# Data visualization
flight_count = df_filtered.groupBy("CARRIER").count()

flight_count_pd = flight_count.toPandas()
display(flight_count_pd)
