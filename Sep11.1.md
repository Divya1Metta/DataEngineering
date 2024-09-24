
# Data processing

# Create a spark dataframe
data = [("John",25),("Jane",30),("Sam",22)]
df = spark.createDataFrame(data, ["Name","Age"])

display(df)
     
Name	Age
John	25
Jane	30
Sam	22



# Display the data as a bar chart
df.groupBy("Age").count().display()
     
Age	count
25	1
30	1
22	1

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
df_filtered = df.filter(col("ARR_DELAY")>=0)
df_filtered.show()

# Aggregation and Summary statistics
df_grouped = df.groupBy("CARRIER").agg(F.avg("ARR_DELAY").alias("average_delay"))
df_grouped.show()

df.select(F.min("ARR_DELAY"), F.max("ARR_DELAY"), F.mean("ARR_DELAY")).show()

# Data visualization
flight_count = df_filtered.groupBy("CARRIER").count()

flight_count_pd = flight_count.toPandas()
display(flight_count_pd)




     
+----------+-------+------+----+---------+---------+
|   FL_DATE|CARRIER|ORIGIN|DEST|DEP_DELAY|ARR_DELAY|
+----------+-------+------+----+---------+---------+
|2023-09-01|     AA|   ATL| DFW|        5|       10|
|2023-09-01|     UA|   LAX| JFK|       -3|        0|
|2023-09-01|     DL|   SFO| ORD|        7|       15|
|2023-09-02|     AA|   DFW| LAX|        0|       -5|
|2023-09-02|     UA|   JFK| ATL|       -2|        0|
|2023-09-02|     DL|   ORD| LAX|       20|       30|
|2023-09-03|     AA|   LAX| SFO|       10|       12|
|2023-09-03|     UA|   ATL| ORD|        0|      -10|
|2023-09-03|     DL|   SFO| JFK|        5|       25|
|2023-09-04|     AA|   JFK| LAX|        0|        0|
+----------+-------+------+----+---------+---------+
only showing top 10 rows

+----------+-------+------+----+---------+---------+
|   FL_DATE|CARRIER|ORIGIN|DEST|DEP_DELAY|ARR_DELAY|
+----------+-------+------+----+---------+---------+
|2023-09-01|     AA|   ATL| DFW|        5|       10|
|2023-09-01|     UA|   LAX| JFK|       -3|        0|
|2023-09-01|     DL|   SFO| ORD|        7|       15|
|2023-09-02|     UA|   JFK| ATL|       -2|        0|
|2023-09-02|     DL|   ORD| LAX|       20|       30|
|2023-09-03|     AA|   LAX| SFO|       10|       12|
|2023-09-03|     DL|   SFO| JFK|        5|       25|
|2023-09-04|     AA|   JFK| LAX|        0|        0|
|2023-09-04|     UA|   ORD| ATL|       15|       20|
|2023-09-05|     AA|   LAX| JFK|       20|       25|
|2023-09-05|     UA|   DFW| ATL|        0|        0|
|2023-09-05|     DL|   JFK| LAX|       10|       15|
+----------+-------+------+----+---------+---------+

+-------+-------------+
|CARRIER|average_delay|
+-------+-------------+
|     UA|          2.0|
|     AA|          8.4|
|     DL|         15.0|
+-------+-------------+

+--------------+--------------+-----------------+
|min(ARR_DELAY)|max(ARR_DELAY)|   avg(ARR_DELAY)|
+--------------+--------------+-----------------+
|           -10|            30|8.466666666666667|
+--------------+--------------+-----------------+

CARRIER	count
UA	4
AA	4
DL	4
