# Airline Flight Data Content

flight_id,airline,flight_number,origin,destination,departure_time,arrival_time,delay_min
1,Delta,DL123,JFK,LAX,08:00,11:00,30,3970,2023-07-01
2,United,UA456,SFO,ORD,09:30,15:00,45,2960,2023-07-01
3,Southwest,SW789,DAL,ATL,06:00,08:30,0,1150,2023-07-01
4,Delta,DL124,LAX,JFK,12:00,20:00,20,3970,2023-07-02
5,American,AA101,MIA,DEN,07:00,10:00,15,2770,2023-07-02
6,United,UA457,ORD,SFO,11:00,14:30,0,2960,2023-07-02
7,JetBlue,JB302,BOS,LAX,06:30,09:45,10,4180,2023-07-03
8,American,AA102,DEN,MIA,11:00,14:00,25,2770,2023-07-03
9,Southwest,SW790,ATL,DAL,09:00,11:00,5,1150,2023-07-03
10,Delta,DL125,JFK,SEA,13:00,17:00,0,3900,2023-07-04



## Airline Flight Data solutions


# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, max, count, col, when
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName('AirlineFlightData').getOrCreate()

# Load the dataset
data = spark.read.csv("/content/airline_flight_data.csv", header=True, inferSchema=True)

# Exercise 1
# Find the Total Distance Traveled by Each Airline
# Group the data by airline and calculate the total distance traveled for each airline
total_distance = data.groupBy("airline").agg(sum("distance").alias("total_distance"))
print("Total distance traveled by each airline:")
total_distance.show()

# Exercise 2
# Filter Flights with Delays Greater than 30 Minutes
# Filter the dataset to show only flights where the delay was greater than 30 minutes
delayed_flights = data.filter(data["delay_min"] > 30)
print("Flights with delays greater than 30 minutes:")
delayed_flights.show()

# Exercise 3
# Find the Flight with the Longest Distance
# Identify the flight that covered the longest distance
longest_flight = data.orderBy(col("distance").desc()).limit(1)
print("Flight with the longest distance:")
longest_flight.show()

# Exercise 4
# Calculate the Average Delay Time for Each Airline
# Group the data by airline and calculate the average delay time in minutes for each airline
avg_delay = data.groupBy("airline").agg(avg("delay_min").alias("average_delay"))
print("Average delay time for each airline:")
avg_delay.show()

# Exercise 5
# Identify Flights That Were Not Delayed
# Filter the dataset to show only flights with delay_minutes = 0
on_time_flights = data.filter(data["delay_min"] == 0)
print("Flights that were not delayed:")
on_time_flights.show()

# Exercise 6
# Find the Top 3 Most Frequent Routes
# Group the data by origin and destination to find the top 3 most frequent flight routes
routes = data.groupBy("origin", "destination").count().orderBy(col("count").desc()).limit(3)
print("Top 3 most frequent flight routes:")
routes.show()

# Exercise 7
# Calculate the Total Number of Flights per Day
# Group the data by date and calculate the total number of flights on each day
total_flights_per_day = data.groupBy("date").agg(count("flight_id").alias("total_flights"))
print("Total number of flights per day:")
total_flights_per_day.show()

# Exercise 8
# Find the Airline with the Most Flights
# Identify the airline that operated the most flights
most_flights_airline = data.groupBy("airline").agg(count("flight_id").alias("total_flights")).orderBy(col("total_flights").desc()).limit(1)
print("Airline with the most flights:")
most_flights_airline.show()

# Exercise 9
# Calculate the Average Flight Distance per Day
# Group the data by date and calculate the average flight distance for each day
avg_distance_per_day = data.groupBy("date").agg(avg("distance").alias("avg_distance"))
print("Average flight distance per day:")
avg_distance_per_day.show()

# Exercise 10
# Create a New Column for On-Time Status
# Add a new column called on_time that indicates whether a flight was on time (True if delay_minutes = 0, otherwise False)
data = data.withColumn("on_time", when(data["delay_min"] == 0, True).otherwise(False))
print("New column for on-time status:")
data.show()
