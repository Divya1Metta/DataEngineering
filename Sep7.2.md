# 2. Book Sales Data Content

sale_id,book_title,author,genre,sale_price,quantity,date
1,The Catcher in the Rye,J.D. Salinger,Fiction,15.99,2,2023-01-05
2,To Kill a Mockingbird,Harper Lee,Fiction,18.99,1,2023-01-10
3,Becoming,Michelle Obama,Biography,20.00,3,2023-02-12
4,Sapiens,Yuval Noah Harari,Non-Fiction,22.50,1,2023-02-15
5,Educated,Tara Westover,Biography,17.99,2,2023-03-10
6,The Great Gatsby,F. Scott Fitzgerald,Fiction,10.99,5,2023-03-15
7,Atomic Habits,James Clear,Self-Help,16.99,3,2023-04-01
8,Dune,Frank Herbert,Science Fiction,25.99,1,2023-04-10
9,1984,George Orwell,Fiction,14.99,2,2023-04-12
10,The Power of Habit,Charles Duhigg,Self-Help,18.00,1,2023-05-01


## Book Sales Data Solutions

!pip install pyspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, max, countDistinct

# Initialize Spark session
spark = SparkSession.builder.appName('BookSales').getOrCreate()

# Load the data
data = spark.read.csv("/content/book_sales_data.csv", header=True, inferSchema=True)

# Exercise 1
# Find Total Sales Revenue per Genre
# Group the data by genre and calculate the total sales revenue for each genre.
total_revenue = data.groupBy("genre").agg(sum(data["sale_price"] * data["quantity"]).alias("total_revenue"))
print("Total sales revenue per genre")
total_revenue.show()

# Exercise 2
# Filter Books Sold in the "Fiction" Genre
# Filter the dataset to include only books sold in the "Fiction" genre.
fiction_books = data.filter(data["genre"] == "Fiction")
print("Books sold in the 'Fiction' genre")
fiction_books.show()

# Exercise 3
# Find the Book with the Highest Sale Price
# Identify the book with the highest individual sale price.
highest_sale_price = data.orderBy(data["sale_price"].desc()).limit(1)
print("Book with the highest sale price")
highest_sale_price.show()

# Exercise 4
# Calculate Total Quantity of Books Sold by Author
# Group the data by author and calculate the total quantity of books sold for each author.
total_books_sold = data.groupBy("author").agg(sum("quantity").alias("total_quantity"))
print("Total quantity of books sold by each author")
total_books_sold.show()

# Exercise 5
# Identify Sales Transactions Worth More Than $50
# Filter the sales transactions where the total sales amount (sale_price * quantity) is greater than $50.
high_value_sales = data.filter((data["sale_price"] * data["quantity"]) > 50)
print("Sales transactions worth more than $50")
high_value_sales.show()

# Exercise 6
# Find the Average Sale Price per Genre
# Group the data by genre and calculate the average sale price for books in each genre.
average_sale_price = data.groupBy("genre").agg(avg("sale_price").alias("avg_sale_price"))
print("Average sale price per genre")
average_sale_price.show()

# Exercise 7
# Count the Number of Unique Authors in the Dataset
# Count how many unique authors are present in the dataset.
unique_authors = data.select(countDistinct("author").alias("unique_authors"))
print("Number of unique authors in the dataset")
unique_authors.show()

# Exercise 8
# Find the Top 3 Best-Selling Books by Quantity
# Identify the top 3 best-selling books based on the total quantity sold.
top_3_books = data.groupBy("book_title").agg(sum("quantity").alias("total_quantity")).orderBy("total_quantity", ascending=False).limit(3)
print("Top 3 best-selling books by quantity")
top_3_books.show()

# Exercise 9
# Calculate Total Sales for Each Month
# Group the sales data by month and calculate the total sales revenue for each month.
from pyspark.sql.functions import month
monthly_sales = data.withColumn("month", month("date")).groupBy("month").agg(sum(data["sale_price"] * data["quantity"]).alias("total_revenue"))
print("Total sales per month")
monthly_sales.show()

# Exercise 10
# Create a New Column for Total Sales Amount
# Add a new column total_sales that calculates the total sales amount for each transaction (sale_price * quantity).
data = data.withColumn("total_sales", data["sale_price"] * data["quantity"])
print("New column for total sales amount")
data.show()
