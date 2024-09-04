
# RDD Hands-on

sales_data = [
    ("ProductA", 100),
    ("ProductB", 150),
    ("ProductA", 200),
    ("ProductC", 300),
    ("ProductB", 250),
    ("ProductC", 100)
]


regional_sales_data = [
    ("ProductA", 50),
    ("ProductC", 150)
]


# Task 1: Create an RDD from the Sales Data**
#    - Create an RDD from the `sales_data` list provided above.
#    - Print the first few elements of the RDD.


rdd_sales = sc.parallelize(sales_data)
# print(rdd_sales.collect())


# Task 2: Group Data by Product Name**
#    - Group the sales data by product name using `groupByKey()`.
#    - Print the grouped data to understand its structure.


group_rdd = rdd_sales.groupByKey()
print(group_rdd.mapValues(list).collect())


# Task 3: Calculate Total Sales by Product**
#    - Use `reduceByKey()` to calculate the total sales for each product.
#    - Print the total sales for each product.


reduce_rdd = rdd_sales.reduceByKey(lambda x,y: x+y)
print(reduce_rdd.collect())


# Task 4: Sort Products by Total Sales**
#    - Sort the products by their total sales in descending order.
#    - Print the sorted list of products along with their sales amounts.


sort_rdd = rdd_sales.sortBy(lambda x: x[1], ascending = False)
print(sort_rdd.collect())


# Task 5: Filter Products with High Sales**
#    - Filter the products that have total sales greater than 200.
#    - Print the products that meet this condition.


high_sales = rdd_sales.filter(lambda x: x[1] > 200)
print(high_sales.collect())


# Task 6: Combine Regional Sales Data**
#    - Create another RDD from the `regional_sales_data` list.
#    - Combine this RDD with the original sales RDD using `union()`.
#    - Calculate the new total sales for each product after combining the datasets.
#    - Print the combined sales data.


rdd_regional = sc.parallelize(regional_sales_data)
combine_rdd = rdd_sales.union(rdd_regional)
combine_total_sales = combine_rdd.reduceByKey(lambda x,y: x+y)
print(combine_total_sales.collect())


# Task 7: Count the Number of Distinct Products**
#    - Count the number of distinct products in the RDD.
#    - Print the count of distinct products.


distinct_count = rdd_sales.map(lambda x: x[0]).distinct().count()
print(distinct_count)


# Task 8: Identify the Product with Maximum Sales**
#    - Find the product with the maximum total sales using `reduce()`.
#    - Print the product name and its total sales amount.


max_sales_product = reduce_rdd.reduce(lambda x, y: x if x[1] > y[1] else y)
print(f"Product: {max_sales_product[0]}, Sales: {max_sales_product[1]}")


# Challenge Task:**
#     - Calculate the average sales amount per product using the key-value pair RDD.
#     - Print the average sales for each product.


def calculate_average(data):
  total_sum = sum(data)
  count = len(data)
  return total_sum / count


average_sales = rdd_sales.groupByKey().mapValues(calculate_average)
print(average_sales.collect())
