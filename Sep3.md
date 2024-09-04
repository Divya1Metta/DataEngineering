
# PySpark Tasks

# Tasks
# 1. **Join the DataFrames:** 
# Join the `product_df` and `sales_df` DataFrames on `ProductID` to create a combined DataFrame with product and sales data


product_sales_df = product_df.join(sales_df, on="ProductID")
product_sales_df.show()


# Calculate Total Sales Value:**
# For each product, calculate the total sales value by multiplying the price by the quantity sold.


total_sale_product_df = product_sales_df.withColumn("TotalSalesValue", col("Price") * col("Quantity"))
total_sale_product_df.show()


# Find the Total Sales for Each Product Category:**
# Group the data by the `Category` column and calculate the total sales value for each product category.
total_sale_category_df = total_sale_product_df.groupBy("Category").sum("TotalSalesValue").withColumnRenamed("sum(TotalSalesValue)","TotalSales")
total_sale_category_df.show()


# Identify the Top-Selling Product:**
# Find the product that generated the highest total sales value.
total_sale_productName = total_sale_product_df.groupBy("ProductName").sum("TotalSalesValue").withColumnRenamed("sum(TotalSalesValue)","TotalSales")
total_sale_productName.show()


sorted_product_Sale = total_sale_productName.orderBy(col("TotalSales").desc())
top_selling_product = sorted_product_Sale.limit(1)
top_selling_product.show()


# Sort the Products by Total Sales Value:**
# Sort the products by total sales value in descending order.
sorted_product_Sale.show()


# Count the Number of Sales for Each Product:**
# Count the number of sales transactions for each product.


sales_count_df = product_sales_df.groupBy("ProductID").count().withColumnRenamed("count","TransactionCount")
sales_count_df.show()


# Filter the Products with Total Sales Value Greater Than ₹50,000:**
# Filter out the products that have a total sales value greater than ₹50,000.


product_big_price_df = sorted_product_Sale.filter(col("TotalSales") > 50000)
product_big_price_df.show()
