
# Week1

-- Exercises (Morning Session)

-- Calculate the total amount spent by each customer


```sql
SELECT c.customer_name, SUM(o.price) AS TotalSpent FROM Orders o JOIN Customers c ON
o.customer_id = c.customer_id GROUP BY c.customer_name;
```

-- Find the customers who have spent more that 1000 in total

```sql
SELECT c.customer_name, SUM(o.price) AS TotalSpent FROM Orders o JOIN Customers c ON
o.customer_id = c.customer_id GROUP BY c.customer_name HAVING SUM(o.price) > 1000;
```

-- Find Product categories with more than 5 products


```sql
SELECT Category, COUNT(*) AS ProductCount FROM Products
GROUP BY Category HAVING COUNT(*) > 5;
```

-- Calculate the total number of products for each category and supplier combination


```sql
SELECT Category, Supplier, COUNT(*) AS ProductCount FROM Products
GROUP BY Category, Supplier;
```

-- Summarize total sales by product and customer, and also provide an overall total


```sql
SELECT product_id, customer_id, SUM(price) AS TotalSales FROM Orders
GROUP BY product_id, customer_id;
UNION ALL
SELECT NULL AS product_id, NULL AS customer_id, SUM(price) AS TotalSales
FROM Orders;
```





-Hands-on excercise (afternoon session)


```

#### Insert Sample Data into `Products`:

```sql
INSERT INTO Products (ProductName, Category, Price, StockQuantity)
VALUES 
('Laptop', 'Electronics', 75000.00, 15),
('Smartphone', 'Electronics', 25000.00, 30),
('Desk Chair', 'Furniture', 5000.00, 10),
('Monitor', 'Electronics', 12000.00, 20),
('Bookshelf', 'Furniture', 8000.00, 8);
```

### **3. Table: `Orders`**

#### Create `Orders` Table:

```sql
CREATE TABLE Orders (
    OrderID INT PRIMARY KEY IDENTITY(1,1),
    CustomerID INT,
    ProductID INT,
    Quantity INT,
    TotalAmount DECIMAL(10, 2),
    OrderDate DATE,
    FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID),
    FOREIGN KEY (ProductID) REFERENCES Products(ProductID)
);
```

#### Insert Sample Data into `Orders`:

```sql
INSERT INTO Orders (CustomerID, ProductID, Quantity, TotalAmount, OrderDate)
VALUES 
(1, 1, 2, 150000.00, '2024-08-01'),
(2, 2, 1, 25000.00, '2024-08-02'),
(3, 3, 4, 20000.00, '2024-08-03'),
(4, 4, 2, 24000.00, '2024-08-04'),
(5, 5, 5, 40000.00, '2024-08-05');
```


1.Hands-on Exercise: Filtering Data using SQL Queries
Retrieve all products from the Products table that belong to the category 'Electronics' and have a price greater than 500.

```sql
SELECT * FROM Products
WHERE Category = 'Electronics' AND Price > 500;
```



2. Hands-on Exercise: Total Aggregations using SQL Queries
Calculate the total quantity of products sold from the Orders table.

```sql
SELECT SUM(Quantity) AS TotalQuantity
FROM Orders;
```

3. Hands-on Exercise: Group By Aggregations using SQL Queries
Calculate the total revenue generated for each product in the Orders table.

```sql
SELECT ProductID, SUM(TotalAmount) AS TotalRevenue
FROM Orders
GROUP BY ProductID;
```

4. Hands-on Exercise: Order of Execution of SQL Queries
Write a query that uses WHERE, GROUP BY, HAVING, and ORDER BY clauses and explain the order of execution.

```sql
SELECT ProductID, SUM(TotalAmount) AS TotalRevenue
FROM Orders
WHERE OrderDate > '2024-08-01'
GROUP BY ProductID
HAVING SUM(TotalAmount) > 100000
ORDER BY TotalRevenue DESC;
```
order of execution:

WHERE clause filters the data based on the condition
GROUP BY clause groups the data by ProductID
HAVING clause filters the grouped data based on the condition
ORDER BY clause sorts the result in descending order by TotalRevenue



5. Hands-on Exercise: Rules and Restrictions to Group and Filter Data in SQL Queries
Write a query that corrects a violation of using non-aggregated columns without grouping them.

```sql
SELECT ProductID, SUM(TotalAmount) AS TotalRevenue
FROM Orders
GROUP BY ProductID;
```

6. Hands-on Exercise: Filter Data based on Aggregated Results using Group By and Having
Retrieve all customers who have placed more than 5 orders using GROUP BY and HAVING clauses.

```sql
SELECT CustomerID, COUNT(OrderID) AS TotalOrders
FROM Orders
GROUP BY CustomerID
HAVING COUNT(OrderID) > 5;
```


1. Basic Stored Procedure
Create a stored procedure named GetAllCustomers that retrieves all customer details from the Customers table.

```sql
CREATE PROCEDURE GetAllCustomers
AS
BEGIN
    SELECT * FROM Customers;
END;
GO
```

2. Stored Procedure with Input Parameter
Create a stored procedure named GetOrderDetailsByOrderID that accepts an OrderID as a parameter and retrieves the order details for that specific order.

```sql
CREATE PROCEDURE GetOrderDetailsByOrderID
    @OrderID INT
AS
BEGIN
    SELECT * FROM Orders
    WHERE OrderID = @OrderID;
END;
GO
```

3. Stored Procedure with Multiple Input Parameters
Create a stored procedure named GetProductsByCategoryAndPrice that accepts a product Category and a minimum Price as input parameters and retrieves all products that meet the criteria.

```sql
CREATE PROCEDURE GetProductsByCategoryAndPrice
    @Category VARCHAR(50),
    @MinPrice DECIMAL(10, 2)
AS
BEGIN
    SELECT * FROM Products
    WHERE Category = @Category AND Price >= @MinPrice;
END;
GO
```

4. Stored Procedure with Insert Operation
Create a stored procedure named InsertNewProduct that accepts parameters for ProductName, Category, Price, and StockQuantity and inserts a new product into the Products table.

```sql
CREATE PROCEDURE InsertNewProduct
    @ProductName VARCHAR(100),
    @Category VARCHAR(50),
    @Price DECIMAL(10, 2),
    @StockQuantity INT
AS
BEGIN
    INSERT INTO Products (ProductName, Category, Price, StockQuantity)
    VALUES (@ProductName, @Category, @Price, @StockQuantity);
END;
GO
```

5. Stored Procedure with Update Operation
Create a stored procedure named UpdateCustomerEmail that accepts a CustomerID and a NewEmail parameter and updates the email address for the specified customer.

```sql
CREATE PROCEDURE UpdateCustomerEmail
    @CustomerID INT,
    @NewEmail VARCHAR(100)
AS
BEGIN
    UPDATE Customers
    SET Email = @NewEmail
    WHERE CustomerID = @CustomerID;
END;
GO
```

6. Stored Procedure with Delete Operation
Create a stored procedure named DeleteOrderByID that accepts an OrderID as a parameter and deletes the corresponding order from the Orders table.

```sql
CREATE PROCEDURE DeleteOrderByID
    @OrderID INT
AS
BEGIN
    DELETE FROM Orders
    WHERE OrderID = @OrderID;
END;
GO
```


7. Stored Procedure with Output Parameter
Create a stored procedure named GetTotalProductsInCategory that accepts a Category parameter and returns the total number of products in that category using an output parameter.

```sql
CREATE PROCEDURE GetTotalProductsInCategory
    @Category VARCHAR(50),
    @TotalProducts INT OUTPUT
AS
BEGIN
    SELECT @TotalProducts = COUNT(ProductID)
    FROM Products
    WHERE Category = @Category;
END;
GO
```