use company

-- Date Function Exercises

-- Calculate the number of months between your birthday and the current date.

select datediff(month,'2002-07-27',getdate()) as MonthsDifference

-- Retrieve all orders that were placed in the last 30 days.

select * from Orders where order_date > dateadd(day,-30,getdate())

-- Write a query to extract the year, month, and day from the current date.

select year(getdate()) as CurrentYear,month(getdate()) as CurrentMonth, Day(getdate()) as CurrentDate

-- Calculate the difference in years between two given dates.

select datediff(year,'2022-08-22',getdate()) as DifferenceYears

-- Retrieve the last day of the month for a given date.

select eomonth(getdate()) as LastDayOfMonth

-- String Function Exercises

-- Convert all customer names to uppercase.

select upper(customer_name) from Customers

-- Extract the first 5 characters of each product name.

select left(product_name,5) as First5Characters from Products

-- Concatenate the product name and category with a hyphen in between.

select concat(product_name,'-',product_id) from Products

-- Replace the word 'Phone' with 'Device' in all product names.

select product_name, REPLACE(product_name,'Phone','Device') from Products

-- Find the position of the letter 'a' in customer names.

select charindex(customer_name,'a') as PositionOfA from Customers

-- Aggregate Functions Exercises

-- Calculate the total sales amount for all orders.

select sum(o.quantity * p.price) as TotalSalesAmount from Products p join
Orders o on p.product_id = o.product_id

-- Find the average price of products in each category:

select category, avg(price) as AveragePrice from Products
group by category

-- Count the number of orders placed in each month of the year

select month(order_date) as OrderMonth, count(*) as OrderCount
from Orders group by month(order_date)

-- Find the maximum and minimum order quantities

select max(quantity) as MaxQuantity, min(quantity) as MinQuantity
from Orders

-- Calculate the sum of stock quantities grouped by product category

select category, sum(stock) as TotalStock from Products group by category

-- Join Exercises

-- Write a query to join the Customers and Orders tables to display customer names and their order details

select c.customer_name, o.order_id, o.quantity, o.price from Customers c
join Orders o on c.customer_id = o.customer_id

-- Perform an inner join between Products and Orders to retrieve product names and quantities sold

select p.product_name, sum(o.quantity) as Quantity from Products p join Orders o
on p.product_id = o.product_id group by p.product_name

-- Use a left join to display all products, including those that have not been ordered.

select p.product_name, o.quantity from Products p left join Orders o
on p.product_id = o.product_id

-- Write a query to join Employees with Departments and list employee names and their respective department names

select e.Name, d.DepartmentName from tblEmployee e join tblDepartment d on
e.DepartmentId = d.Id

-- Perform a self-join on an Employees table to show pairs of employees who work in the same department

select e1.DepartmentId, e1.Name, e2.Name from tblEmployee e1 join tblEmployee e2
on e1.DepartmentId = e2.DepartmentId where e1.ID < e2.ID

-- SubQueries

-- Write a query to find products whose price is higher than the average price of all products.

select product_name from Products 
where price > (select avg(price) from Products)

-- Retrieve customer names who have placed at least one order by using a subquery.

select u.user_id, u.user_name from Users u
where EXISTS (select 1 from Orders o where o.user_id = u.user_id)

-- Find the top 3 most expensive products using a subquery.

select top 3 product_name from (select product_name, price from Products 
order by price desc offset 0 rows) as t1

-- Write a query to list all employees whose salary is higher than the average salary of their department

select e.employee_name from Employees e join Salaries s
on e.employee_id = s.employee_id where s.salary >
( select avg(s1.salary) from Salaries s1 join Employees e1
on s1.employee_id = e1.employee_id and
e1.department = e.department)

-- Grouping and Summarizing

-- Group orders by customer and calculate the total amount spent by each customer.

select customer_id, sum(price) from Orders group by customer_id 

-- Group products by category and calculate the average price for each category.

select category, avg(price) from Products group by category

-- Group orders by month and calculate the total sales for each month.

select month(o.order_date) as Month, sum(o.quantity * p.price) as TotalSales from Orders o join
Products p on o.product_id = p.product_id group by month(o.order_date)

-- Write a query to group products by category and calculate the number of products in each category.

select category, count(*) as NoOfProducts from Products group by category

-- Use the HAVING clause to filter groups of customers who have placed more than 5 orders.

select customer_id, count(*) as NoOfOrders from Orders group by customer_id having count(*) > 5

-- Set Operations

-- Write a query to list all employee names from the HR and Finance departments using UNION.

select employee_name from Employees where department = 'HR'
union
select employee_name from Employees where department = 'Finance'

-- Find products that are in both the Electronics and Accessories categories using INTERSECT.

select product_name from Products where category = 'Electronics'
intersect
select product_name from Products where category = 'Accessories'

-- Write a query to find products that are in the Electronics category but not in the Furniture category using EXCEPT

select product_name from Products where category = 'Electronics'
except
select product_name from Products where category = 'Furniture'
