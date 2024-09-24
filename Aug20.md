use company

create table tblEmployee
( ID int not null primary key,
Name varchar(50),
Gender varchar(50),
Salary int,
DepartmentId int)

create table tblDepartment
(Id int,
DepartmentName varchar(50),
Location varchar(50),
DepartmentHead varchar(50))

insert into tblEmployee values
(1,'Tom','Male',4000,1),
(2,'Pam','Female',3000,3),
(3,'John','Male',3500,1),
(4,'Sam','Male',4500,2),
(5,'Todd','Male',2800,2),
(6,'Ben','Male',7000,1),
(7,'Sara','Female',4800,3),
(8,'Valarie','Female',5500,1),
(9,'James','Male',6500,NULL),
(10,'Russell','Male',8800,NULL)

insert into tblDepartment values
(1,'IT','London','Rick'),
(2,'Payroll','Delhi','Ron'),
(3,'HR','New York','Christie'),
(4,'Other Department','Sydney','Cindrella')

--joins

-- inner join
select Name, Gender, Salary, DepartmentName from tblEmployee
inner join tblDepartment 
on tblEmployee.DepartmentId = tblDepartment.ID

-- left outer join
select Name, Gender, Salary, DepartmentName from tblEmployee
left join tblDepartment 
on tblEmployee.DepartmentId = tblDepartment.ID

--Right outer join
select Name, Gender, Salary, DepartmentName from tblEmployee
right join tblDepartment 
on tblEmployee.DepartmentId = tblDepartment.ID

--Full outer join
select Name, Gender, Salary, DepartmentName from tblEmployee
Full outer join tblDepartment 
on tblEmployee.DepartmentId = tblDepartment.ID

create table Products 
(product_id int,
product_name varchar(50),
price decimal(10,2))

create table Orders
(order_id int,
product_id int,
quantity int,
order_date date)

insert into Products (product_id,product_name,price) values
(1,'Laptop',800.00),
(2,'Smartphone',500.00),
(3,'Tablet',300.00),
(4,'Headphones',50.00),
(5,'Monitor',150.00)

insert into Orders(order_id,product_id,quantity,order_date) values
(1,1,2,'2024-08-01'),
(2,2,1,'2024-08-02'),
(3,3,3,'2024-08-03'),
(4,1,1,'2024-08-04'),
(5,4,4,'2024-08-05'),
(6,5,2,'2024-08-06'),
(7,6,1,'2024-08-07')

--Inner join
select p.product_id, p.product_name, quantity, order_date from Products p
inner join Orders on p.product_id = Orders.product_id

--Left outer join
select p.product_id, p.product_name, quantity, order_date from Products p
left join Orders on p.product_id = Orders.product_id

--Right outer join
select p.product_id, p.product_name, quantity, order_date from Products p
right join Orders on p.product_id = Orders.product_id

--Full outer join
select p.product_id, p.product_name, quantity, order_date from Products p
full outer join Orders on p.product_id = Orders.product_id

--Grouping Sets
select p.product_name, o.order_date, SUM(o.quantity) as total_quantity
from Orders o join Products p on
o.product_id = p.product_id
group by grouping sets ((p.product_name),(o.order_date))

--Sub query
select o.order_id, o.product_id,
(select p.product_name from Products p where p.product_id = o.product_id) as product_name
from Orders o

select order_id, order_date, product_id
from Orders
where product_id in (select product_id from Products where price > 500)

--select u.user_id, u.user_name from Users u
--where EXISTS (select 1 from Orders o where o.user_id = u.user_id)

Select p.product_name, p.price
from Products p
where p.price > any (select price from Products where product_name like 'Laptop%');

--query to retrive users who have ordered products priced above 1000. The query uses two levels of nested subqueries.
 
 select u.user_name from Users where u.user_id in 
 (select user_id from Orders where product_id in 
 (select product_id from Products where price > 1000))

 -- Union 
 select product_name from Products where price > 500
 union
 select product_name from Products where product_name like 'Smart%'

 -- Intersection
 select product_name from Products where price > 500
 intersect
 select product_name from Products where product_name like 'Smart%'

 --Except
 select product_name from Products where price > 500
 except
 select product_name from Products where product_name like 'Smart%'

 CREATE TABLE Employees (
    employee_id INT PRIMARY KEY,
    employee_name VARCHAR(255),
    department VARCHAR(255),
    manager_id INT
);

CREATE TABLE Salaries (
    salary_id INT PRIMARY KEY,
    employee_id INT,
    salary DECIMAL(10, 2),
    salary_date DATE,
    FOREIGN KEY (employee_id) REFERENCES Employees(employee_id)
);



INSERT INTO Employees (employee_id, employee_name, department, manager_id) VALUES
(1, 'John Doe', 'HR', NULL),
(2, 'Jane Smith', 'Finance', 1),
(3, 'Robert Brown', 'Finance', 1),
(4, 'Emily Davis', 'Engineering', 2),
(5, 'Michael Johnson', 'Engineering', 2);

INSERT INTO Salaries (salary_id, employee_id, salary, salary_date) VALUES
(1, 1, 5000, '2024-01-01'),
(2, 2, 6000, '2024-01-15'),
(3, 3, 5500, '2024-02-01'),
(4, 4, 7000, '2024-02-15'),
(5, 5, 7500, '2024-03-01');

-- Equi join
-- Write a query to list all employees and their salaries using an equi join 
-- between the Employees and Salaries tables.

select e.employee_name,s.salary from Employees e inner join
Salaries s on e.employee_id = s.employee_id

-- Self join
-- Write a query to list each employee and their manager's name 
-- using a self join on the Employees table

select e1.employee_name, e2.manager_id from Employees e1
join Employees e2 on e1.employee_id = e2.employee_id

-- Group by with having
-- Write a query to calculate the average salary by department. 
-- Use GROUP BY and filter out departments where the average salary is below 6000.

select e.department, avg(s.salary) as AverageSalary from Employees e
join Salaries s on e.employee_id = s.employee_id
group by e.department having avg(s.salary) < 6000

-- Group by with grouping sets
-- Write a query using grouping sets to calculate the total salary by department 
-- and the overall total salary.

select e.department, sum(s.salary) as DepartmentSalary from Salaries s join Employees e on
s.employee_id = e.employee_id group by grouping sets ((department),())

-- Subqueries
-- Write a query to list all employees whose salary is above the average salary using a subquery

select e.employee_name from Employees e join Salaries s 
on e.employee_id = s.employee_id where s.salary > 
(select avg(salary) from Salaries)

-- Using Exists 
-- Write a query to list all employees who have received a salary in 2024 using the EXISTS keyword.

select e.employee_name from Employees e join Salaries s on 
e.employee_id = s.employee_id where Exists 
(select 1 from Salaries s where DATEPART(YEAR,s.salary_date)='2024' 
and s.employee_id = e.employee_id)

-- Using Any
-- Write a query to find employees whose salary is greater than the 
-- salary of any employee in the Engineering department

select e.employee_name from Employees e join Salaries s
on e.employee_id = s.employee_id where s.salary > any 
(select s1.salary from Salaries s1 join Employees e1 on 
s1.employee_id = e1.employee_id where e1.department = 'Engineering')

-- Using All
-- Write a query to find employees whose salary is greater than the 
-- salary of all employees in the Finance department

select e.employee_name from Employees e join Salaries s
on e.employee_id = s.employee_id where s.salary > ALL 
(select s1.salary from Salaries s1 join Employees e1 on 
s1.employee_id = e1.employee_id where e1.department = 'Finance')

-- Using Nested Subqueries
-- Write a query to list employees who earn more than the average 
-- salary of employees in the HR department using nested subqueries

select e.employee_name from Employees e join Salaries s
on e.employee_id = s.salary_id where s.salary > 
(select avg(s1.salary) from Salaries s1 join Employees e1
on s1.employee_id = e1.employee_id where e1.department = 'HR')

-- Using correlated Subqueries
-- Write a query to find employees whose salary is above the 
-- average salary for their respective department using a correlated subquery.

select * from Employees
select * from Salaries

select e.employee_name from Employees e join Salaries s 
on e.employee_id = s.employee_id where s.salary >
( select avg(s1.salary) from Salaries s1 join Employees e1
on s1.employee_id = e1.employee_id and 
e1.department = e.department)

-- Using Union
-- Write a query to list all employee names from the HR and Finance departments using UNION.

select employee_name from Employees where department = 'HR'
union
select employee_name from Employees where department = 'Finance'

-- Using Intersect
-- Write a query to list employees who have worked in both Finance and Engineering using INTERSECT.

select employee_name from Employees where department = 'Finance'
intersect
select employee_name from Employees where department = 'Engineering'

-- Using Except
-- Write a query to list employees who are in Finance but not in HR using EXCEPT.

select employee_name from Employees where department = 'Finance'
except
select employee_name from Employees where department = 'HR'

-- Using Merge

