
use college

create table tblGender
(ID int Not Null Primary Key,
Gender nvarchar(50))

-- Foreign Key relations

alter table tblPerson
add constraint tblPerson_GenderId_FK foreign key
(GenderId) references tblGender(ID)

-- inserting values
-- Insert into gender table first cause it has a foreign key

insert into tblGender (ID, Gender)
values (1,'Male'),
(2,'Female'),
(3,'Others')

insert into tblPerson (Id, Name, Email, GenderId)
values (1,'Tom','Tom@gmail.com',1),
(2,'Jessy','Jessy@gmail.com',2),
(3,'Kristy','Kristy@gmail.com',2),
(4,'John','John@gmail.com',1),
(5,'Rob','Rob@gmail.com',1)

-- updating
update tblPerson
set Email = 'TomUpdated@gmail.com'
where Id = 1

update tblPerson
set GenderId = 1
where Name = 'Jessy'

select * from tblPerson

-- Delete

delete from tblPerson
where Id = 3

--delete from tblPerson
--where GenderId = 1

-- Create, Read, Update, Delete

-- Create
create table Employees
(
EmployeeID int not null primary key,
FirstName nvarchar(50),
LastName nvarchar(50),
Position nvarchar(50),
Department nvarchar(50),
HireDate date)

--Insert
--insert into Employees (EmployeeID,FirstName,LastName,Position,Department,HireDate)
--values (1,'Sai','Subash','Developer','Dev Team','2024-08-19'),
--(2,'Akash','MR','Tester','Testing Team','2024-08-01'),
--(3,'Rex','Milan','Architect','DevOps Team','2024-09-01')

-- Update

update Employees
set Position = 'UI Developer'
where EmployeeID = 3

-- Read

select * from Employees

-- Delete

delete from Employees
where EmployeeID = 3

-- Insert
insert into Employees (EmployeeID,FirstName,LastName,Position,Department,HireDate)
values (1,'Amit','Sharma','Software Engineer','IT','2022-01-15'),
(2,'Priya','Mehta','Project Manager','Operations','2023-02-20'),
(3,'Raj','Patel','Business Analyst','Finance','2021-06-30'),
(4,'Sunita','Verma','HR Specialist','HR','2019-08-12'),
(5,'Vikram','Rao','Software Engineer','IT','2021-03-18'),
(6,'Anjali','Nair','HR Manager','HR','2020-05-14'),
(7,'Rohan','Desai','Finance Manager','Finance','2022-11-25'),
(8,'Sneha','Kumar','Operations Coordinator','Operations','2023-07-02'),
(9,'Deepak','Singh','Data Scientist','IT','2022-08-05'),
(10,'Neha','Gupta','Business Analyst','Finance','2020-10-10')

-- Tasks

--select FirstName from Employees

--select * from Employees
--where Department = 'IT'

--select * from Employees
--where HireDate > '2022-01-01'

select * from Employees
where Department in ('IT','HR')

select Distinct Department from Employees

select * from Employees
where Department = 'IT' and HireDate > '2022-01-01'

select * from Employees
where Department = 'IT' or HireDate > '2022-01-01'

select * from Employees
where HireDate between '2022-01-01' and '2022-12-31'

select * from Employees
where LastName like 'S%'

select FirstName + ' ' + LastName as FullName, Department from Employees

select E.FirstName, E.LastName, E.Department 
from Employees as E
where E.Department = 'IT'

select count(*) as EmployeeCount from Employees

select Department, count(*) as EmployeeCount from Employees
group by Department

create table Departments 
( DepartmentID int primary key,
DepartmentName varchar(50)
)

insert into Departments (DepartmentID,DepartmentName)
values
(1,'IT'),
(2,'HR'),
(3,'Finance'),
(4,'Operations')

select * from Employees
select * from Departments

select e.EmployeeID, e.FirstName, e.LastName, e.Position, d.DepartmentID
from Employees e join Departments d on e.Department = d.DepartmentName

-- Subquery

Select FirstName, LastName from Employees
where HireDate = (Select min(HireDate) from Employees)

--Retrieve the names of employees who work in a department where the department has more than 2 employees.

select FirstName, LastName from Employees 
where Department in (select Department from Employees group by Department having count(*)>2)
