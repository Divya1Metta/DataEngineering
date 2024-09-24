use company

-- Data Integrity

create table Products (
	ProductID int primary key,            -- Enforces unique ProductID
	ProductName Varchar(100) not null,    -- Prevents Null values
	Category Varchar(50) not null,
	Price decimal(10,2) check (Price > 0), -- Ensures Price is positive
	StockQuantity int default 0 );         -- Default value for stockQuantity


	-- String Functions
	-- LEN(ProductName)
	-- UPPER(ProductName)
	-- LOWER(ProductName)
	-- SUBSTRING(ProductName,1,3)
	-- REPLACE(ProductName,'Phone','Device')
	-- LTRIM() and RTRIM() -- Remove Spaces
	-- CHARINDEX('e',ProductName) -- Finds the first occurance of the character
	-- CONCAT(ProductName,' - ',Category)
	-- LEFT(ProductName, 5) -- Gives the first 5 characters from left
	-- RIGHT(ProductName, 3) -- Gives the last 3 characters from right
	-- REVERSE(ProductName) -- Reverse the String
	-- FORMAT(Price, 'N2') -- Formats the values, N2 adds commas
	-- REPLICATE(ProductName, 3) -- Repeats the value

-- DATE functions
select getdate() as CurrentDate, DATEADD(day,10,getdate()) as FutureDate;

select DATEADD(Year, -1, GETDATE()) as DateMinus1Year;

select DATEDIFF(DAY, '2024-01-01',GETDATE()) as DaysDifference

select FORMAT(GETDATE(),'MMMM dd, yyyy') as FormattedDate

select YEAR(getdate()) as CurrentYear

select MONTH(getdate()) as CurrentMonth

select DAY(getdate()) as CurrentDay

-- Find the number of months since bday

select DATEDIFF(month,'2002-07-27',getdate()) as MonthsDifference
