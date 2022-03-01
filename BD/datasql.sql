--CREACION DimCustomer

create table DimCustomer (
    CustomerID    char(5),
    CustomerName  varchar (40),
    City varchar (15),
    Country varchar (15),
    Region varchar (15),
primary key (CustomerID)
);

--CREACION DimCustomer

create table DimEmployee (
    EmployeeID    int,
    Name  varchar (30),
    City varchar (15),
    Country varchar (15),
    Region varchar (15),
    hiredate datetime,
    primary key (EmployeeID)
);

--CREACION DimCustomer

create table DimTime (
  orderDate  Datetime,
  primary key (orderDate)
);

--CREACION DimCustomer

create table DimProduct (
  ProductID    int,
  ProductName  varchar (40),
  categoryName varchar (15),
  primary key (productID)
);

--CREACION DimCustomer

create table FactSales (
  ProductID       int ,
  EmployeeID      int ,
  CustomerID      char(5) ,
  orderDate       datetime ,
  OrderID         int,
  Quantity        smallint,
  unitPrice       money,
  total           money,
  primary key (ProductID, EmployeeID, CustomerID, orderDate),
  foreign key (ProductID)  references DimProduct(productID),
  foreign key (EmployeeID)     references DimEmployee(employeeID),
  foreign key (CustomerID)     references DimCustomer(CustomerID),
  foreign key (orderDate)  references DimTime(orderDate)
);




-- Insert into DimProduct
--  select p.productId, p.productName, c.categoryName
  -- from Northwind.products p, Northwind.categories c
  -- where p.categoryID=c.categoryID;





select od.ProductID, o.EmployeeID, o.CustomerID, o.OrderDate AS orderDate,
                o.Id AS orderID, od.quantity AS Quantity, od.unitPrice,
                od.quantity * od.unitPrice as total        
              from 'Order' o, OrderDetail od
              where o.ID = od.OrderId;

select distinct ProductName as Ten_Most_Expensive_Products, 
         UnitPrice
from Product as a
where 10 >= (select count(distinct UnitPrice)
                    from Product as b
                    where b.UnitPrice >= a.UnitPrice)
order by UnitPrice desc;


select * from
(
    select distinct ProductName as Ten_Most_Expensive_Products, 
           UnitPrice
    from Product
    order by UnitPrice desc
) as a
limit 10;

