use WideWorldImporters
--1.	List of Persons’ full name, all their fax and phone numbers, as well as the phone number and fax of the company they are working for (if any). 
select Distinct a.FullName,a.PhoneNumber,a.FaxNumber, s.PhoneNumber as CompanyPhone,s.FaxNumber as CompanyFax,s.CustomerName,a.PersonID
from Application.People a 
left join Sales.Customers s 
on a.PersonID=s.PrimaryContactPersonID
order by PersonID

--2.	If the customer's primary contact person has the same phone number as the customer’s phone number, list the customer companies. 
select s.CustomerName,s.CustomerID,a.FullName 
from Sales.Customers s 
join Application.People a 
on s.PrimaryContactPersonID=a.PersonID 
Where s.PhoneNumber=a.PhoneNumber
--3.	List of customers to whom we made a sale prior to 2016 but no sale since 2016-01-01.
with cte_2016 as(
	select distinct customerID
	from Sales.CustomerTransactions
	where LastEditedWhen < '2016-01-01'
)

select customerID
from sales.Customers SC
where SC.CustomerID not in (Select* from cte_2016)

--4.	List of Stock Items and total quantity for each stock item in Purchase Orders in Year 2013.           (total stock in hand or just ordered)
Select StockItemID, sum(OrderedOuters) AS TotalQuantity
from Purchasing.PurchaseOrderLines
where lastReceiptDate Between '2013-01-01' and '2013-12-31'
group by StockItemID

--5 List of stock items that have at least 10 characters in description.
Select Distinct StockItemID,len(Description)AS CHARLength
From Purchasing.PurchaseOrderLines
Where len(Description)>10

-- 6.	List of stock items that are not sold to the state of Alabama and Georgia in 2014.
Select Distinct OL.StockItemID--,C.CustomerID,CS.CityName,CS.StateProvinceID            --one to one, i can use distinct, such as id and name
From Sales.OrderLines OL
join sales.Orders O on OL.OrderID = O.OrderID and YEAR (O.OrderDate)=YEAR('2014')
join Sales.Customers C on O.CustomerID = C.CustomerCategoryID
Join Application.Cities CS on C.PostalCityID = cs.CityID
--join Application.StateProvinces st on cs.StateProvinceID=st.StateProvinceID
where CS.StateProvinceID not in (1,11) 




--7.	List of States and Avg dates for processing (confirmed delivery date – order date).
select 
	st.StateProvinceCode,
	AVG(DATEDIFF(DAY, so.OrderDate,SI.ConfirmedDeliveryTime)) as 'ADFP'
from Sales.Invoices SI
	join Sales.Orders SO on SI.OrderID= SO.OrderID
	join Sales.Customers C on SO.CustomerID = C.CustomerCategoryID
    Join Application.Cities CS on C.PostalCityID = cs.CityID
    join Application.StateProvinces st on cs.StateProvinceID=st.StateProvinceID
	Group by st.StateProvinceCode

--8.	List of States and Avg dates for processing (confirmed delivery date – order date) by month.

select 
	cast(Year(so.OrderDate) as varchar(4)) As 'year', cast (Month(so.OrderDate) as varchar(2)) as 'MONTH', --change data type to vari from date.
	st.StateProvinceCode,
	AVG(DATEDIFF(DAY, so.OrderDate,cast(SI.ConfirmedDeliveryTime as date))) as 'ADFP'
from Sales.Invoices SI
	join Sales.Orders SO on SI.OrderID= SO.OrderID
	join Sales.Customers C on SO.CustomerID = C.CustomerCategoryID
    Join Application.Cities CS on C.PostalCityID = cs.CityID
    join Application.StateProvinces st on cs.StateProvinceID=st.StateProvinceID
	Group by Year (so.OrderDate),Month (so.OrderDate), st.StateProvinceCode
	Order by st.StateProvinceCode,Year (so.OrderDate),Month (so.OrderDate)

--pivot
SELECT StateProvinceCode as StateName, [1], [2], [3], [4], [5], [6], [7], [8], [9], [10],[11], [12]
 FROM (
	select st.StateProvinceCode,
		cast (Month(so.OrderDate) as varchar(2)) as 'MONTH',
		DATEDIFF(DAY, so.OrderDate,cast(SI.ConfirmedDeliveryTime as date))as 'DataDiff'
	from Sales.Invoices SI
		join Sales.Orders SO on SI.OrderID= SO.OrderID
		join Sales.Customers C on SO.CustomerID = C.CustomerCategoryID
		Join Application.Cities CS on C.PostalCityID = cs.CityID
		join Application.StateProvinces st on cs.StateProvinceID=st.StateProvinceID
) t 
PIVOT(
AVG(t.DataDiff) for Month in ([1], [2], [3], [4], [5], [6], [7], [8], [9], [10],[11], [12])
) AS pivot_table;


--9.	List of StockItems that the company purchased more than sold in the year of 2015.
Select* 
from (select StockItemID,
		sum(SIL.Quantity) as SA
	from sales.InvoiceLines SIL
	where year(LastEditedWhen)= year('2015')
	group by StockItemID) as SAT
join
	(select StockitemID,
		sum(POL.ReceivedOuters) as PA
	from Purchasing.PurchaseOrderLines POL
	where year(LastEditedWhen)= year('2015')
	group by StockItemID) as PAT
on SAT.StockitemID = PAT.StockitemId
where SAT.SA<PAT.PA

--lab 1
Select * 
from
	(SELECT 
		OL.StockItemID,
		Sum(OL.Quantity) AS SaleAmt
		FROM Sales.OrderLines OL join Sales.Orders so on ol.OrderID=so.OrderID
		Where YEAR(so.OrderDate) = YEAR('2015') 
		group by StockItemID
	) as OS 
JOIN (
	SELECT  
		StockItemID,
		Sum(POL.OrderedOuters) AS PurchaseAmt
	FROM Purchasing.PurchaseOrderLines POL 
	join Purchasing.PurchaseOrders PO on pol.PurchaseOrderID=po.PurchaseOrderID
	where YEAR (po.OrderDate) =YEAR('2015') 
	group by StockItemID
	) AS OP 
on os.StockItemID=op.StockItemID
where op.PurchaseAmt>os.SaleAmt
--10.	List of Customers and their phone number, together with the primary contact person’s name, to whom we did not sell more than 10  mugs (search by name) in the year 2016.

SELECT SC.CustomerName, SC.PhoneNumber, AP.FullName
FROM Sales.Customers SC
JOIN Application.People AS AP
ON  SC.PrimaryContactPersonID = AP.PersonID 
WHERE SC.CustomerID NOT IN (SELECT CustomerID
							FROM (SELECT SO.CustomerID,
								  SUM(Quantity) AS TotalQuantity
								  FROM Sales.OrderLines  Ol
								  JOIN Sales.Orders  SO
								  ON SO.OrderID = Ol.OrderID 
								  WHERE Ol.Description LIKE '%mug%' AND Year(SO.OrderDate) = Year('2016') 
								  GROUP BY  SO.CustomerID
								  HAVING SUM(Quantity) > 10) AS MG)

--11.	List all the cities that were updated after 2015-01-01. 这是temporal table.
SELECT CityName,ValidFrom
From Application.Cities
where ValidFrom >'20141231 23:59:59.9999999'
order by CityName,ValidFrom

--12.	List all the Order Detail (Stock Item name, delivery address, delivery state, city, country, customer name, customer contact 
--person name, customer phone, quantity) for the date of 2014-07-01. Info should be relevant to that date.
	 SELECT 
		 wsi.StockItemName,
		 si.DeliveryInstructions,
		 ASP.StateProvinceName,
		 AC.CityName,
		 SC.CustomerName,
		 AP.FullName,
		 SC.PhoneNumber,
		 SIL.Quantity
	 FROM Sales.Invoices SI
	 JOIN Sales.InvoiceLines SIL on SI.InvoiceID=SIL.InvoiceID
	 JOIN Warehouse.StockItems FOR SYSTEM_TIME AS OF '2014-07-01' wsi 
	 on SIL.StockItemID= wsi.StockItemID
	 JOIN Sales.Customers FOR SYSTEM_TIME AS OF '2014-07-01' SC 
	 on SI.CustomerID=SC.CustomerID
	 JOIN Application.People FOR SYSTEM_TIME AS OF '2014-07-01' AP 
	 on AP.PersonID = SC.PrimaryContactPersonID
	 JOIN Application.Cities FOR SYSTEM_TIME AS OF '2014-07-01' AC 
	 on SC.DeliveryCityID = AC.CityID
	 JOIN Application.StateProvinces FOR SYSTEM_TIME AS OF '2014-07-01' ASP 
	 on ASP.StateProvinceID = AC.StateProvinceID
	 JOIN Application.Countries FOR SYSTEM_TIME AS OF '2014-07-01' ACT 
	 on ACT.CountryID = ASP.CountryID
	


--13.	List of stock item groups and total quantity purchased, total quantity sold, and the remaining stock quantity (quantity purchased – quantity sold) subquery join
 SELECT WSG.StockGroupName,SIP.StockItemID,SIP.QuantityPurchased,SIS.QuantitySold,SIP.QuantityPurchased-SIS.QuantitySold as Remaining
	 FROM(
		 select pol.StockItemID,SUM(pol.ReceivedOuters) as QuantityPurchased
		 From Purchasing.PurchaseOrderLines pol 
		 group by pol.StockItemID) AS SIP
	  JOIN(
		 Select SOL.StockItemID, SUM(SOL.Quantity) as QuantitySold
		 FROM Sales.OrderLines SOL
		 Group by SOL.StockItemID) AS SIS
	ON SIP.StockItemID=SIS.StockItemID
	 JOIN Warehouse.StockItemStockGroups SISG on SIP.StockItemID= SISG.StockItemID
	 JOIN Warehouse.StockGroups WSG on WSG.StockGroupID=SISG.StockGroupID

--14.	List of Cities in the US and the stock item that the city got the most deliveries in 2016. If the city did not purchase any stock items in 2016, print “No Sales”.subquery join subquery
WITH ItemByCity AS 
(SELECT l.StockItemID, l.Description, c.PostalCityID, SUM(Quantity) AS Total, 
RANK() OVER(PARTITION BY PostalCityID ORDER BY SUM(Quantity)DESC ) AS StockItemRank
FROM Sales.Orderlines AS l
JOIN Sales.Orders AS o
ON l.OrderID = o.OrderID
JOIN Sales.Customers AS c
ON o.CustomerID = c.CustomerID
WHERE Year(o.OrderDate)='2016'
GROUP BY c.PostalCityID,l.StockItemID, l.Description)

SELECT ac.CityID, ac.CityName,
	   ISNULL(i.Description, 'No Sales') AS ItemDescription
FROM Application.Cities AS ac
LEFT JOIN (
	SELECT  Description, 
			PostalCityID 
	FROM ItemByCity  
	WHERE ItemBYCity.StockItemRank=1) AS i
ON i.PostalCityID = ac.CityID 
JOIN Application.StateProvinces AS st
ON ac.StateProvinceID = st.StateProvinceID
JOIN Application.Countries AS co
ON st.CountryID = co.CountryID
WHERE co.CountryName = 'United States'
ORDER BY CityID

--15.	List any orders that had more than one delivery attempt (located in invoice table).-- 选第二个括号里的数据就是【1】
Select  SI.OrderID, json_value(SI.ReturnedDeliveryData,'$.Events[1].Comment') as comment
from sales.Invoices SI
where json_value(SI.ReturnedDeliveryData,'$.Events[1].Comment')  IS NOT NULL;


--16.	List all stock items that are manufactured in China. (Country of Manufacture)
SELECT WSI.stockItemID, WSI.StockItemName, json_value(WSI.CustomFields,'$.CountryOfManufacture') as Country
FROM Warehouse.StockItems as WSI
where json_value(WSI.CustomFields,'$.CountryOfManufacture') = 'China'
--17.	Total quantity of stock items sold in 2015, group by country of manufacturing.
select json_value(WSI.CustomFields,'$.CountryOfManufacture') as Country, sum(SIL.quantity) as TotalQuantity, WSI.StockItemName
from sales.InvoiceLines SIL
join Warehouse.StockItems WSI
On SIL.StockItemID =WSI.StockItemID
where year(SIL.LastEditedWhen)= '2015'
group by json_value(WSI.CustomFields,'$.CountryOfManufacture'),WSI.StockItemName


--18.	Create a view that shows the total quantity of stock items of each stock group sold (in orders) by year 2013-2017. [Stock Group Name, 2013, 2014, 2015, 2016, 2017]
Create view TQSI as
SELECT * from
(select sg.StockGroupName,YEAR(so.OrderDate) as Year ,SOL.Quantity as QUANTITY
		FROM 
		Sales.OrderLines SOL
		JOIN Sales.Orders SO on SO.OrderID=sol.OrderID
		JOIN Warehouse.StockItemStockGroups SIG on SIG.StockItemID=sol.StockItemID 
		JOIN Warehouse.StockGroups SG on SG.StockGroupID=SIG.StockGroupID) as source
pivot( sum(Quantity)
for Year in ([2013],[2014],[2015],[2016],[2017])) as pivot_table;

--19.	Create a view that shows the total quantity of stock items of each stock group sold (in orders) by year 2013-2017. [Year, Stock Group Name1, Stock Group Name2, Stock Group Name3, … , Stock Group Name10] 
Create view TQSI2 as
SELECT * from
(select sg.StockGroupName,YEAR(so.OrderDate) as Year ,SOL.Quantity as QUANTITY
		FROM 
		Sales.OrderLines SOL
		JOIN Sales.Orders SO on SO.OrderID=sol.OrderID
		JOIN Warehouse.StockItemStockGroups SIG on SIG.StockItemID=sol.StockItemID 
		JOIN Warehouse.StockGroups SG on SG.StockGroupID=SIG.StockGroupID) as source
pivot( sum(Quantity)
for StockGroupName in ([Novelty Items],[Clothing],[Mugs],[T-Shirts],[Airline Novelties],
	[Computing Novelties],[USB Novelties],[Furry Footwear],[Toys],[Packaging Materials])) as pivot_t;
select *
from TQSI2


--20.	Create a function, input: order id; return: total of that order. List invoices and use that function to attach the order total to the other fields of invoices. 

DROP FUNCTION  if exists TotalOrder;
CREATE FUNCTION TotalOrder (@OrderId INT)
RETURNS decimal
BEGIN
	DECLARE @totalorder as decimal(18,2)

	Select @totalorder = SUM(SOL.Quantity * SOL.UnitPrice)
	 FROM Sales.OrderLines SOL
     WHERE sol.OrderID= @OrderId

	
	-- Return the result of the function
	RETURN @totalorder

END;
GO
--21.	Create a new table called ods.Orders. Create a stored procedure, with proper error handling and transactions, that input is a date;
--when executed, it would find orders of that day, calculate order total, and save the information (order id, order date, order total, customer id) into the new table.
--If a given date is already existing in the new table, throw an error and roll back. Execute the stored procedure 5 times using different dates. ？？？？？？？？？？？？？
DROP TABLE IF EXISTS ods.Orders
DROP SCHEMA IF EXISTS ods
GO
CREATE SCHEMA ods
GO 
create table ods.Orders(
CustomerID int, 
 OrderID int, 
 OrderDate Date , 
 Total Decimal(18,2), 
 )
 Drop PROCEDURE if exists getFromDate;

CREATE PROCEDURE getFromDate
(@dateinput DATE)
AS
BEGIN TRY
 BEGIN TRANSACTION
 IF (@dateinput NOT IN (SELECT do.orderDate FROM dbo.Orders do ))
  BEGIN
  INSERT INTO dbo.Orders(orderId ,orderDate ,orderTotal,customerId )
  SELECT  so.OrderID,so.OrderDate,[dbo].[OrderTotal](so.OrderID),so.CustomerID
  FROM sales.Orders so
  WHERE so.OrderDate=@dateinput
  COMMIT TRANSACTION
  END
ELSE 
BEGIN
  RAISERROR ('Date already inserted',16, 1)
 END
 END TRY
BEGIN CATCH
  Print ERROR_MESSAGE()
  Print 'transaction rolled back'
  ROLLBACK TRANSACTION
END CATCH

EXEC getFromDate @dateinput = '2013-1-2' 
DELETE FROM dbo.Orders
--22.	Create a new table called ods.StockItem. It has following columns: [StockItemID], [StockItemName] ,[SupplierID] ,[ColorID] ,[UnitPackageID] ,[OuterPackageID] ,[Brand] ,[Size] ,[LeadTimeDays] 
--,[QuantityPerOuter] ,[IsChillerStock] ,[Barcode] ,[TaxRate]  ,[UnitPrice],[RecommendedRetailPrice] ,[TypicalWeightPerUnit] ,[MarketingComments]  ,[InternalComments], [CountryOfManufacture], [Range], 
--[Shelflife]. Migrate all the data in the original stock item table.

create schema ods;
select [StockItemID], [StockItemName] ,[SupplierID] ,[ColorID] ,
[UnitPackageID] ,[OuterPackageID] ,[Brand] ,[Size] ,
[LeadTimeDays] ,[QuantityPerOuter] ,[IsChillerStock] ,
[Barcode] ,[TaxRate]  ,[UnitPrice],[RecommendedRetailPrice] ,
[TypicalWeightPerUnit] ,[MarketingComments]  ,
[InternalComments], 
JSON_VALUE(CustomFields, '$.CountryOfManufacture') AS CountryOFManufacutre, 
JSON_VALUE(CustomFields,'$.Range') AS Range, 
JSON_VALUE(CustomFields,'$.ShelfLife') AS ShelfLife
into ods.StockItem
from Warehouse.StockItems;
select* from ods.StockItem;

--23.	Rewrite your stored procedure in (21). Now with a given date, it should wipe out all the order data prior to the input date and load the order data 
--that was placed in the next 7 days following the input date.？？？？？？？？？？？？？

Drop PROCEDURE get7FromDate;
CREATE PROCEDURE get7FromDate
(@dateinput DATE)
AS
BEGIN TRY
 BEGIN TRANSACTION
 IF (@dateinput NOT IN (SELECT MIN(orderDate) FROM dbo.Orders ))
  BEGIN
  DELETE FROM dbo.Orders  
  WHERE OrderDate < @dateinput;
  INSERT INTO dbo.Orders(orderId ,orderDate ,orderTotal,customerId )
  SELECT  so.OrderID,so.OrderDate,[dbo].[OrderTotal](so.OrderID),so.CustomerID
  FROM sales.Orders so
  WHERE so.OrderDate BETWEEN @dateinput AND DATEADD(DAY, 7, @dateinput);
  COMMIT TRANSACTION
  END
ELSE 
BEGIN
  RAISERROR ('Date already inserted',16, 1)
 END
 END TRY
BEGIN CATCH
  Print ERROR_MESSAGE()
  Print 'transaction rolled back'
  ROLLBACK TRANSACTION
END CATCH


/*24.	Consider the JSON file:

{
   "PurchaseOrders":[
      {
         "StockItemName":"Panzer Video Game",
         "Supplier":"7",
         "UnitPackageId":"1",
         "OuterPackageId":[
            6,
            7
         ],
         "Brand":"EA Sports",
         "LeadTimeDays":"5",
         "QuantityPerOuter":"1",
         "TaxRate":"6",
         "UnitPrice":"59.99",
         "RecommendedRetailPrice":"69.99",
         "TypicalWeightPerUnit":"0.5",
         "CountryOfManufacture":"Canada",
         "Range":"Adult",
         "OrderDate":"2018-01-01",
         "DeliveryMethod":"Post",
         "ExpectedDeliveryDate":"2018-02-02",
         "SupplierReference":"WWI2308"
      },
      {
         "StockItemName":"Panzer Video Game",
         "Supplier":"5",
         "UnitPackageId":"1",
         "OuterPackageId":"7",
         "Brand":"EA Sports",
         "LeadTimeDays":"5",
         "QuantityPerOuter":"1",
         "TaxRate":"6",
         "UnitPrice":"59.99",
         "RecommendedRetailPrice":"69.99",
         "TypicalWeightPerUnit":"0.5",
         "CountryOfManufacture":"Canada",
         "Range":"Adult",
         "OrderDate":"2018-01-025",
         "DeliveryMethod":"Post",
         "ExpectedDeliveryDate":"2018-02-02",
         "SupplierReference":"269622390"
      }
   ]
}


Looks like that it is our missed purchase orders. Migrate these data into Stock Item, Purchase Order and Purchase Order Lines tables. Of course, save the script.*/

--25.	Revisit your answer in (19). Convert the result in JSON string and save it to the server using TSQL FOR JSON PATH.
Select *
		From dbo.TQSI2
		FOR JSON AUTO
		
--26.	Revisit your answer in (19). Convert the result into an XML string and save it to the server using TSQL FOR XML PATH.
Select *
		From dbo.TQSI2
		FOR XML AUTO,ELEMENTS
--27.	Create a new table called ods.ConfirmedDeviveryJson with 3 columns (id, date, value) . Create a stored procedure, input is a date.
--The logic would load invoice information (all columns) as well as invoice line information (all columns) and forge them into a JSON string and then insert into the new table just created.
--Then write a query to run the stored procedure for each DATE that customer id 1 got something delivered to him.

Drop Table IF EXISTS ods.ConfirmedDeviveryJson;
create table ods.ConfirmedDeviveryJson(
  id int not null Primary Key,
  [date] Date,
  [Value] Varchar(max)
  )
  Drop Procedure if exists invoiceforge;
  create procedure (@dateinput Date)
  AS 
  BEGIN 
 INSERT INTO ods.ConfirmedDeviveryJson([date],[value]) 
 VALUES(@dateinput,
 (SELECT * FROM
 Sales.Invoices SI
 LEFT JOIN Sales.InvoiceLines SIL on si.InvoiceID=sil.InvoiceID 
 where si.CustomerID=1 and  CAST(si.ConfirmedDeliveryTime as date)= @dateinput
 FOR JSON AUTO)
 )
 END
--EXEC forge @dateinput = '2013-03-13' 
-- run query for each day
DECLARE @StarDate DATE, @MaxxDate DATE  
SELECT @StarDate=MIN(CAST(si.ConfirmedDeliveryTime as date)),@MaxxDate=MAX(CAST(si.ConfirmedDeliveryTime as date))
FROM
 Sales.Invoices SI
 LEFT JOIN Sales.InvoiceLines SIL on si.InvoiceID=sil.InvoiceID 
WHERE si.CustomerID=1 
WHILE (@StarDate<@MaxxDate )
BEGIN
if( @StarDate in (select CAST(ConfirmedDeliveryTime as date) from sales.Invoices where CustomerID=1))
begin
EXEC forge @dateinput = @StarDate
end 
SET @StarDate = DATEADD(DAY,1,@StarDate)
END

