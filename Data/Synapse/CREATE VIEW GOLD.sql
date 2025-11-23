--Calender View
CREATE VIEW gold.calender
AS 
select* from OPENROWSET(
    bulk 'https://adsstoragedatalake.blob.core.windows.net/silver/AdventureWorks_Calendar/',
    FORMAT ='PARQUET'
)as query11

--Customer View

CREATE VIEW gold.customers
AS 
select* from OPENROWSET(
    bulk 'https://adsstoragedatalake.blob.core.windows.net/silver/AdventureWorks_Customers/',
    FORMAT ='PARQUET'
)as query11

--Product View
CREATE VIEW gold.products
AS 
select* from OPENROWSET(
    bulk 'https://adsstoragedatalake.blob.core.windows.net/silver/AdventureWorks_Products/',
    FORMAT ='PARQUET'
)as query11


--Returns View
CREATE VIEW gold.products
AS 
select* from OPENROWSET(
    bulk 'https://adsstoragedatalake.blob.core.windows.net/silver/AdventureWorks_Returns/',
    FORMAT ='PARQUET'
)as query11


--Sales View
CREATE VIEW gold.sales
AS 
select* from OPENROWSET(
    bulk 'https://adsstoragedatalake.blob.core.windows.net/silver/AdventureWorks_Sales/',
    FORMAT ='PARQUET'
)as query11


--Subcat View
CREATE VIEW gold.subcategories
AS 
select* from OPENROWSET(
    bulk 'https://adsstoragedatalake.blob.core.windows.net/silver/AdventureWorks_Subcategories/',
    FORMAT ='PARQUET'
)as query11


--Territories View
CREATE VIEW gold.territories
AS 
select* from OPENROWSET(
    bulk 'https://adsstoragedatalake.blob.core.windows.net/silver/AdventureWorks_Territories/',
    FORMAT ='PARQUET'
)as query11