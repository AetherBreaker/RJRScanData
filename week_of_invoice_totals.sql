-- find end of last saturday date 
DECLARE @endDate DATETIME = DATEADD(
  DAY,
  -1,
  DATEADD(WEEK, DATEDIFF(WEEK, 0, GETDATE()), 0)
) DECLARE @startDate DATETIME = DATEADD(DAY, -7, @endDate)
SELECT
  IT.Invoice_Number,
  IT.Store_ID,
  IT.CustNum,
  IT.DateTime,
  IT.Total_Cost,
  IT.Total_Price,
  IT.Total_Tax1,
  IT.Grand_Total,
  IT.Amt_Tendered,
  IT.Amt_Change,
  IT.Cashier_ID,
  IT.Station_ID,
  IT.Taxed_Sales,
  IT.NonTaxed_Sales,
  IT.CA_Amount,
  IT.ReferenceInvoiceNumber,
  IT.Total_UndiscountedSale,
  IT.AgeVerificationMethod,
  IT.AgeVerification
FROM
  cresql.dbo.Invoice_Totals IT
WHERE
  IT.DateTime BETWEEN @startDate AND @endDate