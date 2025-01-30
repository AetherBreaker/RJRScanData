-- find end of last saturday date 
DECLARE @endDate DATETIME = DATEADD(
  DAY,
  -1,
  DATEADD(WEEK, DATEDIFF(WEEK, 0, GETDATE()), 0)
) DECLARE @startDate DATETIME = DATEADD(DAY, -7, @endDate)
SELECT
  II.Invoice_Number,
  IT.CustNum,
  C.Phone_1,
  IT.AgeVerificationMethod,
  IT.AgeVerification,
  II.LineNum,
  IT.Cashier_ID,
  IT.Station_ID,
  II.ItemNum,
  I.ItemName,
  I.ItemName_Extra,
  II.DiffItemName,
  I.Dept_ID,
  I.Unit_Type,
  IT.DateTime,
  II.Quantity,
  II.CostPer,
  II.PricePer,
  II.Tax1Per,
  II.Store_ID,
  II.origPricePer,
  II.BulkRate,
  II.SalePricePer,
  II.PricePerBeforeDiscount,
  II.PriceChangedBy
FROM
  cresql.dbo.Invoice_Itemized II
  LEFT JOIN cresql.dbo.Inventory I ON II.ItemNum = I.ItemNum
  LEFT JOIN cresql.dbo.Invoice_Totals IT ON II.Invoice_Number = IT.Invoice_Number
  LEFT JOIN cresql.dbo.Customer C ON IT.CustNum = C.CustNum
WHERE
  IT.DateTime BETWEEN @startDate AND @endDate