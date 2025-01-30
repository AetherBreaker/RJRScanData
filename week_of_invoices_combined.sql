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
  IT.AgeVerification,
  II.Invoice_Number,
  II.LineNum,
  II.ItemNum,
  II.Quantity,
  II.CostPer,
  II.PricePer,
  II.Tax1Per,
  II.Kit_ItemNum,
  II.DiffItemName,
  II.Store_ID,
  II.origPricePer,
  II.Special_Price_Lock,
  II.BulkRate,
  II.SalePricePer,
  II.KitchenQuantityPrinted,
  II.PricePerBeforeDiscount,
  II.OrigPriceSetBy,
  II.PriceChangedBy,
  II.Kit_Override,
  II.KitTotal
FROM
  cresql.dbo.Invoice_Itemized II
  LEFT JOIN cresql.dbo.Invoice_Totals IT ON II.Invoice_Number = IT.Invoice_Number
WHERE
  IT.DateTime BETWEEN @startDate AND @endDate