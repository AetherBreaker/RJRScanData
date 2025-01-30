-- find end of last saturday date 
DECLARE @endDate DATETIME = DATEADD(
  DAY,
  -1,
  DATEADD(WEEK, DATEDIFF(WEEK, 0, GETDATE()), 0)
) DECLARE @startDate DATETIME = DATEADD(DAY, -7, @endDate)
SELECT
  ICD.Store_ID,
  ICD.Invoice_Number,
  ICD.LineNum,
  ICD.CouponLineNum,
  ICD.Amount AS "CouponDiscountAmount"
FROM
  cresql.dbo.Invoice_CouponDiscounts ICD
  LEFT JOIN cresql.dbo.Invoice_Itemized II ON ICD.Invoice_Number = II.Invoice_Number
  AND ICD.LineNum = II.LineNum
  LEFT JOIN cresql.dbo.Invoice_Totals IT ON II.Invoice_Number = IT.Invoice_Number
WHERE
  IT.DateTime BETWEEN @startDate AND @endDate