SELECT
  "Invoice_Itemized"."Invoice_Number",
  "Invoice_Totals"."CustNum",
  "Customer"."Phone_1",
  "Invoice_Totals"."AgeVerificationMethod",
  "Invoice_Totals"."AgeVerification",
  "Invoice_Itemized"."LineNum",
  "Invoice_Totals"."Cashier_ID",
  "Invoice_Totals"."Station_ID",
  "Invoice_Itemized"."ItemNum",
  "Inventory"."ItemName",
  "Inventory"."ItemName_Extra",
  "Invoice_Itemized"."DiffItemName",
  "Inventory"."Dept_ID",
  "Inventory"."Unit_Type",
  "Invoice_Totals"."DateTime",
  "Invoice_Itemized"."Quantity",
  "Invoice_Itemized"."CostPer",
  "Invoice_Itemized"."PricePer",
  "Invoice_Itemized"."Tax1Per",
  "Invoice_Itemized"."origPricePer",
  "Invoice_Itemized"."BulkRate",
  "Invoice_Itemized"."SalePricePer",
  "Invoice_Itemized"."PricePerBeforeDiscount",
  "Invoice_Itemized"."PriceChangedBy"
FROM
  "cresql"."dbo"."Invoice_Itemized"
  LEFT JOIN "cresql"."dbo"."Inventory" ON "Invoice_Itemized"."ItemNum" = "Inventory"."ItemNum"
  LEFT JOIN "cresql"."dbo"."Invoice_Totals" ON "Invoice_Itemized"."Invoice_Number" = "Invoice_Totals"."Invoice_Number"
  LEFT JOIN "cresql"."dbo"."Customer" ON "Invoice_Totals"."CustNum" = "Customer"."CustNum"
WHERE
  "Invoice_Totals"."DateTime" >= '2025-01-18'
  AND "Invoice_Totals"."DateTime" < '2025-01-26'