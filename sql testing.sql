-- find end of last saturday date 
DECLARE @endDate DATETIME = DATEADD(
  DAY,
  -1,
  DATEADD(WEEK, DATEDIFF(WEEK, 0, GETDATE()), 0)
)
SELECT
  @endDate AS EndDate
FROM
  FasTraxWarehouse.dbo.AcctJournal