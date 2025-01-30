WITH
  ctePreAgg AS ( --==== Pre-aggregate the relevant columns to make display easier.
    SELECT
      DBName = DB_NAME(database_id) --Easier than joining to sys.databases
,
      MaxSeek = MAX(last_user_seek),
      MaxScan = MAX(last_user_scan),
      MaxLookUP = MAX(last_user_lookup),
      MaxUpdate = MAX(last_user_update)
    FROM
      sys.dm_db_index_usage_stats
    GROUP BY
      database_id
  ) --==== Display only the latest user activity date for each database.
  -- The OUTER APPLY does a fast and easy "UNPIVOT" to make finding the MAX
  -- from the 4 pre-aggregated columns a simple task.
SELECT
  TOP 1 [Database Name] = DBName
FROM
  ctePreAgg pa
  OUTER APPLY (
    VALUES
      (MaxSeek),
      (MaxScan),
      (MaxLookUP),
      (MaxUpdate)
  ) oa (LastReadDT)
GROUP BY
  DBName,
  oa.LastReadDT
HAVING
  oa.LastReadDT >= CONVERT(DATE, GETDATE())
  AND DBName <> 'msdb'
ORDER BY
  oa.LastReadDT DESC,
  DBName;