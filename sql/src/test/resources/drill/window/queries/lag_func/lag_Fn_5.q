SELECT col4 , LAG(col4 ) OVER ( PARTITION BY col2 ORDER BY col0 ) LAG_col4 FROM "fewRowsAllData.parquet"