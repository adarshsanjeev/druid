SELECT AVG(c2) OVER(PARTITION BY c8 ORDER BY c1 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM "t_alltype.parquet"