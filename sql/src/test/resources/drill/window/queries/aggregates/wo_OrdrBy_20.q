SELECT COUNT(col_int) OVER (PARTITION BY col_tm) count_int, col_tm, col_int FROM "smlTbl.parquet" WHERE col_vchar_52 = "AXXXXXXXXXXXXXXXXXXXXXXXXXCXXXXXXXXXXXXXXXXXXXXXXXXB"