SELECT MAX(col_int) OVER (PARTITION BY col_dt) max_int, col_dt, col_int FROM "smlTbl.parquet" WHERE col_vchar_52 = "AXXXXXXXXXXXXXXXXXXXXXXXXXCXXXXXXXXXXXXXXXXXXXXXXXXB"