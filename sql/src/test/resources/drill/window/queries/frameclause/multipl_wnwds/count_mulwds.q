SELECT c8, c1,
       COUNT(c1) OVER W as w_count_c1,
       COUNT(c2) OVER W2 as w2_count_c2,
       COUNT(c3) OVER W3 as w3_count_c3,
       COUNT(c4) OVER W_DFLT_FRM AS wDfrm_count_c4,
       COUNT(c5) OVER W2 as w2_count_c5,
       COUNT(c7) OVER W3 as w3_count_c7,
       COUNT(c8) OVER W as w_count_c8,
       COUNT(c9) OVER W_DFLT_FRM AS wDfrm_count_c9
FROM "t_alltype.parquet"
       WINDOW W AS ( PARTITION BY c8 ORDER BY c1 RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ),
       W2 AS ( PARTITION BY c8 ORDER BY c1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ),
       W3 AS ( PARTITION BY c8 ORDER BY c1 RANGE BETWEEN CURRENT ROW AND CURRENT ROW ),
       W_DFLT_FRM AS ( PARTITION BY c8 ORDER BY c1 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW )
