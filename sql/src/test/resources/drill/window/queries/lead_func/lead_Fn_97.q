select col6, lead(col6) over(partition by col7 order by col6) lead_col6 from "allTypsUniq.parquet" where col6 > "1947-05-12" and col6 < "2007-10-01"