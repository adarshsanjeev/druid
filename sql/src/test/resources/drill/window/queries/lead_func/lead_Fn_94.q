select col5, lead(col5) over(partition by col7 order by col5) lead_col5 from "allTypsUniq.parquet" where col5 >= "1947-07-02 00:28:02.418" and col5 < "2011-06-02 00:28:02.218"