# Athena Queries


## Aggregate sales across all stores and sort them by store ID

Change the database name `miztiik_sales_db` and table name `sales_txns_in_parquet_stream_etl` as needed.

```sql
SELECT
   SUM(sales) as total_sales,
   store_id 
FROM
   "miztiik_sales_db"."sales_txns_in_parquet_stream_etl" 
WHERE
   ingest_year = '2021' 
   AND cast(ingest_year as bigint) = year(now()) 
   AND cast(ingest_month as bigint) = month(now()) 
   AND cast(ingest_day as bigint) = day_of_month(now()) 
   AND cast(ingest_hour as bigint) = hour(now()) 
GROUP BY
   store_id 
Order by
   total_sales DESC;
```