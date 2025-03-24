# APACHE_SPARK
TO READ PARQUEST FILE INTO COMMAND PROMPT
import pyarrow as pa
import pyarrow.parquet as pq

parquet_file = pq.ParquetFile(r'C:\Users\rushil\Downloads\Spark-The-Definitive-Guide-master\data\flight-data\parquet\2010-summary.parquet\part-r-00000-1a9822ba-b8fb-4d8e-844a-ea30d0801b9e.gz.parquet')
parquet_file.metadata
parquet_file.metadata.row_group(0) 
parquet_file.metadata.row_group(0).column(0)
parquet_file.metadata.row_group(0).column(0).statistics 

Run the below command in cmd/terminal
parquet-tools show  C:\Users\RUSHIL\Downloads\Spark-The-Definitive-Guide-master\data\flight-data\parquet\2010-summary.parquet\part-r-00000-1a9822ba-b8fb-4d8e-844a-ea30d0801b9e.gz.parquet
parquet-tools inspect  (path of your file location as above)
