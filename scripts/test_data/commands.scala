val hudiDf = spark.read.format("org.apache.hudi").load("/tmp/hudi-deltastreamer-ny/")
hudiDf.registerTempTable("hudi_tbl")

val df = spark.read.format("parquet").load("/Users/jon/Documents/test_files/parquet_src")
df.registerTempTable("src_tbl")
val hudiCount = spark.sql("select distinct date_col, VendorID from hudi_tbl").count()
val srcCount = spark.sql("select distinct date_col, VendorID from src_tbl").count()

assert(hudiCount == srcCount)