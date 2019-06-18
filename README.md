# Hudi
Apache Hudi (pronounced Hoodie) stands for `Hadoop Upserts anD Incrementals`. Hudi manages storage of large analytical datasets on DFS (Cloud stores, HDFS or any Hadoop FileSystem compatible storage) and provide ability to query them  via three types of views

 * **Read Optimized View** - Provides excellent query performance via purely columnar storage (e.g. [Parquet](https://parquet.apache.org/))
 * **Incremental View** - Provides a change stream with records inserted or updated after a point in time.
 * **Real time View ** - Provides queries on real-time data, using a combination of columnar & row based storage (e.g Parquet + [Avro](http://avro.apache.org/docs/current/mr.html))

For more, head over [here](https://hudi.apache.org)
