# Hudi
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fuber%2Fhudi.svg?type=shield)](https://app.fossa.io/projects/git%2Bgithub.com%2Fuber%2Fhudi?ref=badge_shield)

Hudi (pronounced Hoodie) stands for `Hadoop Upserts anD Incrementals`. Hudi manages storage of large analytical datasets on [HDFS](http://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html) and serve them out via two types of tables

 * **Read Optimized Table** - Provides excellent query performance via purely columnar storage (e.g. [Parquet](https://parquet.apache.org/))
 * **Near-Real time Table (WIP)** - Provides queries on real-time data, using a combination of columnar & row based storage (e.g Parquet + [Avro](http://avro.apache.org/docs/current/mr.html))

For more, head over [here](https://uber.github.io/hudi)


## License
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fuber%2Fhudi.svg?type=large)](https://app.fossa.io/projects/git%2Bgithub.com%2Fuber%2Fhudi?ref=badge_large)