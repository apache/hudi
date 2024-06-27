---
title: Encryption
keywords: [ hudi, security ]
summary: This section offers an overview of encryption feature in Hudi
toc: true
last_modified_at: 2022-02-14T15:59:57-04:00
---

Since Hudi 0.11.0, Spark 3.2 support has been added and accompanying that, Parquet 1.12 has been included, which brings encryption feature to Hudi. In this section, we will show a guide on how to enable encryption in Hudi tables.

## Encrypt Copy-on-Write tables

First, make sure Hudi Spark 3.2 bundle jar is used.

Then, set the following Parquet configurations to make data written to Hudi COW tables encrypted.

```java
// Activate Parquet encryption, driven by Hadoop properties
jsc.hadoopConfiguration().set("parquet.crypto.factory.class", "org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory")
// Explicit master keys (base64 encoded) - required only for mock InMemoryKMS
jsc.hadoopConfiguration().set("parquet.encryption.kms.client.class" , "org.apache.parquet.crypto.keytools.mocks.InMemoryKMS")
jsc.hadoopConfiguration().set("parquet.encryption.key.list", "k1:AAECAwQFBgcICQoLDA0ODw==, k2:AAECAAECAAECAAECAAECAA==")
// Write encrypted dataframe files. 
// Column "rider" will be protected with master key "key2".
// Parquet file footers will be protected with master key "key1"
jsc.hadoopConfiguration().set("parquet.encryption.footer.key", "k1")
jsc.hadoopConfiguration().set("parquet.encryption.column.keys", "k2:rider")
    
spark.read().format("org.apache.hudi").load("path").show();
```

Here is an example.

```java
JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
jsc.hadoopConfiguration().set("parquet.crypto.factory.class", "org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory");
jsc.hadoopConfiguration().set("parquet.encryption.kms.client.class" , "org.apache.parquet.crypto.keytools.mocks.InMemoryKMS");
jsc.hadoopConfiguration().set("parquet.encryption.footer.key", "k1");
jsc.hadoopConfiguration().set("parquet.encryption.column.keys", "k2:rider");
jsc.hadoopConfiguration().set("parquet.encryption.key.list", "k1:AAECAwQFBgcICQoLDA0ODw==, k2:AAECAAECAAECAAECAAECAA==");

QuickstartUtils.DataGenerator dataGen = new QuickstartUtils.DataGenerator();
List<String> inserts = convertToStringList(dataGen.generateInserts(3));
Dataset<Row> inputDF1 = spark.read().json(jsc.parallelize(inserts, 1));
inputDF1.write().format("org.apache.hudi")
	.option("hoodie.table.name", "encryption_table")
    .option("hoodie.upsert.shuffle.parallelism","2")
    .option("hoodie.insert.shuffle.parallelism","2")
    .option("hoodie.delete.shuffle.parallelism","2")
    .option("hoodie.bulkinsert.shuffle.parallelism","2")
    .mode(SaveMode.Overwrite)
    .save("path");

spark.read().format("org.apache.hudi").load("path").select("rider").show();
```

Reading the table works if configured correctly

```
+---------+
|rider    |
+---------+
|rider-213|
|rider-213|
|rider-213|
+---------+
```

Read more from [Spark docs](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#columnar-encryption) and [Parquet docs](https://github.com/apache/parquet-format/blob/master/Encryption.md).

### Note

This feature is currently only available for COW tables due to only Parquet base files present there.