---
title: KS3 Filesystem
keywords: [hudi, hive, aws, s3, spark, presto, ks3]
summary: In this page, we go over how to configure Hudi with KS3 filesystem.
last_modified_at: 2021-08-09T15:59:57-04:00
---
In this page, we explain how to get your Hudi spark job to store into KS3.

## KS3 configs

There are two configurations required for Hudi-KS3 compatibility:

- Adding KS3 Credentials for Hudi
- Adding required Jars to classpath

### KS3 Credentials

Simplest way to use Hudi with KS3, is to configure your `SparkSession` or `SparkContext` with KS3 credentials. Hudi will automatically pick this up and talk to KS3.

Alternatively, add the required configs in your core-site.xml from where Hudi can fetch them. Replace the `fs.defaultFS` with your KS3 bucket name and Hudi should be able to read/write from the bucket.

```xml
  <property>
      <name>fs.defaultFS</name>
      <value>hdfs://ks3node</value>
  </property>

  <property>
      <name>fs.ks3.impl</name>
      <value>com.ksyun.kmr.hadoop.fs.Ks3FileSystem</value>
  </property>

  <property>
      <name>fs.ks3.AccessKey</name>
      <value>KS3_KEY</value>
  </property>

  <property>
       <name>fs.ks3.AccessSecret</name>
       <value>KS3_SECRET</value>
  </property>

```

### KS3 Libs

KS3 hadoop libraries to add to our classpath

 - com.ksyun:ks3-kss-java-sdk:1.0.2
