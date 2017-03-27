---
title: S3 Filesystem (experimental)
keywords: sql hive s3 spark presto
sidebar: mydoc_sidebar
permalink: s3_hoodie.html
toc: false
summary: In this page, we go over how to configure hoodie with S3 filesystem.
---
Hoodie works with HDFS by default. There is an experimental work going on Hoodie-S3 compatibility.

## S3 configs

There are two configurations required for Hoodie-S3 compatibility:
- Adding AWS Credentials for Hoodie
- Adding required Jars to classpath

Add the required configs in your core-site.xml from where Hoodie can fetch them. Replace the `fs.defaultFS` with your S3 bucket name and Hoodie should be able to read/write from the bucket. 

```
  <property>
      <name>fs.defaultFS</name>
      <value>s3://ysharma</value>
  </property>

  <property>
      <name>fs.s3.impl</name>
      <value>org.apache.hadoop.fs.s3native.NativeS3FileSystem</value>
  </property>

  <property>
      <name>fs.s3.awsAccessKeyId</name>
      <value>AWS_KEY</value>
  </property>

  <property>
       <name>fs.s3.awsSecretAccessKey</name>
       <value>AWS_SECRET</value>
  </property>

  <property>
       <name>fs.s3n.awsAccessKeyId</name>
       <value>AWS_KEY</value>
  </property>

  <property>
       <name>fs.s3n.awsSecretAccessKey</name>
       <value>AWS_SECRET</value>
  </property>
```

AWS hadoop libraries to add to your classpath -
 - com.amazonaws:aws-java-sdk:1.10.34
 - org.apache.hadoop:hadoop-aws:2.7.3


