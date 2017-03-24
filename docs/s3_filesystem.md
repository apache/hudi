---
title: S3 Filesystem (experimental)
keywords: sql hive s3 spark presto
sidebar: mydoc_sidebar
permalink: s3_hoodie.html
toc: false
summary: In this page, we go over how to configure hoodie with S3 filesystem.
---
Hoodie works with HDFS by default. There is an experimental work going on Hoodie-S3 compatibility.

In the following sections, we cover the configs needed across different query engines to achieve this.

{% include callout.html content="Get involved to improve this integration [here](https://github.com/uber/hoodie/issues/110) " type="info" %}

## S3 configs

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


