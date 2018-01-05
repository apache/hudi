---
title: S3 Filesystem (experimental)
keywords: sql hive s3 spark presto
sidebar: mydoc_sidebar
permalink: s3_hoodie.html
toc: false
summary: In this page, we go over how to configure hoodie with S3 filesystem.
---
Hoodie works with HDFS by default. There is an experimental work going on Hoodie-S3 compatibility.

## AWS configs

There are two configurations required for Hoodie-S3 compatibility:

- Adding AWS Credentials for Hoodie
- Adding required Jars to classpath

### AWS Credentials

Simplest way to use Hoodie with S3, is to configure your `SparkSession` or `SparkContext` with S3 credentials. Hoodie will automatically pick this up and talk to S3.

Alternatively, add the required configs in your core-site.xml from where Hoodie can fetch them. Replace the `fs.defaultFS` with your S3 bucket name and Hoodie should be able to read/write from the bucket.

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


Utilities such as hoodie-cli or deltastreamer tool, can pick up s3 creds via environmental variable prefixed with `HOODIE_ENV_`. For e.g below is a bash snippet to setup
such variables and then have cli be able to work on datasets stored in s3

```
export HOODIE_ENV_fs_DOT_s3a_DOT_access_DOT_key=$accessKey
export HOODIE_ENV_fs_DOT_s3a_DOT_secret_DOT_key=$secretKey
export HOODIE_ENV_fs_DOT_s3_DOT_awsAccessKeyId=$accessKey
export HOODIE_ENV_fs_DOT_s3_DOT_awsSecretAccessKey=$secretKey
export HOODIE_ENV_fs_DOT_s3n_DOT_awsAccessKeyId=$accessKey
export HOODIE_ENV_fs_DOT_s3n_DOT_awsSecretAccessKey=$secretKey
export HOODIE_ENV_fs_DOT_s3n_DOT_impl=org.apache.hadoop.fs.s3a.S3AFileSystem
```



### AWS Libs

AWS hadoop libraries to add to our classpath

 - com.amazonaws:aws-java-sdk:1.10.34
 - org.apache.hadoop:hadoop-aws:2.7.3


