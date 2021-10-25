---
title: S3 文件系统
keywords: [ hudi, hive, aws, s3, spark, presto]
summary: 在本页中，我们将讨论如何在 S3 文件系统中配置 Hudi 。
last_modified_at: 2019-12-30T15:59:57-04:00
language: cn
---
在本页中，我们将解释如何让你的 Hudi Spark 作业存储到 AWS S3 。

## AWS 配置


Hudi 与 S3 的适配需要两项配置：

- 为 Hudi 加 AWS 凭证
- 将需要的 jar 包添加到类路径

### AWS 凭证

在 S3 上使用 Hudi 的最简单的办法，是为你的 `SparkSession` 或 `SparkContext` 设置 S3 凭证。 Hudi 将自动拾取并通知 S3 。

或者，将需要的配置添加到你的 core-site.xml 文件中， Hudi 可以从那里获取它们。用你的 S3 Bucket 名称替换 `fs.defaultFS` ，之后 Hudi 应该能够从 Bucket 中读取/写入.

```xml
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


`hudi-cli` 或 DeltaStreamer 这些工具集能通过 `HOODIE_ENV_` 前缀的环境变量拾取。以下是一个作为示例的基础代码片段，它设置了这些变量并让 CLI 能够在保存在 S3 上的数据集上工作。

```java
export HOODIE_ENV_fs_DOT_s3a_DOT_access_DOT_key=$accessKey
export HOODIE_ENV_fs_DOT_s3a_DOT_secret_DOT_key=$secretKey
export HOODIE_ENV_fs_DOT_s3_DOT_awsAccessKeyId=$accessKey
export HOODIE_ENV_fs_DOT_s3_DOT_awsSecretAccessKey=$secretKey
export HOODIE_ENV_fs_DOT_s3n_DOT_awsAccessKeyId=$accessKey
export HOODIE_ENV_fs_DOT_s3n_DOT_awsSecretAccessKey=$secretKey
export HOODIE_ENV_fs_DOT_s3n_DOT_impl=org.apache.hadoop.fs.s3a.S3AFileSystem
```



### AWS 库

将 AWS Hadoop 库添加到我们的类路径。

 - com.amazonaws:aws-java-sdk:1.10.34
 - org.apache.hadoop:hadoop-aws:2.7.3

如果使用了 AWS Glue 的数据，则需要 AWS Glue 库。

 - com.amazonaws.glue:aws-glue-datacatalog-hive2-client:1.11.0
 - com.amazonaws:aws-java-sdk-glue:1.11.475