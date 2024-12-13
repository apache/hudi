---
title: GCS 文件系统
keywords: [ hudi, hive, google cloud, storage, spark, presto, 存储 ]
summary: 在本页中，我们探讨如何在 Google Cloud Storage 中配置 Hudi。
last_modified_at: 2019-12-30T15:59:57-04:00
language: cn
---
对于存储在 GCS 上的 Hudi ， **区域** Bucket 提供了带有强一致性的 DFS API 。

## GCS 配置

Hudi 的 GCS 适配需要两项配置：

- 为 Hudi 添加 GCS 凭证
- 将需要的 jar 包添加到类路径

### GCS 凭证

在你的 core-site.xml 文件中添加必要的配置，Hudi 将从那里获取这些配置。 用你的 GCS 分区名称替换掉 `fs.defaultFS` ，以便 Hudi 能够在 Bucket 中读取/写入。

```xml
  <property>
    <name>fs.defaultFS</name>
    <value>gs://hudi-bucket</value>
  </property>

  <property>
    <name>fs.gs.impl</name>
    <value>com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem</value>
    <description>The FileSystem for gs: (GCS) uris.</description>
  </property>

  <property>
    <name>fs.AbstractFileSystem.gs.impl</name>
    <value>com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS</value>
    <description>The AbstractFileSystem for gs: (GCS) uris.</description>
  </property>

  <property>
    <name>fs.gs.project.id</name>
    <value>GCS_PROJECT_ID</value>
  </property>
  <property>
    <name>google.cloud.auth.service.account.enable</name>
    <value>true</value>
  </property>
  <property>
    <name>google.cloud.auth.service.account.email</name>
    <value>GCS_SERVICE_ACCOUNT_EMAIL</value>
  </property>
  <property>
    <name>google.cloud.auth.service.account.keyfile</name>
    <value>GCS_SERVICE_ACCOUNT_KEYFILE</value>
  </property>
```

### GCS 库

将 GCS Hadoop 库添加到我们的类路径

- com.google.cloud.bigdataoss:gcs-connector:1.6.0-hadoop2
