---
title: Google Cloud
keywords: [ hudi, hive, google cloud, storage, spark, presto]
summary: In this page, we go over how to configure hudi with Google Cloud Storage.
last_modified_at: 2019-12-30T15:59:57-04:00
---
For Hudi storage on GCS, **regional** buckets provide an DFS API with strong consistency.

## GCS Configs

There are two configurations required for Hudi GCS compatibility:

- Adding GCS Credentials for Hudi
- Adding required jars to classpath

### GCS Credentials

Add the required configs in your core-site.xml from where Hudi can fetch them. Replace the `fs.defaultFS` with your GCS bucket name and Hudi should be able to read/write from the bucket.

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

### GCS Libs

GCS hadoop libraries to add to our classpath

- com.google.cloud.bigdataoss:gcs-connector:1.6.0-hadoop2
