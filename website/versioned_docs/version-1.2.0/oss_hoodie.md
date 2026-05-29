---
title: Alibaba Cloud
keywords: [ hudi, hive, aliyun, oss, spark, presto]
summary: In this page, we go over how to configure Hudi with OSS filesystem.
last_modified_at: 2020-04-21T11:38:24-10:00
---
In this page, we explain how to get your Hudi spark job to store into Aliyun OSS.

## Aliyun OSS configs

There are two configurations required for Hudi-OSS compatibility:

- Adding Aliyun OSS Credentials for Hudi
- Adding required Jars to classpath

### Aliyun OSS Credentials

Add the required configs in your core-site.xml from where Hudi can fetch them. Replace the `fs.defaultFS` with your OSS bucket name, replace `fs.oss.endpoint` with your OSS endpoint, replace `fs.oss.accessKeyId` with your OSS key, replace `fs.oss.accessKeySecret` with your OSS secret. Hudi should be able to read/write from the bucket.

```xml
<property>
  <name>fs.defaultFS</name>
  <value>oss://bucketname/</value>
</property>

<property>
  <name>fs.oss.endpoint</name>
  <value>oss-endpoint-address</value>
  <description>Aliyun OSS endpoint to connect to.</description>
</property>

<property>
  <name>fs.oss.accessKeyId</name>
  <value>oss_key</value>
  <description>Aliyun access key ID</description>
</property>

<property>
  <name>fs.oss.accessKeySecret</name>
  <value>oss-secret</value>
  <description>Aliyun access key secret</description>
</property>

<property>
  <name>fs.oss.impl</name>
  <value>org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem</value>
</property>
```

### Aliyun OSS Libs

Aliyun hadoop libraries jars to add to our pom.xml. Since hadoop-aliyun depends on the version of hadoop 2.9.1+, you need to use the version of hadoop 2.9.1 or later.

```xml
<dependency>
  <groupId>org.apache.hadoop</groupId>
  <artifactId>hadoop-aliyun</artifactId>
  <version>3.2.1</version>
</dependency>
<dependency>
  <groupId>com.aliyun.oss</groupId>
  <artifactId>aliyun-sdk-oss</artifactId>
  <version>3.8.1</version>
</dependency>
<dependency>
  <groupId>org.jdom</groupId>
  <artifactId>jdom</artifactId>
  <version>1.1</version>
</dependency>
```
