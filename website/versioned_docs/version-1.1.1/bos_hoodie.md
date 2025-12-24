---
title: Baidu Cloud
keywords: [ hudi, hive, baidu, bos, spark, presto]
summary: In this page, we go over how to configure Hudi with bos filesystem.
last_modified_at: 2021-06-09T11:38:24-10:00
---
In this page, we explain how to get your Hudi job to store into Baidu BOS.

## Baidu BOS configs

There are two configurations required for Hudi-BOS compatibility:

- Adding Baidu BOS Credentials for Hudi
- Adding required Jars to classpath

### Baidu BOS Credentials

Add the required configs in your core-site.xml from where Hudi can fetch them. Replace the `fs.defaultFS` with your BOS bucket name, replace `fs.bos.endpoint` with your bos endpoint, replace `fs.bos.access.key` with your bos key, replace `fs.bos.secret.access.key` with your bos secret key. Hudi should be able to read/write from the bucket.

```xml
<property>
  <name>fs.defaultFS</name>
  <value>bos://bucketname/</value>
</property>

<property>
  <name>fs.bos.endpoint</name>
  <value>bos-endpoint-address</value>
  <description>Baidu bos endpoint to connect to,for example : http://bj.bcebos.com</description>
</property>

<property>
  <name>fs.bos.access.key</name>
  <value>bos-key</value>
  <description>Baidu access key</description>
</property>

<property>
  <name>fs.bos.secret.access.key</name>
  <value>bos-secret-key</value>
  <description>Baidu secret key.</description>
</property>

<property>
  <name>fs.bos.impl</name>
  <value>org.apache.hadoop.fs.bos.BaiduBosFileSystem</value>
</property>
```

### Baidu bos Libs

Baidu hadoop libraries jars to add to our classpath

- com.baidubce:bce-java-sdk:0.10.165
- bos-hdfs-sdk-1.0.2-community.jar 

You can  download the bos-hdfs-sdk jar from [here](https://sdk.bce.baidu.com/console-sdk/bos-hdfs-sdk-1.0.2-community.jar.zip) , and then unzip it.