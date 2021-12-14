---
title: IBM Cloud Object Storage 文件系统
keywords: [ hudi, hive, ibm, cos, spark, presto]
summary: 在本页中，我们讨论在 IBM Cloud Object Storage 文件系统中配置 Hudi 。
last_modified_at: 2020-10-01T11:38:24-10:00
language: cn
---
在本页中，我们解释如何将你的 Hudi Spark 作业存储到 IBM Cloud Object Storage 当中。

## IBM COS 配置

Hudi 适配 IBM Cloud Object Storage 需要两项配置：

- 为 Hudi 添加 IBM COS 凭证
- 添加需要的 jar 包到类路径

### IBM Cloud Object Storage 凭证

在 IBM Cloud Object Storage 上使用 Hudi 的最简单的办法，就是使用 [Stocator](https://github.com/CODAIT/stocator) 的 Spark 存储连接器为 `SparkSession` 或 `SparkContext` 配置 IBM Cloud Object Storage 凭证。 Hudi 将自动拾取配置并告知 IBM Cloud Object Storage 。

或者，向你的 core-site.xml 文件中添加必要的配置，Hudi 可以从那里获取这些配置。用你的 IBM Cloud Object Storage 的 Bucket 名称替换 `fs.defaultFS` 以便 Hudi 能够在 Bucket 中读取/写入。

例如，使用 HMAC 密钥以及服务名 `myCOS` ：
```xml
  <property>
      <name>fs.defaultFS</name>
      <value>cos://myBucket.myCOS</value>
  </property>

  <property>
      <name>fs.cos.flat.list</name>
      <value>true</value>
  </property>

  <property>
	  <name>fs.stocator.scheme.list</name>
	  <value>cos</value>
  </property>

  <property>
	  <name>fs.cos.impl</name>
	  <value>com.ibm.stocator.fs.ObjectStoreFileSystem</value>
  </property>

  <property>
	  <name>fs.stocator.cos.impl</name>
	  <value>com.ibm.stocator.fs.cos.COSAPIClient</value>
  </property>

  <property>
	  <name>fs.stocator.cos.scheme</name>
	  <value>cos</value>
  </property>

  <property>
	  <name>fs.cos.myCos.access.key</name>
	  <value>ACCESS KEY</value>
  </property>

  <property>
	  <name>fs.cos.myCos.endpoint</name>
	  <value>http://s3-api.us-geo.objectstorage.softlayer.net</value>
  </property>

  <property>
	  <name>fs.cos.myCos.secret.key</name>
	  <value>SECRET KEY</value>
  </property>

```

更多信息请参考 Stocator [文档](https://github.com/CODAIT/stocator/blob/master/README.md) 。

### IBM Cloud Object Storage 库

将 IBM Cloud Object Storage Hadoop 库添加到我们的类路径中：

 - com.ibm.stocator:stocator:1.1.3
