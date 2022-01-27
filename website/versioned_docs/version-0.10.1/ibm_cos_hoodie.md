---
title: IBM Cloud
keywords: [ hudi, hive, ibm, cos, spark, presto]
summary: In this page, we go over how to configure Hudi with IBM Cloud Object Storage filesystem.
last_modified_at: 2020-10-01T11:38:24-10:00
---
In this page, we explain how to get your Hudi spark job to store into IBM Cloud Object Storage.

## IBM COS configs

There are two configurations required for Hudi-IBM Cloud Object Storage compatibility:

- Adding IBM COS Credentials for Hudi
- Adding required Jars to classpath

### IBM Cloud Object Storage Credentials

Simplest way to use Hudi with IBM Cloud Object Storage, is to configure your `SparkSession` or `SparkContext` with IBM Cloud Object Storage credentials using [Stocator](https://github.com/CODAIT/stocator) storage connector for Spark. Hudi will automatically pick this up and talk to IBM Cloud Object Storage.

Alternatively, add the required configs in your `core-site.xml` from where Hudi can fetch them. Replace the `fs.defaultFS` with your IBM Cloud Object Storage bucket name and Hudi should be able to read/write from the bucket.

For example, using HMAC keys and service name `myCOS`:
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

For more options see Stocator [documentation](https://github.com/CODAIT/stocator/blob/master/README.md).

### IBM Cloud Object Storage Libs

IBM Cloud Object Storage hadoop libraries to add to our classpath

 - com.ibm.stocator:stocator:1.1.3
