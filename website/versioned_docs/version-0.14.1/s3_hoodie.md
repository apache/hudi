---
title: AWS S3 
keywords: [ hudi, hive, aws, s3, spark, presto]
summary: In this page, we go over how to configure Hudi with S3 filesystem.
last_modified_at: 2019-12-30T15:59:57-04:00
---
In this page, we explain how to get your Hudi spark job to store into AWS S3.

## AWS configs

There are two configurations required for Hudi-S3 compatibility:

- Adding AWS Credentials for Hudi
- Adding required Jars to classpath

### AWS Credentials

The simplest way to use Hudi with S3, is to configure your `SparkSession` or `SparkContext` with S3 credentials. Hudi will automatically pick this up and talk to S3.

Alternatively, add the required configs in your core-site.xml from where Hudi can fetch them. Replace the `fs.defaultFS` with your S3 bucket name and Hudi should be able to read/write from the bucket.

```xml
  <property>
    <name>fs.defaultFS</name>
    <value>s3://ysharma</value>
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
    <name>fs.s3a.awsAccessKeyId</name>
    <value>AWS_KEY</value>
  </property>

  <property>
    <name>fs.s3a.awsSecretAccessKey</name>
    <value>AWS_SECRET</value>
  </property>

  <property>
    <name>fs.s3a.endpoint</name>
    <value>http://IP-Address:Port</value>
  </property>

  <property>
    <name>fs.s3a.path.style.access</name>
    <value>true</value>
  </property>

  <property>
    <name>fs.s3a.signing-algorithm</name>
    <value>S3SignerType</value>
  </property>
```


Utilities such as hudi-cli or Hudi Streamer tool, can pick up s3 creds via environmental variable prefixed with `HOODIE_ENV_`. For e.g below is a bash snippet to setup
such variables and then have cli be able to work on datasets stored in s3

```java
export HOODIE_ENV_fs_DOT_s3a_DOT_access_DOT_key=$accessKey
export HOODIE_ENV_fs_DOT_s3a_DOT_secret_DOT_key=$secretKey
export HOODIE_ENV_fs_DOT_s3_DOT_awsAccessKeyId=$accessKey
export HOODIE_ENV_fs_DOT_s3_DOT_awsSecretAccessKey=$secretKey
```



### AWS Libs

AWS hadoop libraries to add to our classpath

 - com.amazonaws:aws-java-sdk:1.10.34
 - org.apache.hadoop:hadoop-aws:2.7.3

AWS glue data libraries are needed if AWS glue data is used

 - com.amazonaws.glue:aws-glue-datacatalog-hive2-client:1.11.0
 - com.amazonaws:aws-java-sdk-glue:1.11.475

## AWS S3 Versioned Bucket

With versioned buckets any object deleted creates a [Delete Marker](https://docs.aws.amazon.com/AmazonS3/latest/userguide/DeleteMarker.html), as Hudi cleans up files using [Cleaner utility](hoodie_cleaner) the number of Delete Markers increases over time.
It is important to configure the [Lifecycle Rule](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html) correctly
to clean up these delete markers as the List operation can choke if the number of delete markers reaches 1000.
We recommend cleaning up Delete Markers after 1 day in Lifecycle Rule.