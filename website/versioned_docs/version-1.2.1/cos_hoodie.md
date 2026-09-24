---
title: Tencent Cloud
keywords: [ hudi, hive, tencent, cos, spark, presto]
summary: In this page, we go over how to configure Hudi with COS filesystem.
last_modified_at: 2020-04-21T11:38:24-10:00
---
In this page, we explain how to get your Hudi spark job to store into Tencent Cloud COS.

## Tencent Cloud COS configs

There are two configurations required for Hudi-COS compatibility:

- Adding Tencent Cloud COS Credentials for Hudi
- Adding required Jars to classpath

### Tencent Cloud COS Credentials

Add the required configs in your core-site.xml from where Hudi can fetch them. Replace the `fs.defaultFS` with your COS bucket name, replace `fs.cosn.userinfo.secretId` with your COS secret Id, replace `fs.cosn.userinfo.secretKey` with your COS key. Hudi should be able to read/write from the bucket.

```xml
    <property>
        <name>fs.defaultFS</name>
        <value>cosn://bucketname</value>
        <description>COS bucket name</description>
    </property>

    <property>
        <name>fs.cosn.userinfo.secretId</name>
        <value>cos-secretId</value>
        <description>Tencent Cloud Secret Id</description>
    </property>

    <property>
        <name>fs.cosn.userinfo.secretKey</name>
        <value>cos-secretkey</value>
        <description>Tencent Cloud Secret Key</description>
    </property>

    <property>
        <name>fs.cosn.bucket.region</name>
        <value>ap-region</value>
        <description>The region where the bucket is located.</description>
    </property>

    <property>
        <name>fs.cosn.bucket.endpoint_suffix</name>
        <value>cos.endpoint.suffix</value>
        <description>
          COS endpoint to connect to. 
          For public cloud users, it is recommended not to set this option, and only the correct area field is required.
        </description>
    </property>

    <property>
        <name>fs.cosn.impl</name>
        <value>org.apache.hadoop.fs.CosFileSystem</value>
        <description>The implementation class of the CosN Filesystem.</description>
    </property>

    <property>
        <name>fs.AbstractFileSystem.cosn.impl</name>
        <value>org.apache.hadoop.fs.CosN</value>
        <description>The implementation class of the CosN AbstractFileSystem.</description>
    </property>

```

### Tencent Cloud COS Libs
COS hadoop libraries to add to our classpath

- org.apache.hadoop:hadoop-cos:2.8.5
