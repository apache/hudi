---
version: 0.6.0
title: COS Filesystem
keywords: hudi, hive, tencent, cos, spark, presto
permalink: /cn/docs/0.6.0-cos_hoodie.html
summary: In this page, we go over how to configure Hudi with COS filesystem.
last_modified_at: 2020-04-21T12:50:50-10:00
language: cn
---
这个页面描述了如何让你的Hudi spark任务使用Tencent Cloud COS存储。

## Tencent Cloud COS 部署

为了让Hudi使用COS，需要增加两部分的配置:

- 为Hidi增加Tencent Cloud COS的相关配置
- 增加Jar包的MVN依赖

### Tencent Cloud COS 相关的配置

新增下面的配置到你的Hudi能访问的core-site.xml文件。使用你的COS bucket name替换掉`fs.defaultFS`，使用COS的key和secret分别替换`fs.cosn.userinfo.secretKey`和`fs.cosn.userinfo.secretId`。主要Hudi就能读写相应的bucket。


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
添加COS依赖jar包到classpath

- org.apache.hadoop:hadoop-cos:2.8.5
