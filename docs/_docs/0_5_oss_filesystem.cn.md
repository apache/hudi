---
title: OSS Filesystem
keywords: hudi, hive, aliyun, oss, spark, presto
permalink: /cn/docs/oss_hoodie.html
summary: In this page, we go over how to configure Hudi with OSS filesystem.
last_modified_at: 2020-04-21T12:50:50-10:00
language: cn
---
这个页面描述了如何让你的Hudi spark任务使用Aliyun OSS存储。

## Aliyun OSS 部署

为了让Hudi使用OSS，需要增加两部分的配置:

- 为Hidi增加Aliyun OSS的相关配置
- 增加Jar包的MVN依赖

### Aliyun OSS 相关的配置

新增下面的配置到你的Hudi能访问的core-site.xml文件。使用你的OSS bucket name替换掉`fs.defaultFS`，使用OSS endpoint地址替换`fs.oss.endpoint`，使用OSS的key和secret分别替换`fs.oss.accessKeyId`和`fs.oss.accessKeySecret`。主要Hudi就能读写相应的bucket。

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

新增Aliyun hadoop的jar包的MVN依赖到pom.xml文件。由于hadoop-aliyun依赖hadoop 2.9.1+，因此你需要使用hadoop 2.9.1或更新的版本。

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
