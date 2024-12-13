---
title: BOS Filesystem
keywords: [ hudi, hive, baidu, bos, spark, presto]
summary: In this page, we go over how to configure Hudi with BOS filesystem.
last_modified_at: 2021-06-09T11:38:24-10:00
language: cn
---
这个页面描述了如何让你的Hudi任务使用Baidu BOS存储。

## Baidu BOS 部署

为了让Hudi使用BOS，需要增加两部分的配置:

- 为Hudi增加Baidu BOS的相关配置
- 增加Jar包到classpath

### Baidu BOS 相关的配置

新增下面的配置到你的Hudi能访问的core-site.xml文件。使用你的BOS bucket name替换掉`fs.defaultFS`，使用BOS endpoint地址替换`fs.bos.endpoint`，使用BOS的key和secret分别替换`fs.bos.access.key`和`fs.bos.secret.access.key`，这样Hudi就能读写相应的bucket。

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

### Baidu BOS Libs

新增Baidu hadoop的jar包添加到classpath.

- com.baidubce:bce-java-sdk:0.10.165
- bos-hdfs-sdk-1.0.2-community.jar 

可以从[这里](https://sdk.bce.baidu.com/console-sdk/bos-hdfs-sdk-1.0.2-community.jar.zip) 下载bos-hdfs-sdk jar包，然后解压。
