---
title: GooseFS Filesystem
keywords: [ hudi, hive, tencent, goosefs, spark, presto]
summary: In this page, we go over how to configure Hudi with GooseFS filesystem.
last_modified_at: 2021-11-17T12:50:50-10:00
language: cn
---
这个页面描述了如何让你的Hudi spark任务使用Tencent Cloud GooseFS。

## Tencent Cloud GooseFS 部署

为了让Hudi使用GooseFS，需要增加两部分的配置:

- 为Hudi增加Tencent Cloud GooseFS的相关配置
- 增加Jar包的MVN依赖

### Tencent Cloud GooseFS 相关的配置

新增下面的配置到你的Hudi能访问的core-site.xml文件。使用你的GooseFS location 替换掉`fs.defaultFS`。主要Hudi就能读写GooseFS。


```xml
    <property>
        <name>fs.defaultFS</name>
        <value>gfs://localhost:9200</value>
        <description>GooseFS location</description>
    </property>

    <property>
        <name>fs.AbstractFileSystem.gfs.impl</name>
        <value>com.qcloud.cos.goosefs.hadoop.GooseFileSystem</value>
        <description>The implementation class of the GooseFS AbstractFileSystem.</description>
    </property>

    <property>
        <name>fs.gfs.impl</name>
        <value>com.qcloud.cos.goosefs.hadoop.FileSystem</value>
        <description>The implementation class of the GooseFS Filesystem.</description>
    </property>

```

### Tencent Cloud GooseFS Libs
添加GooseFS依赖jar包到classpath

- com.qcloud.cos:goosefs-client:1.1.0
