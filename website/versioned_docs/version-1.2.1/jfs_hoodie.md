---
title: JuiceFS
keywords: [ hudi, hive, juicefs, jfs, spark, flink ]
summary: In this page, we go over how to configure Hudi with JuiceFS file system.
last_modified_at: 2021-10-12T10:50:00+08:00
---

In this page, we explain how to use Hudi with JuiceFS.

## JuiceFS configs

[JuiceFS](https://github.com/juicedata/juicefs) is a high-performance distributed file system. Any data stored into JuiceFS, the data itself will be persisted in object storage (e.g. Amazon S3), and the metadata corresponding to the data can be persisted in various database engines such as Redis, MySQL, and TiKV according to the needs of the scene.

There are three configurations required for Hudi-JuiceFS compatibility:

1. Creating JuiceFS file system
2. Adding JuiceFS configuration for Hudi
3. Adding required JAR to `classpath`

### Creating JuiceFS file system

JuiceFS supports multiple [metadata engines](https://juicefs.com/docs/community/databases_for_metadata) such as Redis, MySQL, SQLite, and TiKV. And supports almost all [object storage](https://juicefs.com/docs/community/how_to_setup_object_storage#supported-object-storage) as data storage, e.g. Amazon S3, Google Cloud Storage, Azure Blob Storage.

The following example uses Redis as "Metadata Engine" and Amazon S3 as "Data Storage" in Linux environment.

#### Download JuiceFS client

```shell
$ JFS_LATEST_TAG=$(curl -s https://api.github.com/repos/juicedata/juicefs/releases/latest | grep 'tag_name' | cut -d '"' -f 4 | tr -d 'v')
$ wget "https://github.com/juicedata/juicefs/releases/download/v${JFS_LATEST_TAG}/juicefs-${JFS_LATEST_TAG}-linux-amd64.tar.gz"
```

#### Install JuiceFS client

```shell
$ mkdir juice && tar -zxvf "juicefs-${JFS_LATEST_TAG}-linux-amd64.tar.gz" -C juice
$ sudo install juice/juicefs /usr/local/bin
```

#### Format a JuiceFS file system

```shell
$ juicefs format \
    --storage s3 \
    --bucket https://<bucket>.s3.<region>.amazonaws.com \
    --access-key <your-access-key-id> \
    --secret-key <your-access-key-secret> \
    redis://:<password>@<redis-host>:6379/1 \
    myjfs
```

For more information, please refer to ["JuiceFS Quick Start Guide"](https://juicefs.com/docs/community/quick_start_guide).

### Adding JuiceFS configuration for Hudi

Add the required configurations in your `core-site.xml` from where Hudi can fetch them.

```xml
<property>
    <name>fs.defaultFS</name>
    <value>jfs://myjfs</value>
    <description>Optional, you can also specify full path "jfs://myjfs/path-to-dir" with location to use JuiceFS</description>
</property>
<property>
    <name>fs.jfs.impl</name>
    <value>io.juicefs.JuiceFileSystem</value>
</property>
<property>
    <name>fs.AbstractFileSystem.jfs.impl</name>
    <value>io.juicefs.JuiceFS</value>
</property>
<property>
    <name>juicefs.meta</name>
    <value>redis://:<password>@<redis-host>:6379/1</value>
</property>
<property>
    <name>juicefs.cache-dir</name>
    <value>/path-to-your-disk</value>
</property>
<property>
    <name>juicefs.cache-size</name>
    <value>1024</value>
</property>
<property>
    <name>juicefs.access-log</name>
    <value>/tmp/juicefs.access.log</value>
</property>
```

You can visit [here](https://juicefs.com/docs/community/hadoop_java_sdk#client-configurations) for more configuration information.

### Adding JuiceFS Hadoop Java SDK

You can download latest JuiceFS Hadoop Java SDK from [here](http://github.com/juicedata/juicefs/releases/latest) (download the file called like `juicefs-hadoop-X.Y.Z-linux-amd64.jar`), and place it to the `classpath`. You can also [compile](https://juicefs.com/docs/community/hadoop_java_sdk#client-compilation) it by yourself.

For example, if you use Hudi in Spark, please put the JAR in `$SPARK_HOME/jars`.
