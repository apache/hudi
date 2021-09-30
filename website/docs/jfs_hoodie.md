---
title: JuiceFS 
keywords: [ hudi, hive, jfs, spark, flink]
summary: On this page, we go over how to configure Hudi with JuiceFS.
last_modified_at: 2021-09-30T17:24:24-10:00
---
On this page, we explain how to use Hudi with JuiceFS.

## JuiceFS Preparing

JuiceFS is a high-performance distributed file system. Any data stored into JuiceFS, the data itself will be persisted in object storage (e.g. Amazon S3), and the metadata corresponding to the data can be persisted in various database engines such as Redis, MySQL, and TiKV according to the needs of the scene.

There are three configurations required for Hudi-JuiceFS compatibility:

- Creating JuiceFS
- Adding JuiceFS configuration for Hudi
- Adding required jar to `classpath`

### Creating JuiceFS

JuiceFS supports multiple engines such as Redis, MySQL, SQLite, and TiKV.

This example uses Redis as Meta Engine and AWS S3 as Data Storage in Linux env.

- Download
```shell
JFS_LATEST_TAG=$(curl -s https://api.github.com/repos/juicedata/juicefs/releases/latest | grep 'tag_name' | cut -d '"' -f 4 | tr -d 'v')
wget "https://github.com/juicedata/juicefs/releases/download/v${JFS_LATEST_TAG}/juicefs-${JFS_LATEST_TAG}-linux-amd64.tar.gz"
```

- Install
```shell
mkdir juice && tar -zxvf "juicefs-${JFS_LATEST_TAG}-linux-amd64.tar.gz" -C juice
sudo install juice/juicefs /usr/local/bin
```

- Format a filesystem
```shell
juicefs format \
    --storage s3 \
    --bucket https://<your-bucket-name> \
    --access-key <your-access-key-id> \
    --secret-key <your-access-key-secret> \
    redis://:<password>@<redis-host>:6379/1 \
    myjfs 
```

### JuiceFS configuration

Add the required configurations in your core-site.xml from where Hudi can fetch them.

```xml
<property>
    <name>fs.defaultFS</name>
    <value>jfs://myfs</value>
    <description>Optional, you can also specify full path "jfs://myfs/path-to-dir" with location to use JuiceFS</description>
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

### JuiceFS Hadoop SDK
You can download the JuiceFS java Hadoop SDK jar from [here](https://github.com/juicedata/juicefs/releases/download/v0.17.0/juicefs-hadoop-0.17.0-linux-amd64.jar), and place it to the `classpath`. 
You can also visit [JuiceFS Releases](https://github.com/juicedata/juicefs/releases)) to get the latest version or compile by your self.

For example:
- $SPARK_HOME/jars


