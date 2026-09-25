---
title: GooseFS Filesystem
keywords: [ hudi, hive, tencent, goosefs, gfs, spark, presto]
summary: In this page, we go over how to configure Hudi with GooseFS filesystem.
last_modified_at: 2026-08-13T21:00:51+05:30
---
In this page, we explain how to get your Hudi jobs to read from and write to Tencent Cloud GooseFS.

GooseFS is a distributed caching filesystem from Tencent Cloud that fronts COS. Hudi recognises its `gfs` scheme, so a
table can be stored at a `gfs://` path the same way as on any other supported filesystem.

## GooseFS configs

There are two configurations required for Hudi-GooseFS compatibility:

- Adding the GooseFS filesystem implementations for Hudi
- Adding required Jars to classpath

### GooseFS filesystem implementations

Add the required configs in your core-site.xml from where Hudi can fetch them, so that the `gfs` scheme resolves to the
GooseFS client.

```xml
    <property>
        <name>fs.gfs.impl</name>
        <value>com.qcloud.cos.goosefs.hadoop.FileSystem</value>
        <description>The implementation class of the GooseFS Filesystem.</description>
    </property>

    <property>
        <name>fs.AbstractFileSystem.gfs.impl</name>
        <value>com.qcloud.cos.goosefs.hadoop.GooseFileSystem</value>
        <description>The implementation class of the GooseFS AbstractFileSystem.</description>
    </property>

```

Address the table through the GooseFS master, whose default RPC port is `9200`, and Hudi should be able to read and
write there:

```
gfs://<goosefs-master>:9200/<path>/<to>/<table>
```

Setting `fs.defaultFS` to a `gfs://` location also works, if you want GooseFS to be the default filesystem for the job,
but it is not required: a fully qualified `gfs://` base path is enough.

### GooseFS Libs

GooseFS client library to add to our classpath

 - com.qcloud.cos:goosefs-client:1.4.9.1

The client jar that ships with a GooseFS installation, `${GOOSEFS_HOME}/client/goosefs-x.x.x-client.jar`, can be used in
place of the Maven artifact. On Spark it is passed through `spark.driver.extraClassPath` and
`spark.executor.extraClassPath`.

:::note
GooseFS is not marked as supporting atomic file creation, so [`FileSystemBasedLockProvider`](concurrency_control.md)
refuses to start on it with `Unsupported scheme :gfs, since this fs can not support atomic creation`. To use that lock
provider on GooseFS, add the scheme to `hoodie.fs.atomic_creation.support`:

```properties
hoodie.fs.atomic_creation.support=gfs
```

The other lock providers are unaffected.
:::
