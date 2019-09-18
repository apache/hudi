---
title: Quickstart
keywords: hudi, quickstart
tags: [quickstart]
sidebar: mydoc_sidebar
toc: false
permalink: /cn/quickstart.html
---
<br/>
为快速了解Hudi的功能，我们制作了一个基于Docker设置、所有依赖系统都在本地运行的[演示视频](https://www.youtube.com/watch？V=vhngusxdrd0)。
我们建议您复制相同的设置然后按照[这里](docker_demo.html)的步骤自己运行这个演示。另外，如果您正在寻找将现有数据迁移到Hudi的方法，请参阅[迁移指南](migration_guide.html)。

如果您已经安装了hive、hadoop和spark，那么请继续阅读。

## 下载Hudi

Git检出[代码](https://github.com/apache/incubator-hudi)或下载[最新版本](https://github.com/apache/incubator-hudi/archive/hudi-0.4.5.zip)

并通过命令行构建maven项目

```
$ mvn clean install -DskipTests -DskipITs
```

如果使用旧版本的Hive(Hive-1.2.1以前)，使用
```
$ mvn clean install -DskipTests -DskipITs -Dhive11
```

> 对于IDE，您可以将代码作为普通的maven项目导入IntelliJ。
您可能需要将spark jars文件夹添加到 'Module Settings' 下的项目依赖中，以便能够在IDE运行。


### 版本兼容性

Hudi要求在*nix系统上安装Java 8。 Hudi使用Spark-2.x版本。此外，我们已经验证Hudi可使用以下Hadoop/Hive/Spark组合。

| Hadoop | Hive  | Spark | 构建Hudi说明 |
| ---- | ----- | ---- | ---- |
| 2.6.0-cdh5.7.2 | 1.1.0-cdh5.7.2 | spark-2.[1-3].x | Use “mvn clean install -DskipTests -Dhadoop.version=2.6.0-cdh5.7.2 -Dhive.version=1.1.0-cdh5.7.2” |
| Apache hadoop-2.8.4 | Apache hive-2.3.3 | spark-2.[1-3].x | Use "mvn clean install -DskipTests" |
| Apache hadoop-2.7.3 | Apache hive-1.2.1 | spark-2.[1-3].x | Use "mvn clean install -DskipTests" |

> 如果你的环境有其他版本的hadoop/hive/spark，请使用Hudi并告诉我们是否存在任何问题。

## 生成样本数据集

### 环境变量

请根据您的设置配置以下环境变量。我们给出了一个CDH版本的示例设置

```
cd incubator-hudi 
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre/
export HIVE_HOME=/var/hadoop/setup/apache-hive-1.1.0-cdh5.7.2-bin
export HADOOP_HOME=/var/hadoop/setup/hadoop-2.6.0-cdh5.7.2
export HADOOP_INSTALL=/var/hadoop/setup/hadoop-2.6.0-cdh5.7.2
export HADOOP_CONF_DIR=$HADOOP_INSTALL/etc/hadoop
export SPARK_HOME=/var/hadoop/setup/spark-2.3.1-bin-hadoop2.7
export SPARK_INSTALL=$SPARK_HOME
export SPARK_CONF_DIR=$SPARK_HOME/conf
export PATH=$JAVA_HOME/bin:$HIVE_HOME/bin:$HADOOP_HOME/bin:$SPARK_INSTALL/bin:$PATH
```

### 运行HoodieJavaApp

运行 __hudi-spark/src/test/java/HoodieJavaApp.java__ 类，将两个提交(提交1 => 100个插入，提交2 => 100个更新到先前插入的100个记录)放到DFS/本地文件系统上。从命令行运行脚本

```
cd hudi-spark
./run_hoodie_app.sh --help
Usage: <main class> [options]
  Options:
    --help, -h
       Default: false
    --table-name, -n
       table name for Hudi sample table
       Default: hoodie_rt
    --table-path, -p
       path for Hudi sample table
       Default: file:///tmp/hoodie/sample-table
    --table-type, -t
       One of COPY_ON_WRITE or MERGE_ON_READ
       Default: COPY_ON_WRITE
```

这个类允许您选择表名、输出路径和存储类型。在您自己的应用程序中，请确保包含`hudi-spark`模块依赖并遵循类似模式通过数据源写入/读取数据集。

## 查询Hudi数据集

接下来，我们将样本数据集注册到Hive Metastore并尝试使用[Hive](#hive)、[Spark](#spark)和[Presto](#presto)进行查询

### 本地启动Hive Server

```
hdfs namenode # start name node
hdfs datanode # start data node

bin/hive --service metastore  # start metastore
bin/hiveserver2 \
  --hiveconf hive.root.logger=INFO,console \
  --hiveconf hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat \
  --hiveconf hive.stats.autogather=false \
  --hiveconf hive.aux.jars.path=/path/to/packaging/hudi-hive-bundle/target/hudi-hive-bundle-0.4.6-SNAPSHOT.jar

```

### 运行Hive同步工具
Hive同步工具将在hive Metastore中更新/创建必要的元数据(模式和分区)，这允许模式演变和新分区的增量添加。它使用一种增量方法，将最后一次的同步提交时间存储在TBLPROPERTIES中，并且只同步上次存储的同步提交时间后的提交。
每次写入后， [Spark Datasource](writing_data.html #datasource-writer)和[DeltaStreamer](writing_data.html＃deltastreamer)都可执行此操作。


```
cd hudi-hive
./run_sync_tool.sh
  --user hive
  --pass hive
  --database default
  --jdbc-url "jdbc:hive2://localhost:10010/"
  --base-path tmp/hoodie/sample-table/
  --table hoodie_test
  --partitioned-by field1,field2

```

> 若你想亲手运行。请参考[这个](https://cwiki.apache.org/confluence/display/HUDI/Registering+sample+dataset+to+Hive+via+beeline).

### HiveQL {#hive}

我们首先对表的最新提交的快照进行查询

```
hive> set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
hive> set hive.stats.autogather=false;
hive> add jar file:///path/to/hudi-hive-bundle-0.4.6-SNAPSHOT.jar;
hive> select count(*) from hoodie_test;
...
OK
100
Time taken: 18.05 seconds, Fetched: 1 row(s)
hive>
```

### SparkSQL {#spark}

只要你按上述方式让Hive工作起来，使用Spark将非常容易。只需启动Spark Shell，如下所示

```
$ cd $SPARK_INSTALL
$ spark-shell --jars $HUDI_SRC/packaging/hudi-spark-bundle/target/hudi-spark-bundle-0.4.6-SNAPSHOT.jar --driver-class-path $HADOOP_CONF_DIR  --conf spark.sql.hive.convertMetastoreParquet=false --packages com.databricks:spark-avro_2.11:4.0.0

scala> val sqlContext = new org.apache.spark.sql.SQLContext(sc)
scala> sqlContext.sql("show tables").show(10000)
scala> sqlContext.sql("describe hoodie_test").show(10000)
scala> sqlContext.sql("describe hoodie_test_rt").show(10000)
scala> sqlContext.sql("select count(*) from hoodie_test").show(10000)
```

### Presto {#presto}

Git检出OSS Presto上的 'master' 分支，构建并安装。

* 将hudi/packaging/hudi-presto-bundle/target/hudi-presto-bundle-*.jar 复制到 $PRESTO_INSTALL/plugin/hive-hadoop2/
* 启动服务器，您应该能够通过Presto查询到相同的Hive表

```
show columns from hive.default.hoodie_test;
select count(*) from hive.default.hoodie_test
```

### 增量 HiveQL

现在我们执行一个查询，以获取自上次提交以来已更改的行。

```
hive> set hoodie.hoodie_test.consume.mode=INCREMENTAL;
hive> set hoodie.hoodie_test.consume.start.timestamp=001;
hive> set hoodie.hoodie_test.consume.max.commits=10;
hive> select `_hoodie_commit_time`, rider, driver from hoodie_test where `_hoodie_commit_time` > '001' limit 10;
OK
All commits :[001, 002]
002	rider-001	driver-001
002	rider-001	driver-001
002	rider-002	driver-002
002	rider-001	driver-001
002	rider-001	driver-001
002	rider-002	driver-002
002	rider-001	driver-001
002	rider-002	driver-002
002	rider-002	driver-002
002	rider-001	driver-001
Time taken: 0.056 seconds, Fetched: 10 row(s)
hive>
hive>
```

> 注意：当前只有读优化视图支持。
