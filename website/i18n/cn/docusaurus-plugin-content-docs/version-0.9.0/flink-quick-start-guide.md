---
title: "Flink 指南"
toc: true
last_modified_at: 2021-08-16T12:31:57-04:00
language: cn
---

本指南提供了使用 Flink SQL 操作 Hudi 的文档。阅读本指南，您可以学习如何快速开始使用 Flink 读写 Hudi，同时对配置和任务优化有更深入的了解：

- **快速开始** ：通过阅读 [快速开始](#快速开始)，你可以快速开始使用 Flink sql client 去读写 Hudi
- **配置** ：对于 [Flink 配置](#flink-配置)，使用 `$FLINK_HOME/conf/flink-conf.yaml` 来配置。 对于任意一个作业的配置，通过[表参数](#表参数)来设置
- **写功能** ：Flink 支持多种写功能用例，例如 [离线批量导入](#离线批量导入)，[全量接增量](#全量接增量)，[Changelog 模式](#changelog-模式)，[Insert 模式](#insert-模式) 和 [离线 Compaction](#离线-compaction)
- **查询功能** ：Flink 支持多种查询功能用例，例如 [Hive 查询](#hive-查询)， [Presto 查询](#presto-查询)
- **优化** ：针对 Flink 读写 Hudi 的操作，本指南提供了一些优化建议，例如 [内存优化](#内存优化) 和 [写入限流](#写入限流)

## 快速开始

### 安装

我们推荐使用 [Flink Sql Client](https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/dev/table/sqlclient/) 来读写 Hudi，因为 Flink
sql client 对于 SQL 用户来说更容易上手。 

#### 步骤1 下载 Flink jar
我们推荐使用 Flink-1.12.x 来读写 Hudi。 你可以按照 [Flink 安装文档](https://flink.apache.org/downloads) 的指导来安装 Flink。 `hudi-flink-bundle.jar`
使用的是 scala 2.11，所以我们推荐 Flink-1.12.x 配合 scala 2.11 来使用。

#### 步骤2 启动 Flink 集群
在 Hadoop 环境下启动 standalone 的 Flink 集群。
在你启动 Flink 集群前，我们推荐先配置如下参数：

- 在 `$FLINK_HOME/conf/flink-conf.yaml` 中添加配置：`taskmanager.numberOfTaskSlots: 4`
- 在 `$FLINK_HOME/conf/flink-conf.yaml` 中，根据数据量大小和集群大小来添加其他的 [Flink 配置](#flink-配置)
- 在 `$FLINK_HOME/conf/workers` 中添加4核 `localhost` 来保证我们本地集群中有4个 workers

启动集群：

```bash
# HADOOP_HOME 是 Hadoop 的根目录。
export HADOOP_CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`

# 启动 Flink standalone 集群
./bin/start-cluster.sh
```
#### 步骤3 启动 Flink SQL client

Hudi 将 Flink 板块单独打包为 `hudi-flink-bundle.jar`，该 Jar 包需要在启动的时候加载。
你可以在 `hudi-source-dir/packaging/hudi-flink-bundle` 下手动的打包这个 Jar 包，或者从 [Apache Official Repository](https://repo.maven.apache.org/maven2/org/apache/hudi/hudi-flink-bundle_2.11/)
中下载。

启动 Flink SQL Client：

```bash
# HADOOP_HOME 是 Hadoop 的根目录。
export HADOOP_CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`

./bin/sql-client.sh embedded -j .../hudi-flink-bundle_2.1?-*.*.*.jar shell
```

<div className="notice--info">
  <h4>小提示：</h4>
<ul>
  <li>为了兼容大部分的对象存储，我们推荐使用 Hadoop 2.9 x+的 Hadoop 版本</li>
  <li>flink-parquet 和 flink-avro 格式已经打包在 hudi-flink-bundle.jar 中了</li>
</ul>
</div>

根据下面的不同功能来设置表名，存储路径和操作类型。
Flink SQL Client 是逐行执行 SQL 的。

### 插入数据

先创建一个 Flink Hudi 表，然后在通过下面的 `VALUES` 语句往该表中插入数据。

```sql
-- 为了更直观的显示结果，推荐把 CLI 的输出结果模式设置为 tableau。
set execution.result-mode=tableau;

CREATE TABLE t1(
  uuid VARCHAR(20),
  name VARCHAR(10),
  age INT,
  ts TIMESTAMP(3),
  `partition` VARCHAR(20)
)
PARTITIONED BY (`partition`)
WITH (
  'connector' = 'hudi',
  'path' = 'schema://base-path',
  'table.type' = 'MERGE_ON_READ' -- 创建一个 MERGE_ON_READ类型的表，默认是 COPY_ON_ERITE
);

-- 使用 values 语句插入数据
INSERT INTO t1 VALUES
  ('id1','Danny',23,TIMESTAMP '1970-01-01 00:00:01','par1'),
  ('id2','Stephen',33,TIMESTAMP '1970-01-01 00:00:02','par1'),
  ('id3','Julian',53,TIMESTAMP '1970-01-01 00:00:03','par2'),
  ('id4','Fabian',31,TIMESTAMP '1970-01-01 00:00:04','par2'),
  ('id5','Sophia',18,TIMESTAMP '1970-01-01 00:00:05','par3'),
  ('id6','Emma',20,TIMESTAMP '1970-01-01 00:00:06','par3'),
  ('id7','Bob',44,TIMESTAMP '1970-01-01 00:00:07','par4'),
  ('id8','Han',56,TIMESTAMP '1970-01-01 00:00:08','par4');
```

### 查询数据

```sql
-- 从 Hudi 表中查询
select * from t1;
```
该查询语句提供的是 快照读（Snapshot Querying）。
如果想了解更多关于表类型和查询类型的介绍，可以参考文档 [表类型和查询类型](/docs/concepts#table-types--queries)

### 更新数据

数据的更新和插入数据类似：

```sql
-- 这条语句会更新 key 为 'id1' 的记录
insert into t1 values
  ('id1','Danny',27,TIMESTAMP '1970-01-01 00:00:01','par1');
```

需要注意的是：现在使用的存储类型为 `Append`。通常我们都是使用 apennd 模式，除非你是第一次创建这个表。 再次 [查询数据](#查询数据) 就会显示更新后的结果。
每一次的插入操作都会在时间轴上生成一个带时间戳的新的 [commit](/docs/concepts)，在元数据字段 `_hoodie_commit_time` 和同一 `_hoodie_record_key` 的 `age`字段中查看更新。

### 流式查询

Hudi Flink 也有能力来查询从指定时间戳开始的流式记录集合。 该功能可以使用Hudi的流式查询，只需要提供一个查询开始的时间戳就可以完成查询。如果我们需要的
是指定时间戳后的所有数据，我们就不需要指定结束时间。

```sql
CREATE TABLE t1(
  uuid VARCHAR(20),
  name VARCHAR(10),
  age INT,
  ts TIMESTAMP(3),
  `partition` VARCHAR(20)
)
PARTITIONED BY (`partition`)
WITH (
  'connector' = 'hudi',
  'path' = 'oss://vvr-daily/hudi/t1',
  'table.type' = 'MERGE_ON_READ',
  'read.streaming.enabled' = 'true',  -- 该参数开启流式查询
  'read.streaming.start-commit' = '20210316134557' -- 指定开始的时间戳
  'read.streaming.check-interval' = '4' -- 指定检查新的commit的周期，默认是60秒
);

-- 开启流式的查询
select * from t1;
``` 
上述的查询会查询出 `read.streaming.start-commit` 时间戳后的所有数据。该功能的特殊在于可以同时在流和批的 pipeline 上执行。

### 删除数据 {#deletes}

在 [流式查询](#流式查询) 中使用数据时，Hudi Flink 源还可以接受来自底层数据源的更改日志，然后可以按行级别应用更新和删除。所以，你可以在 Hudi 上同步各种 RDBMS 的近实时快照。

## Flink 配置

在使用 Flink 前，你需要在 `$FLINK_HOME/conf/flink-conf.yaml` 中设置一些全局的 Flink 配置。

### 并行度

|  名称  | 默认值 | 类型 | 描述 |
|  -----------  | -------  | ------- | ------- |
| `taskmanager.numberOfTaskSlots` | `1` | `Integer` | 单个 TaskManager 可以运行的并行 task 数。我们建议将该值设置为 > 4，实际值需根据数据量进行设置 |
| `parallelism.default` | `1` | `Integer` | 当用户为指定算子并行度时，会使用这个并行度（默认值是1）。例如 [`write.bucket_assign.tasks`](#并行度-1) 没有设置，就会使用这个默认值 |

### 内存

|  名称  | 默认值 | 类型 | 描述 |
|  -----------  | -------  | ------- | ------- |
| `jobmanager.memory.process.size` | `(none)` | `MemorySize` | JobManager 的总进程内存大小。代表 JobManager JVM 进程消耗的所有内存，包括总的 Flink 内存、JVM 元空间和 JVM 开销 |
| `taskmanager.memory.task.heap.size` | `(none)` | `MemorySize` | TaskExecutor 的堆内存大小。这是为写缓存保留的 JVM 堆内存大小 |
| `taskmanager.memory.managed.size`  |  `(none)`  | `MemorySize` | 这是内存管理器管理的堆外内存的大小，用于排序和 RocksDB 状态后端。如果选择 RocksDB 作为状态后端，则需要设置此内存 |

### Checkpoint

|  名称  | 默认值 | 类型 | 描述 |
|  -----------  | -------  | ------- | ------- |
| `execution.checkpointing.interval` | `(none)` | `Duration` | 设置该值的方式为 `execution.checkpointing.interval = 150000ms`，其中150000ms = 2.5min。 设置这个参数等同于开启了 Checkpoint |
| `state.backend` | `(none)` | `String` | 保存状态信息的状态后端。 我们推荐设置状态后端为 `rocksdb` ：`state.backend: rocksdb`  |
| `state.backend.rocksdb.localdir` | `(none)` | `String` | RocksDB 存储状态信息的路径 |
| `state.checkpoints.dir` | `(none)` | `String` | Checkpoint 的默认路径，用于在支持 Flink 的文件系统中存储检查点的数据文件和元数据。存储路径必须可从所有参与进程/节点（即所有 TaskManager 和 JobManager）访问，如 HDFS 和 OSS 路径 |
| `state.backend.incremental`  |  `false`  | `Boolean` | 选项状态后端是否创建增量检查点。对于增量检查点，只存储与前一个检查点的差异，而不是完整的检查点状态。如果存储状态设置为 `rocksdb`，建议打开这个选项 |

## 表参数

对于单个作业的配置，我们可以在 Flink SQL 语句的 [`WITH`](#表参数)中设置。
所以，作业级别的配置如下：

### 内存

:::note
我们在内存调优的时候需要先关注 TaskManager 的数量和 [内存](#内存) 配置，以及 write task 的并发（`write.tasks: 4` 的值），确认每个 write task 能够分配到足够的内存，
再考虑以下相关的内存参数设置。
:::

|  名称  | 说明 | 默认值 | 备注 |
|  -----------  | -------  | ------- | ------- |
| `write.task.max.size` | 一个 write task 的最大可用内存。 默认为 `1024MB` | `1024D` | 当前预留给 write buffer 的内存为 `write.task.max.size` - `compaction.max_memory`。 当 write task 的内存 buffer 达到该阈值后会将内存里最大的 buffer flush |
| `write.batch.size`  | Flink 的 write task 为了提高写数据效率，会按照写 bucket 提前缓存数据，每个 bucket 的数据在内存达到阈值之前会一直缓存在内存中，当阈值达到会把数据 buffer 传递给 Hudi 的 writer 执行写操作。默认为 `64MB` | `64D` |  推荐使用默认值 |
| `write.log_block.size` | Hudi 的 log writer 在收到 write task 的数据后不会马上 flush 数据，writer 是以 LogBlock 为单位往磁盘刷数据的，在 LogBlock 攒够之前 records 会以序列化字节的形式缓存在 writer 内部。默认为 `128MB`  | `128` |  推荐使用默认值  |
| `write.merge.max_memory` | 当表的类型为 `COPY_ON_WRITE`，Hudi 会合并增量数据和 base file 中的数据。增量的数据会缓存在内存的 map 结构里，这个 map 是可溢写的，这个参数控制了 map 可以使用的堆内存大小。 默认为 `100MB`  | `100` | 推荐使用默认值 |
| `compaction.max_memory` | 同 `write.merge.max_memory` 类似，只是发生在压缩时。默认为 `100MB` | `100` | 如果是在线 compaction，资源充足时可以开大些，比如 `1024MB` |

### 并行度

|  名称  | 说明 | 默认值 | 备注 |
|  -----------  | -------  | ------- | ------- |
| `write.tasks` | writer 的并发，每个 writer 顺序写 `1`~`N` 个 buckets。默认为 `4` | `4` | 增加这个并发，对小文件个数没影响 |
| `write.bucket_assign.tasks`  |  bucket assigner 的并发，默认使用 Flink 默认并发 `parallelism.default`  | [`parallelism.default`](#并行度) |  增加并发同时增加了并发写的 bucekt 数，也就变相增加了小文件(小 bucket)数  |
| `write.index_boostrap.tasks` |  Index bootstrap 算子的并发，增加并发可以加快 bootstrap 阶段的效率，bootstrap 阶段会阻塞 Checkpoint，因此需要设置多一些的 Checkpoint 失败容忍次数。 默认使用 Flink 默认并发 `parallelism.default` | [`parallelism.default`](#并行度) | 只在 `index.bootsrap.enabled` 为 `true` 时生效 |
| `read.tasks` | 读算子的并发（batch 和 stream。默认为 `4`  | `4` |  |
| `compaction.tasks` | 在线 compaction 的并发。默认为 `10` | `10` | 在线 compaction 比较耗费资源，建议使用 [`离线 compaction`](#离线-compaction) |

### Compaction

:::note
这些参数都只服务于在线 compaction。
:::

:::note
通过设置`compaction.async.enabled` = `false` 来关闭在线 compaction，但是 `compaction.schedule.enable` 仍然建议开启。之后通过[`离线 compaction`](#离线-compaction) 直接执行在线 compaction 产生的 compaction plan。 
:::

|  名称  | 说明 | 默认值 | 备注 |
|  -----------  | -------  | ------- | ------- |
| `compaction.schedule.enabled` | 是否阶段性生成 compaction plan | `true` |  建议开启，即使 `compaction.async.enabled` = `false` |
| `compaction.async.enabled`  |  是否开启异步压缩，MOR 表时默认开启 | `true` | 通过关闭此参数来关闭 `在线 compaction` |
| `compaction.trigger.strategy`  | 压缩策略 | `num_commits` | 可选择的策略有 `num_commits`：达到 `N` 个 delta commits 时触发 compaction; `time_elapsed`：距离上次 compaction 超过 `N` 秒触发 compaction ; `num_and_time`：`NUM_COMMITS` 和 `TIME_ELAPSED` 同时满足; `num_or_time`：`NUM_COMMITS` 或者 `TIME_ELAPSED` 中一个满足 |
| `compaction.delta_commits` | 默认策略，`5` 个 delta commits 触发一次压缩 | `5` | -- |
| `compaction.delta_seconds`  |  默认 `1` 小时触发一次压缩 | `3600` | -- |
| `compaction.max_memory` | compaction 的 hashMap 可用内存。 默认值为 `100MB` | `100` | 资源够用的话，建议调整到 `1024MB` |
| `compaction.target_io`  |  每个 compaction plan 的 IO 内存上限 (读和写)。 默认值为 `5GB`| `5120` | 离线 compaction 的默认值是 `500GB` |

## 内存优化

### MOR

1. [把 Flink 的状态后端设置为 `rocksdb`](#checkpoint) (默认的 `in memory` 状态后端非常的消耗内存）
2. 如果内存足够，`compaction.max_memory` 可以设置得更大些（默认为 `100MB`，可以调大到 `1024MB`）
3. 关注 taskManager 分配给每个 write task 的内存，保证每个 write task 能够分配到 `write.task.max.size` 所配置的内存大小。 比如 taskManager 的内存是 `4GB`，
运行了 `2` 个 `StreamWriteFunction`，那每个 write function 能分到 `2GB`，尽量预留一些缓存。因为网络缓存，taskManager 上其他类型的 task (比如 `BucketAssignFunction`)也会消耗一些内存
4. 需要关注 compaction 的内存变化。 `compaction.max_memory` 控制了每个 compaction task 读 log 时可以利用的内存大小。`compaction.tasks` 控制了 compaction task 的并发

### COW

1. [把 Flink 的状态后端设置为 `rocksdb`](#checkpoint) (默认的 `in memory` 状态后端非常的消耗内存）
2. 同时调大 `write.task.max.size` 和 `write.merge.max_memory` （默认值分别是 `1024MB` 和 `100MB`，可以调整为 `2014MB` 和 `1024MB`）
3. 关注 taskManager 分配给每个 write task 的内存，保证每个 write task 能够分配到 `write.task.max.size` 所配置的内存大小。 比如 taskManager 的内存是 `4GB`，
   运行了 `2` 个 `StreamWriteFunction`，那每个 write function 能分到 `2GB`，尽量预留一些缓存。因为网络缓存，taskManager 上其他类型的 task （比如 `BucketAssignFunction`）也会消耗一些内存

## 离线批量导入

针对存量数据导入的需求，如果存量数据来源于其他数据源，可以使用离线批量导入功能（`bulk_insert`），快速将存量数据导入 Hudi。


:::note
`bulk_insert` 省去了 avro 的序列化以及数据的 merge 过程，后续也不会再有去重操作。所以，数据的唯一性需要自己来保证。
:::

:::note
`bulk_insert` 在 `batch execution mode` 模式下执行更加高效。 `batch execution mode` 模式默认会按照 partition path 排序输入消息再写入 Hudi，
避免 file handle 频繁切换导致性能下降。
:::

:::note  
`bulk_insert` 的 write tasks 的并发是通过参数 `write.tasks` 来指定，并发的数量会影响到小文件的数量，理论上，`bulk_insert` 的 write tasks 的并发数就是划分的 bucket 数，
当然每个 bucket 在写到文件大小上限（parquet 120 MB）的时候会回滚到新的文件句柄，所以最后：写文件数量 >= [`write.bucket_assign.tasks`](#并行度)。
:::

### 参数

|  名称  | Required | 默认值 | 备注 |
|  -----------  | -------  | ------- | ------- |
| `write.operation` | `true` | `upsert` | 开启 `bulk_insert` 功能  |
| `write.tasks`  |  `false`  | `4` | `bulk_insert` write tasks 的并发，最后的文件数 >= [`write.bucket_assign.tasks`](#并行度) |
| `write.bulk_insert.shuffle_by_partition` | `false` | `true` | 是否将数据按照 partition 字段 shuffle 后，再通过 write task 写入，开启该参数将减少小文件的数量，但是有数据倾斜的风险 |
| `write.bulk_insert.sort_by_partition` | `false`  | `true` | 是否将数据线按照 partition 字段排序后，再通过 write task 写入，当一个 write task 写多个 partition时，开启可以减少小文件数量  |
| `write.sort.memory` | `false` | `128` | sort 算子的可用 managed memory（单位 MB）。默认为 `128` MB |

## 全量接增量

针对全量数据导入后，接增量的需求。如果已经有全量的离线 Hudi 表，需要接上实时写入，并且保证数据不重复，可以开启 全量接增量（`index bootstrap`）功能。

:::note
如果觉得流程冗长，可以在写入全量数据的时候资源调大直接走流模式写，全量走完接新数据再将资源调小（或者开启 [写入限流](#写入限流) ）。
:::

### 参数

| 名称 | Required | 默认值 | 备注 |
|  -----------  | -------  | ------- | ------- |
| `index.bootstrap.enabled` | `true` | `false` | 开启 index bootstrap 索引加载后，会将已存在的 Hudi 表的数据一次性加载到 state 中 |
| `index.partition.regex`  |  `false`  | `*` | 设置正则表达式进行分区筛选，默认为加载全部分区 |

### 使用流程

1. `CREATE TABLE` 创建和 Hudi 表对应的语句，注意 `table.type` 必须正确
2. 设置 `index.bootstrap.enabled` = `true` 开启索引加载功能
3. 在 `flink-conf.yaml` 中设置 Checkpoint 失败容忍 ：`execution.checkpointing.tolerable-failed-checkpoints = n`（取决于checkpoint 调度次数）
4. 等待第一次 Checkpoint 完成，表示索引加载完成
5. 索引加载完成后可以退出并保存 savepoint（也可以直接用 externalized checkpoint）
6. 重启任务，将 `index.bootstrap.enable` 设置为 `false`，参数配置到合适的大小

:::note
1. 索引加载是阻塞式，所以在索引加载过程中 Checkpoint 无法完成
2. 索引加载由数据流触发，需要确保每个 partition 都至少有1条数据，即上游 source 有数据进来
3. 索引加载为并发加载，根据数据量大小加载时间不同，可以在log中搜索 `finish loading the index under partition` 和 `Load record form file` 日志内容来观察索引加载的进
4. 第一次 Checkpoint 成功就表示索引已经加载完成，后续从 Checkpoint 恢复时无需再次加载索引
:::

## Changelog 模式

针对使用 Hudi 保留消息的所有变更（I / -U / U / D），之后接上 Flink 引擎的有状态计算实现全链路近实时数仓生产（增量计算）的需求，Hudi 的 MOR 表
通过行存原生支持保留消息的所有变更（format 层面的集成），通过流读 MOR 表可以消费到所有的变更记录。

### 参数

|  名称  | Required | 默认值 | 备注 |
|  -----------  | -------  | ------- | ------- |
| `changelog.enabled` | `false` | `false` | 默认是关闭状态，即 `UPSERT` 语义，所有的消息仅保证最后一条合并消息，中间的变更可能会被 merge 掉；改成 `true` 支持消费所有变更 |

:::note
批（快照）读仍然会合并所有的中间结果，不管 format 是否已存储中间状态。
:::

:::note
设置 `changelog.enable` 为 `true` 后，中间的变更也只是 `best effort`：异步的压缩任务会将中间变更合并成 `1` 条，所以如果流读消费不够及时，被压缩后
只能读到最后一条记录。当然，通过调整压缩的缓存时间可以预留一定的时间缓冲给 reader，比如调整压缩的两个参数：[`compaction.delta_commits`](#compaction) and [`compaction.delta_seconds`](#compaction)。
:::


## Insert 模式

当前 Hudi 对于 `Insert 模式` 默认会采用小文件策略：MOR 会追加写 avro log 文件，COW 会不断合并之前的 parquet 文件（并且增量的数据会去重），这样会导致性能下降。


如果想关闭文件合并，可以设置 `write.insert.deduplicate` 为 `false`。 关闭后，不会有任何的去重行为，每次 flush 都是直接写独立的 parquet（MOR 表也会直接写 parquet）。

### 参数
|  名称  | Required | 默认值 | 备注 |
|  -----------  | -------  | ------- | ------- |
| `write.insert.deduplicate` | `false` | `true` | 默认 `Insert 模式` 会去重，关闭后每次 flush 都会直接写独立的 parquet |

## Hive 查询

### 打包

第一步是打包 `hudi-flink-bundle_2.11-0.9.0.jar`。 `hudi-flink-bundle` module pom.xml 默认将 Hive 相关的依赖 scope 设置为 provided，
如果想打入 Hive 的依赖，需要显示指定 Profile 为 `flink-bundle-shade-hive`。执行以下命令打入 Hive 依赖：

```bash
# Maven 打包命令
mvn install -DskipTests -Drat.skip=true -Pflink-bundle-shade-hive2

# 如果是 hive3 需要使用 profile -Pflink-bundle-shade-hive3
# 如果是 hive1 需要使用 profile -Pflink-bundle-shade-hive1 
```

:::note 
Hive1.x 现在只能实现同步 metadata 到 Hive，而无法使用 Hive 查询，如需查询可使用 Spark 查询 Hive 外表的方法查询。
:::

:::note 
使用 -Pflink-bundle-shade-hive x，需要修改 Profile 中 Hive 的版本为集群对应版本（只需修改 Profile 里的 Hive 版本）。修改位置为 `packaging/hudi-flink-bundle/pom.xml`
最下面的对应 Profile 段，找到后修改 Profile 中的 Hive 版本为对应版本即可。
:::

### Hive 环境准备

1. 第一步是将 `hudi-hadoop-mr-bundle.jar` 放到 Hive中。 在 Hive 的根目录下创建 `auxlib/` 文件夹，把 `hudi-hadoop-mr-bundle-0.x.x-SNAPSHOT.jar` 移入到 `auxlib`。
`hudi-hadoop-mr-bundle-0.x.x-SNAPSHOT.jar` 可以在 `packaging/hudi-hadoop-mr-bundle/target` 目录下拷贝。

2. 第二步是开启 Hive 相关的服务。Flink SQL Client 远程连接 Hive 的时候，要求 Hive 的 `hive metastore` and `hiveserver2` 两个服务都开启且需要记住端口号。
服务开启的方法如下：

```bash
# 启动 Hive metastore 和 hiveserver2
nohup ./bin/hive --service metastore &
nohup ./bin/hive --service hiveserver2 &

# 每次更新了 /auxlib 下的 jar 包都需要重启上述两个服务
```

### Hive 配置模版
 
Flink hive sync 现在支持两种 hive sync mode，分别是 `hms` 和 `jdbc`。其中 `hms` 模式只需要配置 Metastore uris；而 `jdbc` 模式需要同时
配置 JDBC 属性 和 Metastore uris，具体配置模版如下：

```sql
-- hms mode 配置模版
CREATE TABLE t1(
  uuid VARCHAR(20),
  name VARCHAR(10),
  age INT,
  ts TIMESTAMP(3),
  `partition` VARCHAR(20)
)
PARTITIONED BY (`partition`)
WITH (
  'connector' = 'hudi',
  'path' = 'oss://vvr-daily/hudi/t1',
  'table.type' = 'COPY_ON_WRITE',  --MERGE_ON_READ 方式在没生成 parquet 文件前，Hive 不会有输出
  'hive_sync.enable' = 'true',     -- Required。开启 Hive 同步功能
  'hive_sync.mode' = 'hms'         -- Required。将 hive sync mode 设置为 hms, 默认 jdbc
  'hive_sync.metastore.uris' = 'thrift://ip:9083' -- Required。metastore 的端口
);


-- jdbc mode 配置模版
CREATE TABLE t1(
  uuid VARCHAR(20),
  name VARCHAR(10),
  age INT,
  ts TIMESTAMP(3),
  `partition` VARCHAR(20)
)
PARTITIONED BY (`partition`)
WITH (
  'connector' = 'hudi',
  'path' = 'oss://vvr-daily/hudi/t1',
  'table.type' = 'COPY_ON_WRITE',  --MERGE_ON_READ 方式在没生成 parquet 文件前，Hive 不会有输出
  'hive_sync.enable' = 'true',     -- Required。开启 Hive 同步功能
  'hive_sync.mode' = 'hms'         -- Required。将 hive sync mode 设置为 hms, 默认 jdbc
  'hive_sync.metastore.uris' = 'thrift://ip:9083'  -- Required。metastore 的端口
  'hive_sync.jdbc_url'='jdbc:hive2://ip:10000',    -- required。hiveServer 的端口
  'hive_sync.table'='t1',                          -- required。hive 新建的表名
  'hive_sync.db'='testDB',                         -- required。hive 新建的数据库名
  'hive_sync.username'='root',                     -- required。HMS 用户名
  'hive_sync.password'='your password'             -- required。HMS 密码
);
```

### Hive 查询

使用 beeline 查询时，需要手动设置：
```bash
set hive.input.format = org.apache.hudi.hadoop.hive.HoodieCombineHiveInputFormat;
```

### 与其他包冲突

当 Flink lib/ 下有 `flink-sql-connector-hive-xxx.jar` 时，会出现hive包冲突，解决方法是在 Install 时，另外再指定一个 Profile：`-Pinclude-flink-sql-connector-hive`，
同时删除 Flink lib/ 下的 `flink-sql-connector-hive-xxx.jar`，新的 Install 的命令如下：

```bash
# Maven 打包命令
mvn install -DskipTests -Drat.skip=true -Pflink-bundle-shade-hive2 -Pinclude-flink-sql-connector-hive
```

## Presto 查询

### Hive 同步
首先，参考 [Hive 查询](#hive-查询) 过程，完成 Hive 元数据的同步。

### Presto 环境配置
1. 根据 [Presto 配置文档](https://prestodb.io/docs/current/installation/deployment.html) 来配置 Presto。
2. 根据下面的配置，在 ` /presto-server-0.2xxx/etc/catalog/hive.properties` 中配置 Hive catalog：

```properties
connector.name=hive-hadoop2
hive.metastore.uri=thrift://xxx.xxx.xxx.xxx:9083
hive.config.resources=.../hadoop-2.x/etc/hadoop/core-site.xml,.../hadoop-2.x/etc/hadoop/hdfs-site.xml
```

### Presto 查询

通过 presto-cli 连接 Hive metastore 开启查询。 presto-cli 的设置参考 presto-cli 配置：

```bash
# 连接 presto server 的命令
./presto --server xxx.xxx.xxx.xxx:9999 --catalog hive --schema default
```

:::note
1. `Presto-server-0.2445` 版本较低，在查 MOR 表的 `rt` 表时，会出现包冲突，正在解决中
2. 当 `Presto-server-xxx` 的版本 < 0.233 时，`hudi-presto-bundle.jar` 需要手动导入到 `{presto_install_dir}/plugin/hive-hadoop2/` 中。
:::


## 离线 Compaction

MERGE_ON_READ 表的 compaction 默认是打开的，策略是 `5` 个 delta commits 执行一次压缩。因为压缩操作比较耗费内存，和写流程放在同一个 pipeline，在数据量比较大
的时候（10w+/s），容易干扰写流程，此时采用离线定时任务的方式执行 compaction 任务更稳定。

:::note 
一个 compaction 任务的执行包括两部分：1. 生成 compaction plan； 2. 执行对应的 compaction plan。其中，第一步生成 compaction plan 的过程推荐由写任务定时触发，
 写参数 `compaction.schedule.enable` 默认开启。
:::

离线 compaction 需要手动执行 Flink 任务，程序入口为： `hudi-flink-bundle_2.11-0.9.0-SNAPSHOT.jar` ： `org.apache.hudi.sink.compact.HoodieFlinkCompactor`

```bash
# 命令行执行方式
./bin/flink run -c org.apache.hudi.sink.compact.HoodieFlinkCompactor lib/hudi-flink-bundle_2.11-0.9.0.jar --path hdfs://xxx:9000/table
```

### 参数

|  名称 | Required | 默认值 | 备注 |
|  -----------  | -------  | ------- | ------- |
| `--path` | `frue` | `--` | 目标 Hudi 表的路径 |
| `--compaction-max-memory` | `false` | `100` | 压缩时 log 数据的索引 HashMap 的内存大小，默认 `100MB`，内存足够时，可以调大 |
| `--schedule` | `false` | `false` | 是否要执行 schedule compaction 的操作，当写流程还在持续写入表数据的时候，开启这个参数有丢失查询数据的风险，所以开启该参数一定要保证当前没有任务往表里写数据，写任务的 compaction plan 默认是一直 schedule 的，除非手动关闭（默认 `5` 个 delta commits 一次 compaction）|
| `--seq` | `false` | `LIFO` | 执行压缩任务的顺序，默认是从最新的 compaction plan 开始执行，可选值：`LIFO` ：从最新的 plan 开始执行； `FIFO`：从最老的 plan 开始执行 |

## 写入限流

针对将全量数据（百亿数量级）和增量先同步到 Kafka，再通过 Flink 流式消费的方式将库表数据直接导成 Hudi 表的需求，因为直接消费全量数据：量大
（吞吐高）、乱序严重（写入的 partition 随机），会导致写入性能退化，出现吞吐毛刺等情况，这时候可以开启限速参数，保证流量平稳写入。

### 参数

|  名称  | Required | 默认值 | 备注 |
|  -----------  | -------  | ------- | ------- |
| `write.rate.limit` | `false` | `0` | 默认关闭限速 |

## 从这开始下一步？

您也可以通过[自己构建hudi](https://github.com/apache/hudi#building-apache-hudi-from-source)来快速开始，
并在spark-shell命令中使用`--jars <path to hudi_code>/packaging/hudi-spark-bundle/target/hudi-spark-bundle_2.1?-*.*.*-SNAPSHOT.jar`，
而不是`--packages org.apache.hudi:hudi-spark3-bundle_2.12:0.8.0`


这里我们使用Spark演示了Hudi的功能。但是，Hudi可以支持多种存储类型/视图，并且可以从Hive，Spark，Presto等查询引擎中查询Hudi数据集。
我们制作了一个基于Docker设置、所有依赖系统都在本地运行的[演示视频](https://www.youtube.com/watch?v=VhNgUsxdrD0)，
我们建议您复制相同的设置然后按照[这里](/cn/docs/docker_demo)的步骤自己运行这个演示。
另外，如果您正在寻找将现有数据迁移到Hudi的方法，请参考[迁移指南](/cn/docs/migration_guide)。
