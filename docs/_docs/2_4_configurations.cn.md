---
title: 配置
keywords: garbage collection, hudi, jvm, configs, tuning
permalink: /cn/docs/configurations.html
summary: "Here we list all possible configurations and what they mean"
toc: true
last_modified_at: 2019-12-30T15:59:57-04:00
language: cn
---

该页面介绍了几种配置写入或读取Hudi数据集的作业的方法。
简而言之，您可以在几个级别上控制行为。

- **[Spark数据源配置](#spark-datasource)** : 这些配置控制Hudi Spark数据源，提供如下功能：
   定义键和分区、选择写操作、指定如何合并记录或选择要读取的视图类型。
- **[WriteClient 配置](#writeclient-configs)** : 在内部，Hudi数据源使用基于RDD的`HoodieWriteClient` API
   真正执行对存储的写入。 这些配置可对文件大小、压缩（compression）、并行度、压缩（compaction）、写入模式、清理等底层方面进行完全控制。
   尽管Hudi提供了合理的默认设置，但在不同情形下，可能需要对这些配置进行调整以针对特定的工作负载进行优化。
- **[RecordPayload 配置](#PAYLOAD_CLASS_OPT_KEY)** : 这是Hudi提供的最底层的定制。
   RecordPayload定义了如何根据传入的新记录和存储的旧记录来产生新值以进行插入更新。
   Hudi提供了诸如`OverwriteWithLatestAvroPayload`的默认实现，该实现仅使用最新或最后写入的记录来更新存储。
   在数据源和WriteClient级别，都可以将其重写为扩展`HoodieRecordPayload`类的自定义类。


## Spark数据源配置 {#spark-datasource}

可以通过将以下选项传递到`option(k,v)`方法中来配置使用数据源的Spark作业。
实际的数据源级别配置在下面列出。

### 写选项

另外，您可以使用`options()`或`option(k,v)`方法直接传递任何WriteClient级别的配置。

```java
inputDF.write()
.format("org.apache.hudi")
.options(clientOpts) // 任何Hudi客户端选项都可以传入
.option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "_row_key")
.option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), "partition")
.option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), "timestamp")
.option(HoodieWriteConfig.TABLE_NAME, tableName)
.mode(SaveMode.Append)
.save(basePath);
```

用于通过`write.format.option(...)`写入数据集的选项

#### TABLE_NAME_OPT_KEY {#TABLE_NAME_OPT_KEY}
  属性：`hoodie.datasource.write.table.name` [必须]<br/>
  <span style="color:grey">Hive表名，用于将数据集注册到其中。</span>
  
#### OPERATION_OPT_KEY {#OPERATION_OPT_KEY}
  属性：`hoodie.datasource.write.operation`, 默认值：`upsert`<br/>
  <span style="color:grey">是否为写操作进行插入更新、插入或批量插入。使用`bulkinsert`将新数据加载到表中，之后使用`upsert`或`insert`。
  批量插入使用基于磁盘的写入路径来扩展以加载大量输入，而无需对其进行缓存。</span>
  
#### STORAGE_TYPE_OPT_KEY {#STORAGE_TYPE_OPT_KEY}
  属性：`hoodie.datasource.write.storage.type`, 默认值：`COPY_ON_WRITE` <br/>
  <span style="color:grey">此写入的基础数据的存储类型。两次写入之间不能改变。</span>
  
#### PRECOMBINE_FIELD_OPT_KEY {#PRECOMBINE_FIELD_OPT_KEY}
  属性：`hoodie.datasource.write.precombine.field`, 默认值：`ts` <br/>
  <span style="color:grey">实际写入之前在preCombining中使用的字段。
  当两个记录具有相同的键值时，我们将使用Object.compareTo(..)从precombine字段中选择一个值最大的记录。</span>

#### PAYLOAD_CLASS_OPT_KEY {#PAYLOAD_CLASS_OPT_KEY}
  属性：`hoodie.datasource.write.payload.class`, 默认值：`org.apache.hudi.OverwriteWithLatestAvroPayload` <br/>
  <span style="color:grey">使用的有效载荷类。如果您想在插入更新或插入时使用自己的合并逻辑，请重写此方法。
  这将使得`PRECOMBINE_FIELD_OPT_VAL`设置的任何值无效</span>
  
#### RECORDKEY_FIELD_OPT_KEY {#RECORDKEY_FIELD_OPT_KEY}
  属性：`hoodie.datasource.write.recordkey.field`, 默认值：`uuid` <br/>
  <span style="color:grey">记录键字段。用作`HoodieKey`中`recordKey`部分的值。
  实际值将通过在字段值上调用.toString()来获得。可以使用点符号指定嵌套字段，例如：`a.b.c`</span>

#### PARTITIONPATH_FIELD_OPT_KEY {#PARTITIONPATH_FIELD_OPT_KEY}
  属性：`hoodie.datasource.write.partitionpath.field`, 默认值：`partitionpath` <br/>
  <span style="color:grey">分区路径字段。用作`HoodieKey`中`partitionPath`部分的值。
  通过调用.toString()获得实际的值</span>

#### HIVE_STYLE_PARTITIONING_OPT_KEY {#HIVE_STYLE_PARTITIONING_OPT_KEY}
  属性：`hoodie.datasource.write.hive_style_partitioning`, 默认值：`false` <br/>
  <span style="color:grey">如果设置为true，则生成基于Hive格式的partition目录：<partition_column_name>=<partition_value></span>

#### KEYGENERATOR_CLASS_OPT_KEY {#KEYGENERATOR_CLASS_OPT_KEY}
  属性：`hoodie.datasource.write.keygenerator.class`, 默认值：`org.apache.hudi.SimpleKeyGenerator` <br/>
  <span style="color:grey">键生成器类，实现从输入的`Row`对象中提取键</span>
  
#### COMMIT_METADATA_KEYPREFIX_OPT_KEY {#COMMIT_METADATA_KEYPREFIX_OPT_KEY}
  属性：`hoodie.datasource.write.commitmeta.key.prefix`, 默认值：`_` <br/>
  <span style="color:grey">以该前缀开头的选项键会自动添加到提交/增量提交的元数据中。
  这对于与hudi时间轴一致的方式存储检查点信息很有用</span>

#### INSERT_DROP_DUPS_OPT_KEY {#INSERT_DROP_DUPS_OPT_KEY}
  属性：`hoodie.datasource.write.insert.drop.duplicates`, 默认值：`false` <br/>
  <span style="color:grey">如果设置为true，则在插入操作期间从传入DataFrame中过滤掉所有重复记录。</span>
  
#### HIVE_SYNC_ENABLED_OPT_KEY {#HIVE_SYNC_ENABLED_OPT_KEY}
  属性：`hoodie.datasource.hive_sync.enable`, 默认值：`false` <br/>
  <span style="color:grey">设置为true时，将数据集注册并同步到Apache Hive Metastore</span>
  
#### HIVE_DATABASE_OPT_KEY {#HIVE_DATABASE_OPT_KEY}
  属性：`hoodie.datasource.hive_sync.database`, 默认值：`default` <br/>
  <span style="color:grey">要同步到的数据库</span>
  
#### HIVE_TABLE_OPT_KEY {#HIVE_TABLE_OPT_KEY}
  属性：`hoodie.datasource.hive_sync.table`, [Required] <br/>
  <span style="color:grey">要同步到的表</span>
  
#### HIVE_USER_OPT_KEY {#HIVE_USER_OPT_KEY}
  属性：`hoodie.datasource.hive_sync.username`, 默认值：`hive` <br/>
  <span style="color:grey">要使用的Hive用户名</span>
  
#### HIVE_PASS_OPT_KEY {#HIVE_PASS_OPT_KEY}
  属性：`hoodie.datasource.hive_sync.password`, 默认值：`hive` <br/>
  <span style="color:grey">要使用的Hive密码</span>
  
#### HIVE_URL_OPT_KEY {#HIVE_URL_OPT_KEY}
  属性：`hoodie.datasource.hive_sync.jdbcurl`, 默认值：`jdbc:hive2://localhost:10000` <br/>
  <span style="color:grey">Hive metastore url</span>
  
#### HIVE_PARTITION_FIELDS_OPT_KEY {#HIVE_PARTITION_FIELDS_OPT_KEY}
  属性：`hoodie.datasource.hive_sync.partition_fields`, 默认值：` ` <br/>
  <span style="color:grey">数据集中用于确定Hive分区的字段。</span>
  
#### HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY {#HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY}
  属性：`hoodie.datasource.hive_sync.partition_extractor_class`, 默认值：`org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor` <br/>
  <span style="color:grey">用于将分区字段值提取到Hive分区列中的类。</span>
  
#### HIVE_ASSUME_DATE_PARTITION_OPT_KEY {#HIVE_ASSUME_DATE_PARTITION_OPT_KEY}
  属性：`hoodie.datasource.hive_sync.assume_date_partitioning`, 默认值：`false` <br/>
  <span style="color:grey">假设分区格式是yyyy/mm/dd</span>

### 读选项

用于通过`read.format.option(...)`读取数据集的选项

#### VIEW_TYPE_OPT_KEY {#VIEW_TYPE_OPT_KEY}
属性：`hoodie.datasource.view.type`, 默认值：`read_optimized` <br/>
<span style="color:grey">是否需要以某种模式读取数据，增量模式（自InstantTime以来的新数据）
（或）读优化模式（基于列数据获取最新视图）
（或）实时模式（基于行和列数据获取最新视图）</span>

#### BEGIN_INSTANTTIME_OPT_KEY {#BEGIN_INSTANTTIME_OPT_KEY} 
属性：`hoodie.datasource.read.begin.instanttime`, [在增量模式下必须] <br/>
<span style="color:grey">开始增量提取数据的即时时间。这里的instanttime不必一定与时间轴上的即时相对应。
取出以`instant_time > BEGIN_INSTANTTIME`写入的新数据。
例如：'20170901080000'将获取2017年9月1日08:00 AM之后写入的所有新数据。</span>
 
#### END_INSTANTTIME_OPT_KEY {#END_INSTANTTIME_OPT_KEY}
属性：`hoodie.datasource.read.end.instanttime`, 默认值：最新即时（即从开始即时获取所有新数据） <br/>
<span style="color:grey">限制增量提取的数据的即时时间。取出以`instant_time <= END_INSTANTTIME`写入的新数据。</span>


## WriteClient 配置 {#writeclient-configs}

直接使用RDD级别api进行编程的Jobs可以构建一个`HoodieWriteConfig`对象，并将其传递给`HoodieWriteClient`构造函数。
HoodieWriteConfig可以使用以下构建器模式构建。

```java
HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .forTable(tableName)
        .withSchema(schemaStr)
        .withProps(props) // 从属性文件传递原始k、v对。
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withXXX(...).build())
        .withIndexConfig(HoodieIndexConfig.newBuilder().withXXX(...).build())
        ...
        .build();
```

以下各节介绍了写配置的不同方面，并解释了最重要的配置及其属性名称和默认值。

#### withPath(hoodie_base_path) {#withPath}
属性：`hoodie.base.path` [必须] <br/>
<span style="color:grey">创建所有数据分区所依据的基本DFS路径。
始终在前缀中明确指明存储方式（例如hdfs://，s3://等）。
Hudi将有关提交、保存点、清理审核日志等的所有主要元数据存储在基本目录下的.hoodie目录中。</span>

#### withSchema(schema_str) {#withSchema} 
属性：`hoodie.avro.schema` [必须]<br/>
<span style="color:grey">这是数据集的当前读取器的avro模式（schema）。
这是整个模式的字符串。HoodieWriteClient使用此模式传递到HoodieRecordPayload的实现，以从源格式转换为avro记录。
在更新过程中重写记录时也使用此模式。</span>

#### forTable(table_name) {#forTable} 
属性：`hoodie.table.name` [必须] <br/>
 <span style="color:grey">数据集的表名，将用于在Hive中注册。每次运行需要相同。</span>

#### withBulkInsertParallelism(bulk_insert_parallelism = 1500) {#withBulkInsertParallelism} 
属性：`hoodie.bulkinsert.shuffle.parallelism`<br/>
<span style="color:grey">批量插入旨在用于较大的初始导入，而此处的并行度决定了数据集中文件的初始数量。
调整此值以达到在初始导入期间所需的最佳尺寸。</span>

#### withParallelism(insert_shuffle_parallelism = 1500, upsert_shuffle_parallelism = 1500) {#withParallelism} 
属性：`hoodie.insert.shuffle.parallelism`, `hoodie.upsert.shuffle.parallelism`<br/>
<span style="color:grey">最初导入数据后，此并行度将控制用于读取输入记录的初始并行度。
确保此值足够高，例如：1个分区用于1 GB的输入数据</span>

#### combineInput(on_insert = false, on_update=true) {#combineInput} 
属性：`hoodie.combine.before.insert`, `hoodie.combine.before.upsert`<br/>
<span style="color:grey">在DFS中插入或更新之前先组合输入RDD并将多个部分记录合并为单个记录的标志</span>

#### withWriteStatusStorageLevel(level = MEMORY_AND_DISK_SER) {#withWriteStatusStorageLevel} 
属性：`hoodie.write.status.storage.level`<br/>
<span style="color:grey">HoodieWriteClient.insert和HoodieWriteClient.upsert返回一个持久的RDD[WriteStatus]，
这是因为客户端可以选择检查WriteStatus并根据失败选择是否提交。这是此RDD的存储级别的配置</span>

#### withAutoCommit(autoCommit = true) {#withAutoCommit} 
属性：`hoodie.auto.commit`<br/>
<span style="color:grey">插入和插入更新后，HoodieWriteClient是否应该自动提交。
客户端可以选择关闭自动提交，并在"定义的成功条件"下提交</span>

#### withAssumeDatePartitioning(assumeDatePartitioning = false) {#withAssumeDatePartitioning} 
属性：`hoodie.assume.date.partitioning`<br/>
<span style="color:grey">HoodieWriteClient是否应该假设数据按日期划分，即从基本路径划分为三个级别。
这是支持<0.3.1版本创建的表的一个补丁。最终将被删除</span>

#### withConsistencyCheckEnabled(enabled = false) {#withConsistencyCheckEnabled} 
属性：`hoodie.consistency.check.enabled`<br/>
<span style="color:grey">HoodieWriteClient是否应该执行其他检查，以确保写入的文件在基础文件系统/存储上可列出。
将其设置为true可以解决S3的最终一致性模型，并确保作为提交的一部分写入的所有数据均能准确地用于查询。</span>

### 索引配置
以下配置控制索引行为，该行为将传入记录标记为对较旧记录的插入或更新。

[withIndexConfig](#withIndexConfig) (HoodieIndexConfig) <br/>
<span style="color:grey">可插入以具有外部索引（HBase）或使用存储在Parquet文件中的默认布隆过滤器（bloom filter）</span>

#### withIndexClass(indexClass = "x.y.z.UserDefinedIndex") {#withIndexClass}
属性：`hoodie.index.class` <br/>
<span style="color:grey">用户自定义索引的全路径名，索引类必须为HoodieIndex的子类，当指定该配置时，其会优先于`hoodie.index.type`配置</span>
        
#### withIndexType(indexType = BLOOM) {#withIndexType}
属性：`hoodie.index.type` <br/>
<span style="color:grey">要使用的索引类型。默认为布隆过滤器。可能的选项是[BLOOM | HBASE | INMEMORY]。
布隆过滤器消除了对外部系统的依赖，并存储在Parquet数据文件的页脚中</span>

#### bloomFilterNumEntries(numEntries = 60000) {#bloomFilterNumEntries}
属性：`hoodie.index.bloom.num_entries` <br/>
<span style="color:grey">仅在索引类型为BLOOM时适用。<br/>这是要存储在布隆过滤器中的条目数。
我们假设maxParquetFileSize为128MB，averageRecordSize为1024B，因此，一个文件中的记录总数约为130K。
默认值（60000）大约是此近似值的一半。[HUDI-56](https://issues.apache.org/jira/browse/HUDI-56)
描述了如何动态地对此进行计算。
警告：将此值设置得太低，将产生很多误报，并且索引查找将必须扫描比其所需的更多的文件；如果将其设置得非常高，将线性增加每个数据文件的大小（每50000个条目大约4KB）。</span>

#### bloomFilterFPP(fpp = 0.000000001) {#bloomFilterFPP}
属性：`hoodie.index.bloom.fpp` <br/>
<span style="color:grey">仅在索引类型为BLOOM时适用。<br/>根据条目数允许的错误率。
这用于计算应为布隆过滤器分配多少位以及哈希函数的数量。通常将此值设置得很低（默认值：0.000000001），我们希望在磁盘空间上进行权衡以降低误报率</span>

#### bloomIndexPruneByRanges(pruneRanges = true) {#bloomIndexPruneByRanges}
属性：`hoodie.bloom.index.prune.by.ranges` <br/>
<span style="color:grey">仅在索引类型为BLOOM时适用。<br/>为true时，从文件框定信息，可以加快索引查找的速度。 如果键具有单调递增的前缀，例如时间戳，则特别有用。</span>

#### bloomIndexUseCaching(useCaching = true) {#bloomIndexUseCaching}
属性：`hoodie.bloom.index.use.caching` <br/>
<span style="color:grey">仅在索引类型为BLOOM时适用。<br/>为true时，将通过减少用于计算并行度或受影响分区的IO来缓存输入的RDD以加快索引查找</span>

#### bloomIndexTreebasedFilter(useTreeFilter = true) {#bloomIndexTreebasedFilter}
属性：`hoodie.bloom.index.use.treebased.filter` <br/>
<span style="color:grey">仅在索引类型为BLOOM时适用。<br/>为true时，启用基于间隔树的文件过滤优化。与暴力模式相比，此模式可根据键范围加快文件过滤速度</span>

#### bloomIndexBucketizedChecking(bucketizedChecking = true) {#bloomIndexBucketizedChecking}
属性：`hoodie.bloom.index.bucketized.checking` <br/>
<span style="color:grey">仅在索引类型为BLOOM时适用。<br/>为true时，启用了桶式布隆过滤。这减少了在基于排序的布隆索引查找中看到的偏差</span>

#### bloomIndexKeysPerBucket(keysPerBucket = 10000000) {#bloomIndexKeysPerBucket}
属性：`hoodie.bloom.index.keys.per.bucket` <br/>
<span style="color:grey">仅在启用bloomIndexBucketizedChecking并且索引类型为bloom的情况下适用。<br/>
此配置控制“存储桶”的大小，该大小可跟踪对单个文件进行的记录键检查的次数，并且是分配给执行布隆过滤器查找的每个分区的工作单位。
较高的值将分摊将布隆过滤器读取到内存的固定成本。</span>

#### bloomIndexParallelism(0) {#bloomIndexParallelism}
属性：`hoodie.bloom.index.parallelism` <br/>
<span style="color:grey">仅在索引类型为BLOOM时适用。<br/>这是索引查找的并行度，其中涉及Spark Shuffle。 默认情况下，这是根据输入的工作负载特征自动计算的</span>

#### hbaseZkQuorum(zkString) [必须] {#hbaseZkQuorum}  
属性：`hoodie.index.hbase.zkquorum` <br/>
<span style="color:grey">仅在索引类型为HBASE时适用。要连接的HBase ZK Quorum URL。</span>

#### hbaseZkPort(port) [必须] {#hbaseZkPort}  
属性：`hoodie.index.hbase.zkport` <br/>
<span style="color:grey">仅在索引类型为HBASE时适用。要连接的HBase ZK Quorum端口。</span>

#### hbaseZkZnodeParent(zkZnodeParent)  [必须] {#hbaseTableName}
属性：`hoodie.index.hbase.zknode.path` <br/>
<span style="color:grey">仅在索引类型为HBASE时适用。这是根znode，它将包含HBase创建及使用的所有znode。</span>

#### hbaseTableName(tableName)  [必须] {#hbaseTableName}
属性：`hoodie.index.hbase.table` <br/>
<span style="color:grey">仅在索引类型为HBASE时适用。HBase表名称，用作索引。Hudi将row_key和[partition_path, fileID, commitTime]映射存储在表中。</span>

##### bloomIndexUpdatePartitionPath(updatePartitionPath = false) {#bloomIndexUpdatePartitionPath}
属性：`hoodie.bloom.index.update.partition.path` <br/>
<span style="color:grey">仅在索引类型为GLOBAL_BLOOM时适用。<br/>为true时，当对一个已有记录执行包含分区路径的更新操作时，将会导致把新记录插入到新分区，而把原有记录从旧分区里删除。为false时，只对旧分区的原有记录进行更新。</span>


### 存储选项
控制有关调整parquet和日志文件大小的方面。

[withStorageConfig](#withStorageConfig) (HoodieStorageConfig) <br/>

#### limitFileSize (size = 120MB) {#limitFileSize}
属性：`hoodie.parquet.max.file.size` <br/>
<span style="color:grey">Hudi写阶段生成的parquet文件的目标大小。对于DFS，这需要与基础文件系统块大小保持一致，以实现最佳性能。</span>

#### parquetBlockSize(rowgroupsize = 120MB) {#parquetBlockSize} 
属性：`hoodie.parquet.block.size` <br/>
<span style="color:grey">Parquet行组大小。最好与文件大小相同，以便将文件中的单个列连续存储在磁盘上</span>

#### parquetPageSize(pagesize = 1MB) {#parquetPageSize} 
属性：`hoodie.parquet.page.size` <br/>
<span style="color:grey">Parquet页面大小。页面是parquet文件中的读取单位。 在一个块内，页面被分别压缩。</span>

#### parquetCompressionRatio(parquetCompressionRatio = 0.1) {#parquetCompressionRatio} 
属性：`hoodie.parquet.compression.ratio` <br/>
<span style="color:grey">当Hudi尝试调整新parquet文件的大小时，预期对parquet数据进行压缩的比例。
如果bulk_insert生成的文件小于预期大小，请增加此值</span>

#### parquetCompressionCodec(parquetCompressionCodec = gzip) {#parquetCompressionCodec}
属性：`hoodie.parquet.compression.codec` <br/>
<span style="color:grey">Parquet压缩编解码方式名称。默认值为gzip。可能的选项是[gzip | snappy | uncompressed | lzo]</span>

#### logFileMaxSize(logFileSize = 1GB) {#logFileMaxSize} 
属性：`hoodie.logfile.max.size` <br/>
<span style="color:grey">LogFile的最大大小。这是在将日志文件移到下一个版本之前允许的最大大小。</span>

#### logFileDataBlockMaxSize(dataBlockSize = 256MB) {#logFileDataBlockMaxSize} 
属性：`hoodie.logfile.data.block.max.size` <br/>
<span style="color:grey">LogFile数据块的最大大小。这是允许将单个数据块附加到日志文件的最大大小。
这有助于确保附加到日志文件的数据被分解为可调整大小的块，以防止发生OOM错误。此大小应大于JVM内存。</span>

#### logFileToParquetCompressionRatio(logFileToParquetCompressionRatio = 0.35) {#logFileToParquetCompressionRatio} 
属性：`hoodie.logfile.to.parquet.compression.ratio` <br/>
<span style="color:grey">随着记录从日志文件移动到parquet，预期会进行额外压缩的比例。
用于merge_on_read存储，以将插入内容发送到日志文件中并控制压缩parquet文件的大小。</span>
 
#### parquetCompressionCodec(parquetCompressionCodec = gzip) {#parquetCompressionCodec} 
属性：`hoodie.parquet.compression.codec` <br/>
<span style="color:grey">Parquet文件的压缩编解码方式</span>

### 压缩配置
压缩配置用于控制压缩（将日志文件合并到新的parquet基本文件中）、清理（回收较旧及未使用的文件组）。
[withCompactionConfig](#withCompactionConfig) (HoodieCompactionConfig) <br/>

#### withCleanerPolicy(policy = KEEP_LATEST_COMMITS) {#withCleanerPolicy} 
属性：`hoodie.cleaner.policy` <br/>
<span style="color:grey">要使用的清理政策。Hudi将删除旧版本的parquet文件以回收空间。
任何引用此版本文件的查询和计算都将失败。最好确保数据保留的时间超过最大查询执行时间。</span>

#### retainCommits(no_of_commits_to_retain = 24) {#retainCommits} 
属性：`hoodie.cleaner.commits.retained` <br/>
<span style="color:grey">保留的提交数。因此，数据将保留为num_of_commits * time_between_commits（计划的）。
这也直接转化为您可以逐步提取此数据集的数量</span>

#### archiveCommitsWith(minCommits = 96, maxCommits = 128) {#archiveCommitsWith} 
属性：`hoodie.keep.min.commits`, `hoodie.keep.max.commits` <br/>
<span style="color:grey">每个提交都是`.hoodie`目录中的一个小文件。由于DFS通常不支持大量小文件，因此Hudi将较早的提交归档到顺序日志中。
提交通过重命名提交文件以原子方式发布。</span>

#### withCommitsArchivalBatchSize(batch = 10) {#withCommitsArchivalBatchSize}
属性：`hoodie.commits.archival.batch` <br/>
<span style="color:grey">这控制着批量读取并一起归档的提交即时的数量。</span>

#### compactionSmallFileSize(size = 0) {#compactionSmallFileSize} 
属性：`hoodie.parquet.small.file.limit` <br/>
<span style="color:grey">该值应小于maxFileSize，如果将其设置为0，会关闭此功能。
由于批处理中分区中插入记录的数量众多，总会出现小文件。
Hudi提供了一个选项，可以通过将对该分区中的插入作为对现有小文件的更新来解决小文件的问题。
此处的大小是被视为“小文件大小”的最小文件大小。</span>

#### insertSplitSize(size = 500000) {#insertSplitSize} 
属性：`hoodie.copyonwrite.insert.split.size` <br/>
<span style="color:grey">插入写入并行度。为单个分区的总共插入次数。
写出100MB的文件，至少1kb大小的记录，意味着每个文件有100K记录。默认值是超额配置为500K。
为了改善插入延迟，请对其进行调整以匹配单个文件中的记录数。
将此值设置为较小的值将导致文件变小（尤其是当compactionSmallFileSize为0时）</span>

#### autoTuneInsertSplits(true) {#autoTuneInsertSplits} 
属性：`hoodie.copyonwrite.insert.auto.split` <br/>
<span style="color:grey">Hudi是否应该基于最后24个提交的元数据动态计算insertSplitSize。默认关闭。</span>

#### approxRecordSize(size = 1024) {#approxRecordSize} 
属性：`hoodie.copyonwrite.record.size.estimate` <br/>
<span style="color:grey">平均记录大小。如果指定，hudi将使用它，并且不会基于最后24个提交的元数据动态地计算。
没有默认值设置。这对于计算插入并行度以及将插入打包到小文件中至关重要。如上所述。</span>

#### withInlineCompaction(inlineCompaction = false) {#withInlineCompaction} 
属性：`hoodie.compact.inline` <br/>
<span style="color:grey">当设置为true时，紧接在插入或插入更新或批量插入的提交或增量提交操作之后由摄取本身触发压缩</span>

#### withMaxNumDeltaCommitsBeforeCompaction(maxNumDeltaCommitsBeforeCompaction = 10) {#withMaxNumDeltaCommitsBeforeCompaction} 
属性：`hoodie.compact.inline.max.delta.commits` <br/>
<span style="color:grey">触发内联压缩之前要保留的最大增量提交数</span>

#### withCompactionLazyBlockReadEnabled(true) {#withCompactionLazyBlockReadEnabled} 
属性：`hoodie.compaction.lazy.block.read` <br/>
<span style="color:grey">当CompactedLogScanner合并所有日志文件时，此配置有助于选择是否应延迟读取日志块。
选择true以使用I/O密集型延迟块读取（低内存使用），或者为false来使用内存密集型立即块读取（高内存使用）</span>

#### withCompactionReverseLogReadEnabled(false) {#withCompactionReverseLogReadEnabled} 
属性：`hoodie.compaction.reverse.log.read` <br/>
<span style="color:grey">HoodieLogFormatReader会从pos=0到pos=file_length向前读取日志文件。
如果此配置设置为true，则Reader会从pos=file_length到pos=0反向读取日志文件</span>

#### withCleanerParallelism(cleanerParallelism = 200) {#withCleanerParallelism} 
属性：`hoodie.cleaner.parallelism` <br/>
<span style="color:grey">如果清理变慢，请增加此值。</span>

#### withCompactionStrategy(compactionStrategy = org.apache.hudi.io.compact.strategy.LogFileSizeBasedCompactionStrategy) {#withCompactionStrategy} 
属性：`hoodie.compaction.strategy` <br/>
<span style="color:grey">用来决定在每次压缩运行期间选择要压缩的文件组的压缩策略。
默认情况下，Hudi选择具有累积最多未合并数据的日志文件</span>

#### withTargetIOPerCompactionInMB(targetIOPerCompactionInMB = 500000) {#withTargetIOPerCompactionInMB} 
属性：`hoodie.compaction.target.io` <br/>
<span style="color:grey">LogFileSizeBasedCompactionStrategy的压缩运行期间要花费的MB量。当压缩以内联模式运行时，此值有助于限制摄取延迟。</span>

#### withTargetPartitionsPerDayBasedCompaction(targetPartitionsPerCompaction = 10) {#withTargetPartitionsPerDayBasedCompaction} 
属性：`hoodie.compaction.daybased.target` <br/>
<span style="color:grey">由org.apache.hudi.io.compact.strategy.DayBasedCompactionStrategy使用，表示在压缩运行期间要压缩的最新分区数。</span>    

#### withPayloadClass(payloadClassName = org.apache.hudi.common.model.HoodieAvroPayload) {#payloadClassName} 
属性：`hoodie.compaction.payload.class` <br/>
<span style="color:grey">这需要与插入/插入更新过程中使用的类相同。
就像写入一样，压缩也使用记录有效负载类将日志中的记录彼此合并，再次与基本文件合并，并生成压缩后要写入的最终记录。</span>


    
### 指标配置
配置Hudi指标报告。
[withMetricsConfig](#withMetricsConfig) (HoodieMetricsConfig) <br/>
<span style="color:grey">Hudi会发布有关每次提交、清理、回滚等的指标。</span>

#### GRAPHITE

##### on(metricsOn = false) {#on}
属性：`hoodie.metrics.on` <br/>
<span style="color:grey">打开或关闭发送指标。默认情况下处于关闭状态。</span>

##### withReporterType(reporterType = GRAPHITE) {#withReporterType}
属性：`hoodie.metrics.reporter.type` <br/>
<span style="color:grey">指标报告者的类型。默认使用graphite。</span>

##### toGraphiteHost(host = localhost) {#toGraphiteHost}
属性：`hoodie.metrics.graphite.host` <br/>
<span style="color:grey">要连接的graphite主机</span>

##### onGraphitePort(port = 4756) {#onGraphitePort}
属性：`hoodie.metrics.graphite.port` <br/>
<span style="color:grey">要连接的graphite端口</span>

##### usePrefix(prefix = "") {#usePrefix}
属性：`hoodie.metrics.graphite.metric.prefix` <br/>
<span style="color:grey">适用于所有指标的标准前缀。这有助于添加如数据中心、环境等信息</span>

#### JMX

##### on(metricsOn = false) {#on}
属性：`hoodie.metrics.on` <br/>
<span style="color:grey">打开或关闭发送指标。默认情况下处于关闭状态。</span>

##### withReporterType(reporterType = JMX) {#withReporterType}
属性：`hoodie.metrics.reporter.type` <br/>
<span style="color:grey">指标报告者的类型。</span>

##### toJmxHost(host = localhost) {#toJmxHost}
属性：`hoodie.metrics.jmx.host` <br/>
<span style="color:grey">要连接的Jmx主机</span>

##### onJmxPort(port = 1000-5000) {#onJmxPort}
属性：`hoodie.metrics.graphite.port` <br/>
<span style="color:grey">要连接的Jmx端口</span>

#### DATADOG

##### on(metricsOn = false) {#on}
属性：`hoodie.metrics.on` <br/>
<span style="color:grey">打开或关闭发送指标。默认情况下处于关闭状态。</span>

##### withReporterType(reporterType = DATADOG) {#withReporterType}
属性： `hoodie.metrics.reporter.type` <br/>
<span style="color:grey">指标报告者的类型。</span>

##### withDatadogReportPeriodSeconds(period = 30) {#withDatadogReportPeriodSeconds}
属性： `hoodie.metrics.datadog.report.period.seconds` <br/>
<span style="color:grey">Datadog报告周期，单位为秒，默认30秒。</span>

##### withDatadogApiSite(apiSite) {#withDatadogApiSite}
属性： `hoodie.metrics.datadog.api.site` <br/>
<span style="color:grey">Datadog API站点：EU 或者 US</span>

##### withDatadogApiKey(apiKey) {#withDatadogApiKey}
属性： `hoodie.metrics.datadog.api.key` <br/>
<span style="color:grey">Datadog API密匙</span>

##### withDatadogApiKeySkipValidation(skip = false) {#withDatadogApiKeySkipValidation}
属性： `hoodie.metrics.datadog.api.key.skip.validation` <br/>
<span style="color:grey">在通过Datadog API发送指标前，选择是否跳过验证API密匙。默认不跳过。</span>

##### withDatadogApiKeySupplier(apiKeySupplier) {#withDatadogApiKeySupplier}
属性： `hoodie.metrics.datadog.api.key.supplier` <br/>
<span style="color:grey">Datadog API 密匙提供者，用来在运行时提供密匙。只有当`hoodie.metrics.datadog.api.key`未设定的情况下才有效。</span>

##### withDatadogApiTimeoutSeconds(timeout = 3) {#withDatadogApiTimeoutSeconds}
属性： `hoodie.metrics.datadog.metric.prefix` <br/>
<span style="color:grey">Datadog API超时时长，单位为秒，默认3秒。</span>

##### withDatadogPrefix(prefix) {#withDatadogPrefix}
属性： `hoodie.metrics.datadog.metric.prefix` <br/>
<span style="color:grey">Datadog指标前缀。将被加在所有指标名称前，以点间隔。例如：如果设成`foo`，`foo.`将被用作实际前缀。</span>

##### withDatadogHost(host) {#withDatadogHost}
属性： `hoodie.metrics.datadog.metric.host` <br/>
<span style="color:grey">Datadog指标主机，将和指标数据一并发送。</span>

##### withDatadogTags(tags) {#withDatadogTags}
属性： `hoodie.metrics.datadog.metric.tags` <br/>
<span style="color:grey">Datadog指标标签（逗号分隔），将和指标数据一并发送。</span>

### 内存配置
控制由Hudi内部执行的压缩和合并的内存使用情况
[withMemoryConfig](#withMemoryConfig) (HoodieMemoryConfig) <br/>
<span style="color:grey">内存相关配置</span>

#### withMaxMemoryFractionPerPartitionMerge(maxMemoryFractionPerPartitionMerge = 0.6) {#withMaxMemoryFractionPerPartitionMerge} 
属性：`hoodie.memory.merge.fraction` <br/>
<span style="color:grey">该比例乘以用户内存比例（1-spark.memory.fraction）以获得合并期间要使用的堆空间的最终比例</span>

#### withMaxMemorySizePerCompactionInBytes(maxMemorySizePerCompactionInBytes = 1GB) {#withMaxMemorySizePerCompactionInBytes} 
属性：`hoodie.memory.compaction.fraction` <br/>
<span style="color:grey">HoodieCompactedLogScanner读取日志块，将记录转换为HoodieRecords，然后合并这些日志块和记录。
在任何时候，日志块中的条目数可以小于或等于相应的parquet文件中的条目数。这可能导致Scanner出现OOM。
因此，可溢出的映射有助于减轻内存压力。使用此配置来设置可溢出映射的最大允许inMemory占用空间。</span>

#### withWriteStatusFailureFraction(failureFraction = 0.1) {#withWriteStatusFailureFraction}
属性：`hoodie.memory.writestatus.failure.fraction` <br/>
<span style="color:grey">此属性控制报告给驱动程序的失败记录和异常的比例</span>
