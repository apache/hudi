var store = [{
        "title": "S3 Filesystem",
        "excerpt":"In this page, we explain how to get your Hudi spark job to store into AWS S3. AWS configs There are two configurations required for Hudi-S3 compatibility: Adding AWS Credentials for Hudi Adding required Jars to classpath AWS Credentials Simplest way to use Hudi with S3, is to configure your...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/0.5.0-s3_hoodie.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "S3 Filesystem",
        "excerpt":"In this page, we explain how to get your Hudi spark job to store into AWS S3. AWS configs There are two configurations required for Hudi-S3 compatibility: Adding AWS Credentials for Hudi Adding required Jars to classpath AWS Credentials Simplest way to use Hudi with S3, is to configure your...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/0.5.0-s3_hoodie.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "GCS Filesystem",
        "excerpt":"For Hudi storage on GCS, regional buckets provide an DFS API with strong consistency. GCS Configs There are two configurations required for Hudi GCS compatibility: Adding GCS Credentials for Hudi Adding required jars to classpath GCS Credentials Add the required configs in your core-site.xml from where Hudi can fetch them....","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/0.5.0-gcs_hoodie.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "GCS Filesystem",
        "excerpt":"For Hudi storage on GCS, regional buckets provide an DFS API with strong consistency. GCS Configs There are two configurations required for Hudi GCS compatibility: Adding GCS Credentials for Hudi Adding required jars to classpath GCS Credentials Add the required configs in your core-site.xml from where Hudi can fetch them....","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/0.5.0-gcs_hoodie.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Migration Guide",
        "excerpt":"Hudi maintains metadata such as commit timeline and indexes to manage a dataset. The commit timelines helps to understand the actions happening on a dataset as well as the current state of a dataset. Indexes are used by Hudi to maintain a record key to file id mapping to efficiently...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/0.5.0-migration_guide.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Migration Guide",
        "excerpt":"Hudi maintains metadata such as commit timeline and indexes to manage a dataset. The commit timelines helps to understand the actions happening on a dataset as well as the current state of a dataset. Indexes are used by Hudi to maintain a record key to file id mapping to efficiently...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/0.5.0-migration_guide.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Docker Demo",
        "excerpt":"A Demo using docker containers Lets use a real world example to see how hudi works end to end. For this purpose, a self contained data infrastructure is brought up in a local docker cluster within your computer. The steps have been tested on a Mac laptop Prerequisites Docker Setup...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/0.5.0-docker_demo.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Docker Demo",
        "excerpt":"A Demo using docker containers Lets use a real world example to see how hudi works end to end. For this purpose, a self contained data infrastructure is brought up in a local docker cluster within your computer. The steps have been tested on a Mac laptop Prerequisites Docker Setup...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/0.5.0-docker_demo.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Quick-Start Guide",
        "excerpt":"本指南通过使用spark-shell简要介绍了Hudi功能。使用Spark数据源，我们将通过代码段展示如何插入和更新的Hudi默认存储类型数据集： 写时复制。每次写操作之后，我们还将展示如何读取快照和增量读取数据。 设置spark-shell Hudi适用于Spark-2.x版本。您可以按照此处的说明设置spark。 在提取的目录中，使用spark-shell运行Hudi： bin/spark-shell --packages org.apache.hudi:hudi-spark-bundle:0.5.0-incubating --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' 设置表名、基本路径和数据生成器来为本指南生成记录。 import org.apache.hudi.QuickstartUtils._ import scala.collection.JavaConversions._ import org.apache.spark.sql.SaveMode._ import org.apache.hudi.DataSourceReadOptions._ import org.apache.hudi.DataSourceWriteOptions._ import org.apache.hudi.config.HoodieWriteConfig._ val tableName = \"hudi_cow_table\" val basePath = \"file:///tmp/hudi_cow_table\" val dataGen = new DataGenerator 数据生成器 可以基于行程样本模式 生成插入和更新的样本。 插入数据 生成一些新的行程样本，将其加载到DataFrame中，然后将DataFrame写入Hudi数据集中，如下所示。 val inserts = convertToStringList(dataGen.generateInserts(10)) val df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/0.5.0-quick-start-guide.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Quick-Start Guide",
        "excerpt":"This guide provides a quick peek at Hudi’s capabilities using spark-shell. Using Spark datasources, we will walk through code snippets that allows you to insert and update a Hudi dataset of default storage type: Copy on Write. After each write operation we will also show how to read the data...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/0.5.0-quick-start-guide.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Structure",
        "excerpt":"Hudi (pronounced “Hoodie”) ingests &amp; manages storage of large analytical datasets over DFS (HDFS or cloud stores) and provides three logical views for query access. Read Optimized View - Provides excellent query performance on pure columnar storage, much like plain Parquet tables. Incremental View - Provides a change stream out...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/0.5.0-structure.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "使用案例",
        "excerpt":"以下是一些使用Hudi的示例，说明了加快处理速度和提高效率的好处 近实时摄取 将外部源(如事件日志、数据库、外部源)的数据摄取到Hadoop数据湖是一个众所周知的问题。 尽管这些数据对整个组织来说是最有价值的，但不幸的是，在大多数(如果不是全部)Hadoop部署中都使用零散的方式解决，即使用多个不同的摄取工具。 对于RDBMS摄取，Hudi提供 通过更新插入达到更快加载，而不是昂贵且低效的批量加载。例如，您可以读取MySQL BIN日志或Sqoop增量导入并将其应用于 DFS上的等效Hudi表。这比批量合并任务及复杂的手工合并工作流更快/更有效率。 对于NoSQL数据存储，如Cassandra / Voldemort / HBase，即使是中等规模大小也会存储数十亿行。 毫无疑问， 全量加载不可行，如果摄取需要跟上较高的更新量，那么则需要更有效的方法。 即使对于像Kafka这样的不可变数据源，Hudi也可以 强制在HDFS上使用最小文件大小, 这采取了综合方式解决HDFS小文件问题来改善NameNode的健康状况。这对事件流来说更为重要，因为它通常具有较高容量(例如：点击流)，如果管理不当，可能会对Hadoop集群造成严重损害。 在所有源中，通过commits这一概念，Hudi增加了以原子方式向消费者发布新数据的功能，这种功能十分必要。 近实时分析 通常，实时数据集市由专业(实时)数据分析存储提供支持，例如Druid或Memsql或OpenTSDB。 这对于较小规模的数据量来说绝对是完美的(相比于这样安装Hadoop)，这种情况需要在亚秒级响应查询，例如系统监控或交互式实时分析。 但是，由于Hadoop上的数据太陈旧了，通常这些系统会被滥用于非交互式查询，这导致利用率不足和硬件/许可证成本的浪费。 另一方面，Hadoop上的交互式SQL解决方案(如Presto和SparkSQL)表现出色，在 几秒钟内完成查询。 通过将 数据新鲜度提高到几分钟，Hudi可以提供一个更有效的替代方案，并支持存储在DFS中的 数量级更大的数据集 的实时分析。 此外，Hudi没有外部依赖(如专用于实时分析的HBase集群)，因此可以在更新的分析上实现更快的分析，而不会增加操作开销。 增量处理管道 Hadoop提供的一个基本能力是构建一系列数据集，这些数据集通过表示为工作流的DAG相互派生。 工作流通常取决于多个上游工作流输出的新数据，新数据的可用性传统上由新的DFS文件夹/Hive分区指示。 让我们举一个具体的例子来说明这点。上游工作流U可以每小时创建一个Hive分区，在每小时结束时(processing_time)使用该小时的数据(event_time)，提供1小时的有效新鲜度。 然后，下游工作流D在U结束后立即启动，并在下一个小时内自行处理，将有效延迟时间增加到2小时。 上面的示例忽略了迟到的数据，即processing_time和event_time分开时。 不幸的是，在今天的后移动和前物联网世界中，来自间歇性连接的移动设备和传感器的延迟数据是常态，而不是异常。 在这种情况下，保证正确性的唯一补救措施是重新处理最后几个小时的数据， 每小时一遍又一遍，这可能会严重影响整个生态系统的效率。例如; 试想一下，在数百个工作流中每小时重新处理TB数据。 Hudi通过以单个记录为粒度的方式(而不是文件夹/分区)从上游 Hudi数据集HU消费新数据(包括迟到数据)，来解决上面的问题。 应用处理逻辑，并使用下游Hudi数据集HD高效更新/协调迟到数据。在这里，HU和HD可以以更频繁的时间被连续调度 比如15分钟，并且HD提供端到端30分钟的延迟。 为实现这一目标，Hudi采用了类似于Spark Streaming、发布/订阅系统等流处理框架，以及像Kafka 或Oracle XStream等数据库复制技术的类似概念。 如果感兴趣，可以在这里找到有关增量处理(相比于流处理和批处理)好处的更详细解释。 DFS的数据分发...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/0.5.0-use_cases.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Use Cases",
        "excerpt":"Near Real-Time Ingestion Ingesting data from external sources like (event logs, databases, external sources) into a Hadoop Data Lake is a well known problem. In most (if not all) Hadoop deployments, it is unfortunately solved in a piecemeal fashion, using a medley of ingestion tools, even though this data is...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/0.5.0-use_cases.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "演讲 & Hudi 用户",
        "excerpt":"已使用 Uber Hudi最初由Uber开发，用于实现低延迟、高效率的数据库摄取。 Hudi自2016年8月开始在生产环境上线，在Hadoop上驱动约100个非常关键的业务表，支撑约几百TB的数据规模(前10名包括行程、乘客、司机)。 Hudi还支持几个增量的Hive ETL管道，并且目前已集成到Uber的数据分发系统中。 EMIS Health EMIS Health是英国最大的初级保健IT软件提供商，其数据集包括超过5000亿的医疗保健记录。HUDI用于管理生产中的分析数据集，并使其与上游源保持同步。Presto用于查询以HUDI格式写入的数据。 Yields.io Yields.io是第一个使用AI在企业范围内进行自动模型验证和实时监控的金融科技平台。他们的数据湖由Hudi管理，他们还积极使用Hudi为增量式、跨语言/平台机器学习构建基础架构。 Yotpo Hudi在Yotpo有不少用途。首先，在他们的开源ETL框架中集成了Hudi作为CDC管道的输出写入程序，即从数据库binlog生成的事件流到Kafka然后再写入S3。 演讲 &amp; 报告 “Hoodie: Incremental processing on Hadoop at Uber” - By Vinoth Chandar &amp; Prasanna Rajaperumal Mar 2017, Strata + Hadoop World, San Jose, CA “Hoodie: An Open Source Incremental Processing Framework From Uber” -...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/0.5.0-powered_by.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Talks & Powered By",
        "excerpt":"Adoption Uber Apache Hudi was originally developed at Uber, to achieve low latency database ingestion, with high efficiency. It has been in production since Aug 2016, powering the massive 100PB data lake, including highly business critical tables like core trips,riders,partners. It also powers several incremental Hive ETL pipelines and being...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/0.5.0-powered_by.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "对比",
        "excerpt":"Apache Hudi填补了在DFS上处理数据的巨大空白，并可以和这些技术很好地共存。然而， 通过将Hudi与一些相关系统进行对比，来了解Hudi如何适应当前的大数据生态系统，并知晓这些系统在设计中做的不同权衡仍将非常有用。   Kudu   Apache Kudu是一个与Hudi具有相似目标的存储系统，该系统通过对upserts支持来对PB级数据进行实时分析。 一个关键的区别是Kudu还试图充当OLTP工作负载的数据存储，而Hudi并不希望这样做。 因此，Kudu不支持增量拉取(截至2017年初)，而Hudi支持以便进行增量处理。   Kudu与分布式文件系统抽象和HDFS完全不同，它自己的一组存储服务器通过RAFT相互通信。 与之不同的是，Hudi旨在与底层Hadoop兼容的文件系统(HDFS，S3或Ceph)一起使用，并且没有自己的存储服务器群，而是依靠Apache Spark来完成繁重的工作。 因此，Hudi可以像其他Spark作业一样轻松扩展，而Kudu则需要硬件和运营支持，特别是HBase或Vertica等数据存储系统。 到目前为止，我们还没有做任何直接的基准测试来比较Kudu和Hudi(鉴于RTTable正在进行中)。 但是，如果我们要使用CERN， 我们预期Hudi在摄取parquet上有更卓越的性能。   Hive事务   Hive事务/ACID是另一项类似的工作，它试图实现在ORC文件格式之上的存储读取时合并。 可以理解，此功能与Hive以及LLAP之类的其他工作紧密相关。 Hive事务不提供Hudi提供的读取优化存储选项或增量拉取。 在实现选择方面，Hudi充分利用了类似Spark的处理框架的功能，而Hive事务特性则在用户或Hive Metastore启动的Hive任务/查询的下实现。 根据我们的生产经验，与其他方法相比，将Hudi作为库嵌入到现有的Spark管道中要容易得多，并且操作不会太繁琐。 Hudi还设计用于与Presto/Spark等非Hive引擎合作，并计划引入除parquet以外的文件格式。   HBase   尽管HBase最终是OLTP工作负载的键值存储层，但由于与Hadoop的相似性，用户通常倾向于将HBase与分析相关联。 鉴于HBase经过严格的写优化，它支持开箱即用的亚秒级更新，Hive-on-HBase允许用户查询该数据。 但是，就分析工作负载的实际性能而言，Parquet/ORC之类的混合列式存储格式可以轻松击败HBase，因为这些工作负载主要是读取繁重的工作。 Hudi弥补了更快的数据与分析存储格式之间的差距。从运营的角度来看，与管理分析使用的HBase region服务器集群相比，为用户提供可更快给出数据的库更具可扩展性。 最终，HBase不像Hudi这样重点支持提交时间、增量拉取之类的增量处理原语。   流式处理   一个普遍的问题：”Hudi与流处理系统有何关系？”，我们将在这里尝试回答。简而言之，Hudi可以与当今的批处理(写时复制存储)和流处理(读时合并存储)作业集成，以将计算结果存储在Hadoop中。 对于Spark应用程序，这可以通过将Hudi库与Spark/Spark流式DAG直接集成来实现。在非Spark处理系统(例如Flink、Hive)情况下，可以在相应的系统中进行处理，然后通过Kafka主题/DFS中间文件将其发送到Hudi表中。从概念上讲，数据处理 管道仅由三个部分组成：输入，处理，输出，用户最终针对输出运行查询以便使用管道的结果。Hudi可以充当将数据存储在DFS上的输入或输出。Hudi在给定流处理管道上的适用性最终归结为你的查询在Presto/SparkSQL/Hive的适用性。   更高级的用例围绕增量处理的概念展开， 甚至在处理引擎内部也使用Hudi来加速典型的批处理管道。例如：Hudi可用作DAG内的状态存储(类似Flink使用的[rocksDB(https://ci.apache.org/projects/flink/flink-docs-release-1.2/ops/state_backends.html#the-rocksdbstatebackend))。 这是路线图上的一个项目并将最终以Beam Runner的形式呈现。  ","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/0.5.0-comparison.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Comparison",
        "excerpt":"Apache Hudi fills a big void for processing data on top of DFS, and thus mostly co-exists nicely with these technologies. However, it would be useful to understand how Hudi fits into the current big data ecosystem, contrasting it with a few related systems and bring out the different tradeoffs...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/0.5.0-comparison.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "概念",
        "excerpt":"Apache Hudi(发音为“Hudi”)在DFS的数据集上提供以下流原语 插入更新 (如何改变数据集?) 增量拉取 (如何获取变更的数据?) 在本节中，我们将讨论重要的概念和术语，这些概念和术语有助于理解并有效使用这些原语。 时间轴 在它的核心，Hudi维护一条包含在不同的即时时间所有对数据集操作的时间轴，从而提供，从不同时间点出发得到不同的视图下的数据集。Hudi即时包含以下组件 操作类型 : 对数据集执行的操作类型 即时时间 : 即时时间通常是一个时间戳(例如：20190117010349)，该时间戳按操作开始时间的顺序单调增加。 状态 : 即时的状态 Hudi保证在时间轴上执行的操作的原子性和基于即时时间的时间轴一致性。 执行的关键操作包括 COMMITS - 一次提交表示将一组记录原子写入到数据集中。 CLEANS - 删除数据集中不再需要的旧文件版本的后台活动。 DELTA_COMMIT - 增量提交是指将一批记录原子写入到MergeOnRead存储类型的数据集中，其中一些/所有数据都可以只写到增量日志中。 COMPACTION - 协调Hudi中差异数据结构的后台活动，例如：将更新从基于行的日志文件变成列格式。在内部，压缩表现为时间轴上的特殊提交。 ROLLBACK - 表示提交/增量提交不成功且已回滚，删除在写入过程中产生的所有部分文件。 SAVEPOINT - 将某些文件组标记为”已保存”，以便清理程序不会将其删除。在发生灾难/数据恢复的情况下，它有助于将数据集还原到时间轴上的某个点。 任何给定的即时都可以处于以下状态之一 REQUESTED - 表示已调度但尚未启动的操作。 INFLIGHT - 表示当前正在执行该操作。 COMPLETED - 表示在时间轴上完成了该操作。 上面的示例显示了在Hudi数据集上大约10:00到10:20之间发生的更新事件，大约每5分钟一次，将提交元数据以及其他后台清理/压缩保留在Hudi时间轴上。 观察的关键点是：提交时间指示数据的到达时间（上午10:20），而实际数据组织则反映了实际时间或事件时间，即数据所反映的（从07:00开始的每小时时段）。在权衡数据延迟和完整性时，这是两个关键概念。...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/0.5.0-concepts.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Concepts",
        "excerpt":"Apache Hudi (pronounced “Hudi”) provides the following streaming primitives over datasets on DFS Upsert (how do I change the dataset?) Incremental pull (how do I fetch data that changed?) In this section, we will discuss key concepts &amp; terminologies that are important to understand, to be able to effectively use...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/0.5.0-concepts.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "写入 Hudi 数据集",
        "excerpt":"这一节我们将介绍使用DeltaStreamer工具从外部源甚至其他Hudi数据集摄取新更改的方法， 以及通过使用Hudi数据源的upserts加快大型Spark作业的方法。 对于此类数据集，我们可以使用各种查询引擎查询它们。 写操作 在此之前，了解Hudi数据源及delta streamer工具提供的三种不同的写操作以及如何最佳利用它们可能会有所帮助。 这些操作可以在针对数据集发出的每个提交/增量提交中进行选择/更改。 UPSERT（插入更新） ：这是默认操作，在该操作中，通过查找索引，首先将输入记录标记为插入或更新。 在运行启发式方法以确定如何最好地将这些记录放到存储上，如优化文件大小之类后，这些记录最终会被写入。 对于诸如数据库更改捕获之类的用例，建议该操作，因为输入几乎肯定包含更新。 INSERT（插入） ：就使用启发式方法确定文件大小而言，此操作与插入更新（UPSERT）非常相似，但此操作完全跳过了索引查找步骤。 因此，对于日志重复数据删除等用例（结合下面提到的过滤重复项的选项），它可以比插入更新快得多。 插入也适用于这种用例，这种情况数据集可以允许重复项，但只需要Hudi的事务写/增量提取/存储管理功能。 BULK_INSERT（批插入） ：插入更新和插入操作都将输入记录保存在内存中，以加快存储优化启发式计算的速度（以及其它未提及的方面）。 所以对Hudi数据集进行初始加载/引导时这两种操作会很低效。批量插入提供与插入相同的语义，但同时实现了基于排序的数据写入算法， 该算法可以很好地扩展数百TB的初始负载。但是，相比于插入和插入更新能保证文件大小，批插入在调整文件大小上只能尽力而为。 DeltaStreamer HoodieDeltaStreamer实用工具 (hudi-utilities-bundle中的一部分) 提供了从DFS或Kafka等不同来源进行摄取的方式，并具有以下功能。 从Kafka单次摄取新事件，从Sqoop、HiveIncrementalPuller输出或DFS文件夹中的多个文件 增量导入 支持json、avro或自定义记录类型的传入数据 管理检查点，回滚和恢复 利用DFS或Confluent schema注册表的Avro模式。 支持自定义转换操作 命令行选项更详细地描述了这些功能： [hoodie]$ spark-submit --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer `ls packaging/hudi-utilities-bundle/target/hudi-utilities-bundle-*.jar` --help Usage: &lt;main class&gt; [options] Options: --commit-on-errors Commit even when some records failed...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/0.5.0-writing_data.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Writing Hudi Datasets",
        "excerpt":"In this section, we will cover ways to ingest new changes from external sources or even other Hudi datasets using the DeltaStreamer tool, as well as speeding up large Spark jobs via upserts using the Hudi datasource. Such datasets can then be queried using various query engines. Write Operations Before...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/0.5.0-writing_data.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "查询 Hudi 数据集",
        "excerpt":"从概念上讲，Hudi物理存储一次数据到DFS上，同时在其上提供三个逻辑视图，如之前所述。 数据集同步到Hive Metastore后，它将提供由Hudi的自定义输入格式支持的Hive外部表。一旦提供了适当的Hudi捆绑包， 就可以通过Hive、Spark和Presto之类的常用查询引擎来查询数据集。 具体来说，在写入过程中传递了两个由table name命名的Hive表。 例如，如果table name = hudi_tbl，我们得到 hudi_tbl 实现了由 HoodieParquetInputFormat 支持的数据集的读优化视图，从而提供了纯列式数据。 hudi_tbl_rt 实现了由 HoodieParquetRealtimeInputFormat 支持的数据集的实时视图，从而提供了基础数据和日志数据的合并视图。 如概念部分所述，增量处理所需要的 一个关键原语是增量拉取（以从数据集中获取更改流/日志）。您可以增量提取Hudi数据集，这意味着自指定的即时时间起， 您可以只获得全部更新和新行。 这与插入更新一起使用，对于构建某些数据管道尤其有用，包括将1个或多个源Hudi表（数据流/事实）以增量方式拉出（流/事实） 并与其他表（数据集/维度）结合以写出增量到目标Hudi数据集。增量视图是通过查询上表之一实现的，并具有特殊配置， 该特殊配置指示查询计划仅需要从数据集中获取增量数据。 接下来，我们将详细讨论在每个查询引擎上如何访问所有三个视图。 Hive 为了使Hive能够识别Hudi数据集并正确查询， HiveServer2需要在其辅助jars路径中提供hudi-hadoop-mr-bundle-x.y.z-SNAPSHOT.jar。 这将确保输入格式类及其依赖项可用于查询计划和执行。 读优化表 除了上述设置之外，对于beeline cli访问，还需要将hive.input.format变量设置为org.apache.hudi.hadoop.HoodieParquetInputFormat输入格式的完全限定路径名。 对于Tez，还需要将hive.tez.input.format设置为org.apache.hadoop.hive.ql.io.HiveInputFormat。 实时表 除了在HiveServer2上安装Hive捆绑jars之外，还需要将其放在整个集群的hadoop/hive安装中，这样查询也可以使用自定义RecordReader。 增量拉取 HiveIncrementalPuller允许通过HiveQL从大型事实/维表中增量提取更改， 结合了Hive（可靠地处理复杂的SQL查询）和增量原语的好处（通过增量拉取而不是完全扫描来加快查询速度）。 该工具使用Hive JDBC运行hive查询并将其结果保存在临时表中，这个表可以被插入更新。 Upsert实用程序（HoodieDeltaStreamer）具有目录结构所需的所有状态，以了解目标表上的提交时间应为多少。 例如：/app/incremental-hql/intermediate/{source_table_name}_temp/{last_commit_included}。 已注册的Delta Hive表的格式为{tmpdb}.{source_table}_{last_commit_included}。 以下是HiveIncrementalPuller的配置选项 配置 描述 默认值 hiveUrl 要连接的Hive...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/0.5.0-querying_data.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Querying Hudi Datasets",
        "excerpt":"Conceptually, Hudi stores data physically once on DFS, while providing 3 logical views on top, as explained before. Once the dataset is synced to the Hive metastore, it provides external Hive tables backed by Hudi’s custom inputformats. Once the proper hudi bundle has been provided, the dataset can be queried...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/0.5.0-querying_data.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "配置",
        "excerpt":"该页面介绍了几种配置写入或读取Hudi数据集的作业的方法。 简而言之，您可以在几个级别上控制行为。 Spark数据源配置 : 这些配置控制Hudi Spark数据源，提供如下功能： 定义键和分区、选择写操作、指定如何合并记录或选择要读取的视图类型。 WriteClient 配置 : 在内部，Hudi数据源使用基于RDD的HoodieWriteClient API 真正执行对存储的写入。 这些配置可对文件大小、压缩（compression）、并行度、压缩（compaction）、写入模式、清理等底层方面进行完全控制。 尽管Hudi提供了合理的默认设置，但在不同情形下，可能需要对这些配置进行调整以针对特定的工作负载进行优化。 RecordPayload 配置 : 这是Hudi提供的最底层的定制。 RecordPayload定义了如何根据传入的新记录和存储的旧记录来产生新值以进行插入更新。 Hudi提供了诸如OverwriteWithLatestAvroPayload的默认实现，该实现仅使用最新或最后写入的记录来更新存储。 在数据源和WriteClient级别，都可以将其重写为扩展HoodieRecordPayload类的自定义类。 与云存储连接 无论使用RDD/WriteClient API还是数据源，以下信息都有助于配置对云存储的访问。 AWS S3 S3和Hudi协同工作所需的配置。 Google Cloud Storage GCS和Hudi协同工作所需的配置。 Spark数据源配置 可以通过将以下选项传递到option(k,v)方法中来配置使用数据源的Spark作业。 实际的数据源级别配置在下面列出。 写选项 另外，您可以使用options()或option(k,v)方法直接传递任何WriteClient级别的配置。 inputDF.write() .format(\"org.apache.hudi\") .options(clientOpts) // 任何Hudi客户端选项都可以传入 .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), \"_row_key\") .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), \"partition\") .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), \"timestamp\") .option(HoodieWriteConfig.TABLE_NAME, tableName)...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/0.5.0-configurations.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Configurations",
        "excerpt":"This page covers the different ways of configuring your job to write/read Hudi datasets. At a high level, you can control behaviour at few levels. Spark Datasource Configs : These configs control the Hudi Spark Datasource, providing ability to define keys/partitioning, pick out the write operation, specify how to merge...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/0.5.0-configurations.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "性能",
        "excerpt":"在本节中，我们将介绍一些有关Hudi插入更新、增量提取的实际性能数据，并将其与实现这些任务的其它传统工具进行比较。   插入更新   下面显示了从NoSQL数据库摄取获得的速度提升，这些速度提升数据是通过在写入时复制存储上的Hudi数据集上插入更新而获得的， 数据集包括5个从小到大的表（相对于批量加载表）。           由于Hudi可以通过增量构建数据集，它也为更频繁地调度摄取提供了可能性，从而减少了延迟，并显著节省了总体计算成本。           Hudi插入更新在t1表的一次提交中就进行了高达4TB的压力测试。 有关一些调优技巧，请参见这里。   索引   为了有效地插入更新数据，Hudi需要将要写入的批量数据中的记录分类为插入和更新（并标记它所属的文件组）。 为了加快此操作的速度，Hudi采用了可插拔索引机制，该机制存储了recordKey和它所属的文件组ID之间的映射。 默认情况下，Hudi使用内置索引，该索引使用文件范围和布隆过滤器来完成此任务，相比于Spark Join，其速度最高可提高10倍。   当您将recordKey建模为单调递增时（例如时间戳前缀），Hudi提供了最佳的索引性能，从而进行范围过滤来避免与许多文件进行比较。 即使对于基于UUID的键，也有已知技术来达到同样目的。 例如，在具有80B键、3个分区、11416个文件、10TB数据的事件表上使用100M个时间戳前缀的键（5％的更新，95％的插入）时， 相比于原始Spark Join，Hudi索引速度的提升约为7倍（440秒相比于2880秒）。 即使对于具有挑战性的工作负载，如使用300个核对3.25B UUID键、30个分区、6180个文件的“100％更新”的数据库摄取工作负载，Hudi索引也可以提供80-100％的加速。   读优化查询   读优化视图的主要设计目标是在不影响查询的情况下实现上一节中提到的延迟减少和效率提高。 下图比较了对Hudi和非Hudi数据集的Hive、Presto、Spark查询，并对此进行说明。   Hive           Spark           Presto          ","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/0.5.0-performance.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Performance",
        "excerpt":"In this section, we go over some real world performance numbers for Hudi upserts, incremental pull and compare them against the conventional alternatives for achieving these tasks. Upserts Following shows the speed up obtained for NoSQL database ingestion, from incrementally upserting on a Hudi dataset on the copy-on-write storage, on...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/0.5.0-performance.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "管理 Hudi Pipelines",
        "excerpt":"管理员/运维人员可以通过以下方式了解Hudi数据集/管道 通过Admin CLI进行管理 Graphite指标 Hudi应用程序的Spark UI 本节简要介绍了每一种方法，并提供了有关故障排除的一些常规指南 Admin CLI 一旦构建了hudi，就可以通过cd hudi-cli &amp;&amp; ./hudi-cli.sh启动shell。 一个hudi数据集位于DFS上的basePath位置，我们需要该位置才能连接到Hudi数据集。 Hudi库使用.hoodie子文件夹跟踪所有元数据，从而有效地在内部管理该数据集。 初始化hudi表，可使用如下命令。 18/09/06 15:56:52 INFO annotation.AutowiredAnnotationBeanPostProcessor: JSR-330 'javax.inject.Inject' annotation found and supported for autowiring ============================================ * * * _ _ _ _ * * | | | | | | (_) * * | |__| |...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/0.5.0-admin_guide.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Administering Hudi Pipelines",
        "excerpt":"Admins/ops can gain visibility into Hudi datasets/pipelines in the following ways Administering via the Admin CLI Graphite metrics Spark UI of the Hudi Application This section provides a glimpse into each of these, with some general guidance on troubleshooting Admin CLI Once hudi has been built, the shell can be...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/0.5.0-admin_guide.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Privacy Policy",
        "excerpt":"Information about your use of this website is collected using server access logs and a tracking cookie. The collected information consists of the following: The IP address from which you access the website; The type of browser and operating system you use to access our site; The date and time...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/0.5.0-privacy.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Privacy Policy",
        "excerpt":"Information about your use of this website is collected using server access logs and a tracking cookie. The collected information consists of the following: The IP address from which you access the website; The type of browser and operating system you use to access our site; The date and time...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/0.5.0-privacy.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "文档版本",
        "excerpt":"                                  Latest             英文版             中文版                                      0.5.2             英文版             中文版                                      0.5.1             英文版             中文版                                      0.5.0             英文版             中文版                       ","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/0.5.0-docs-versions.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Docs Versions",
        "excerpt":"                                  Latest             English Version             Chinese Version                                      0.5.2             English Version             Chinese Version                                      0.5.1             English Version             Chinese Version                                      0.5.0             English Version             Chinese Version                       ","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/0.5.0-docs-versions.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "S3 Filesystem",
        "excerpt":"In this page, we explain how to get your Hudi spark job to store into AWS S3. AWS configs There are two configurations required for Hudi-S3 compatibility: Adding AWS Credentials for Hudi Adding required Jars to classpath AWS Credentials Simplest way to use Hudi with S3, is to configure your...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/0.5.1-s3_hoodie.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "S3 Filesystem",
        "excerpt":"In this page, we explain how to get your Hudi spark job to store into AWS S3. AWS configs There are two configurations required for Hudi-S3 compatibility: Adding AWS Credentials for Hudi Adding required Jars to classpath AWS Credentials Simplest way to use Hudi with S3, is to configure your...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/0.5.1-s3_hoodie.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "GCS Filesystem",
        "excerpt":"For Hudi storage on GCS, regional buckets provide an DFS API with strong consistency. GCS Configs There are two configurations required for Hudi GCS compatibility: Adding GCS Credentials for Hudi Adding required jars to classpath GCS Credentials Add the required configs in your core-site.xml from where Hudi can fetch them....","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/0.5.1-gcs_hoodie.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "GCS Filesystem",
        "excerpt":"For Hudi storage on GCS, regional buckets provide an DFS API with strong consistency. GCS Configs There are two configurations required for Hudi GCS compatibility: Adding GCS Credentials for Hudi Adding required jars to classpath GCS Credentials Add the required configs in your core-site.xml from where Hudi can fetch them....","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/0.5.1-gcs_hoodie.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Migration Guide",
        "excerpt":"Hudi maintains metadata such as commit timeline and indexes to manage a dataset. The commit timelines helps to understand the actions happening on a dataset as well as the current state of a dataset. Indexes are used by Hudi to maintain a record key to file id mapping to efficiently...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/0.5.1-migration_guide.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Migration Guide",
        "excerpt":"Hudi maintains metadata such as commit timeline and indexes to manage a table. The commit timelines helps to understand the actions happening on a table as well as the current state of a table. Indexes are used by Hudi to maintain a record key to file id mapping to efficiently...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/0.5.1-migration_guide.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Docker Demo",
        "excerpt":"A Demo using docker containers Lets use a real world example to see how hudi works end to end. For this purpose, a self contained data infrastructure is brought up in a local docker cluster within your computer. The steps have been tested on a Mac laptop Prerequisites Docker Setup...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/0.5.1-docker_demo.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Docker Demo",
        "excerpt":"A Demo using docker containers Lets use a real world example to see how hudi works end to end. For this purpose, a self contained data infrastructure is brought up in a local docker cluster within your computer. The steps have been tested on a Mac laptop Prerequisites Docker Setup...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/0.5.1-docker_demo.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Quick-Start Guide",
        "excerpt":"本指南通过使用spark-shell简要介绍了Hudi功能。使用Spark数据源，我们将通过代码段展示如何插入和更新的Hudi默认存储类型数据集： 写时复制。每次写操作之后，我们还将展示如何读取快照和增量读取数据。 设置spark-shell Hudi适用于Spark-2.x版本。您可以按照此处的说明设置spark。 在提取的目录中，使用spark-shell运行Hudi： bin/spark-shell --packages org.apache.hudi:hudi-spark-bundle:0.5.0-incubating --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' 设置表名、基本路径和数据生成器来为本指南生成记录。 import org.apache.hudi.QuickstartUtils._ import scala.collection.JavaConversions._ import org.apache.spark.sql.SaveMode._ import org.apache.hudi.DataSourceReadOptions._ import org.apache.hudi.DataSourceWriteOptions._ import org.apache.hudi.config.HoodieWriteConfig._ val tableName = \"hudi_cow_table\" val basePath = \"file:///tmp/hudi_cow_table\" val dataGen = new DataGenerator 数据生成器 可以基于行程样本模式 生成插入和更新的样本。 插入数据 生成一些新的行程样本，将其加载到DataFrame中，然后将DataFrame写入Hudi数据集中，如下所示。 val inserts = convertToStringList(dataGen.generateInserts(10)) val df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/0.5.1-quick-start-guide.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Quick-Start Guide",
        "excerpt":"This guide provides a quick peek at Hudi’s capabilities using spark-shell. Using Spark datasources, we will walk through code snippets that allows you to insert and update a Hudi table of default table type: Copy on Write. After each write operation we will also show how to read the data...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/0.5.1-quick-start-guide.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Structure",
        "excerpt":"Hudi (pronounced “Hoodie”) ingests &amp; manages storage of large analytical tables over DFS (HDFS or cloud stores) and provides three types of queries. Read Optimized query - Provides excellent query performance on pure columnar storage, much like plain Parquet tables. Incremental query - Provides a change stream out of the...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/0.5.1-structure.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "使用案例",
        "excerpt":"以下是一些使用Hudi的示例，说明了加快处理速度和提高效率的好处 近实时摄取 将外部源(如事件日志、数据库、外部源)的数据摄取到Hadoop数据湖是一个众所周知的问题。 尽管这些数据对整个组织来说是最有价值的，但不幸的是，在大多数(如果不是全部)Hadoop部署中都使用零散的方式解决，即使用多个不同的摄取工具。 对于RDBMS摄取，Hudi提供 通过更新插入达到更快加载，而不是昂贵且低效的批量加载。例如，您可以读取MySQL BIN日志或Sqoop增量导入并将其应用于 DFS上的等效Hudi表。这比批量合并任务及复杂的手工合并工作流更快/更有效率。 对于NoSQL数据存储，如Cassandra / Voldemort / HBase，即使是中等规模大小也会存储数十亿行。 毫无疑问， 全量加载不可行，如果摄取需要跟上较高的更新量，那么则需要更有效的方法。 即使对于像Kafka这样的不可变数据源，Hudi也可以 强制在HDFS上使用最小文件大小, 这采取了综合方式解决HDFS小文件问题来改善NameNode的健康状况。这对事件流来说更为重要，因为它通常具有较高容量(例如：点击流)，如果管理不当，可能会对Hadoop集群造成严重损害。 在所有源中，通过commits这一概念，Hudi增加了以原子方式向消费者发布新数据的功能，这种功能十分必要。 近实时分析 通常，实时数据集市由专业(实时)数据分析存储提供支持，例如Druid或Memsql或OpenTSDB。 这对于较小规模的数据量来说绝对是完美的(相比于这样安装Hadoop)，这种情况需要在亚秒级响应查询，例如系统监控或交互式实时分析。 但是，由于Hadoop上的数据太陈旧了，通常这些系统会被滥用于非交互式查询，这导致利用率不足和硬件/许可证成本的浪费。 另一方面，Hadoop上的交互式SQL解决方案(如Presto和SparkSQL)表现出色，在 几秒钟内完成查询。 通过将 数据新鲜度提高到几分钟，Hudi可以提供一个更有效的替代方案，并支持存储在DFS中的 数量级更大的数据集 的实时分析。 此外，Hudi没有外部依赖(如专用于实时分析的HBase集群)，因此可以在更新的分析上实现更快的分析，而不会增加操作开销。 增量处理管道 Hadoop提供的一个基本能力是构建一系列数据集，这些数据集通过表示为工作流的DAG相互派生。 工作流通常取决于多个上游工作流输出的新数据，新数据的可用性传统上由新的DFS文件夹/Hive分区指示。 让我们举一个具体的例子来说明这点。上游工作流U可以每小时创建一个Hive分区，在每小时结束时(processing_time)使用该小时的数据(event_time)，提供1小时的有效新鲜度。 然后，下游工作流D在U结束后立即启动，并在下一个小时内自行处理，将有效延迟时间增加到2小时。 上面的示例忽略了迟到的数据，即processing_time和event_time分开时。 不幸的是，在今天的后移动和前物联网世界中，来自间歇性连接的移动设备和传感器的延迟数据是常态，而不是异常。 在这种情况下，保证正确性的唯一补救措施是重新处理最后几个小时的数据， 每小时一遍又一遍，这可能会严重影响整个生态系统的效率。例如; 试想一下，在数百个工作流中每小时重新处理TB数据。 Hudi通过以单个记录为粒度的方式(而不是文件夹/分区)从上游 Hudi数据集HU消费新数据(包括迟到数据)，来解决上面的问题。 应用处理逻辑，并使用下游Hudi数据集HD高效更新/协调迟到数据。在这里，HU和HD可以以更频繁的时间被连续调度 比如15分钟，并且HD提供端到端30分钟的延迟。 为实现这一目标，Hudi采用了类似于Spark Streaming、发布/订阅系统等流处理框架，以及像Kafka 或Oracle XStream等数据库复制技术的类似概念。 如果感兴趣，可以在这里找到有关增量处理(相比于流处理和批处理)好处的更详细解释。 DFS的数据分发...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/0.5.1-use_cases.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Use Cases",
        "excerpt":"Near Real-Time Ingestion Ingesting data from external sources like (event logs, databases, external sources) into a Hadoop Data Lake is a well known problem. In most (if not all) Hadoop deployments, it is unfortunately solved in a piecemeal fashion, using a medley of ingestion tools, even though this data is...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/0.5.1-use_cases.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "演讲 & Hudi 用户",
        "excerpt":"已使用 Uber Hudi最初由Uber开发，用于实现低延迟、高效率的数据库摄取。 Hudi自2016年8月开始在生产环境上线，在Hadoop上驱动约100个非常关键的业务表，支撑约几百TB的数据规模(前10名包括行程、乘客、司机)。 Hudi还支持几个增量的Hive ETL管道，并且目前已集成到Uber的数据分发系统中。 EMIS Health EMIS Health是英国最大的初级保健IT软件提供商，其数据集包括超过5000亿的医疗保健记录。HUDI用于管理生产中的分析数据集，并使其与上游源保持同步。Presto用于查询以HUDI格式写入的数据。 Yields.io Yields.io是第一个使用AI在企业范围内进行自动模型验证和实时监控的金融科技平台。他们的数据湖由Hudi管理，他们还积极使用Hudi为增量式、跨语言/平台机器学习构建基础架构。 Yotpo Hudi在Yotpo有不少用途。首先，在他们的开源ETL框架中集成了Hudi作为CDC管道的输出写入程序，即从数据库binlog生成的事件流到Kafka然后再写入S3。 演讲 &amp; 报告 “Hoodie: Incremental processing on Hadoop at Uber” - By Vinoth Chandar &amp; Prasanna Rajaperumal Mar 2017, Strata + Hadoop World, San Jose, CA “Hoodie: An Open Source Incremental Processing Framework From Uber” -...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/0.5.1-powered_by.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Talks & Powered By",
        "excerpt":"Adoption Uber Apache Hudi was originally developed at Uber, to achieve low latency database ingestion, with high efficiency. It has been in production since Aug 2016, powering the massive 100PB data lake, including highly business critical tables like core trips,riders,partners. It also powers several incremental Hive ETL pipelines and being...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/0.5.1-powered_by.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "对比",
        "excerpt":"Apache Hudi填补了在DFS上处理数据的巨大空白，并可以和这些技术很好地共存。然而， 通过将Hudi与一些相关系统进行对比，来了解Hudi如何适应当前的大数据生态系统，并知晓这些系统在设计中做的不同权衡仍将非常有用。   Kudu   Apache Kudu是一个与Hudi具有相似目标的存储系统，该系统通过对upserts支持来对PB级数据进行实时分析。 一个关键的区别是Kudu还试图充当OLTP工作负载的数据存储，而Hudi并不希望这样做。 因此，Kudu不支持增量拉取(截至2017年初)，而Hudi支持以便进行增量处理。   Kudu与分布式文件系统抽象和HDFS完全不同，它自己的一组存储服务器通过RAFT相互通信。 与之不同的是，Hudi旨在与底层Hadoop兼容的文件系统(HDFS，S3或Ceph)一起使用，并且没有自己的存储服务器群，而是依靠Apache Spark来完成繁重的工作。 因此，Hudi可以像其他Spark作业一样轻松扩展，而Kudu则需要硬件和运营支持，特别是HBase或Vertica等数据存储系统。 到目前为止，我们还没有做任何直接的基准测试来比较Kudu和Hudi(鉴于RTTable正在进行中)。 但是，如果我们要使用CERN， 我们预期Hudi在摄取parquet上有更卓越的性能。   Hive事务   Hive事务/ACID是另一项类似的工作，它试图实现在ORC文件格式之上的存储读取时合并。 可以理解，此功能与Hive以及LLAP之类的其他工作紧密相关。 Hive事务不提供Hudi提供的读取优化存储选项或增量拉取。 在实现选择方面，Hudi充分利用了类似Spark的处理框架的功能，而Hive事务特性则在用户或Hive Metastore启动的Hive任务/查询的下实现。 根据我们的生产经验，与其他方法相比，将Hudi作为库嵌入到现有的Spark管道中要容易得多，并且操作不会太繁琐。 Hudi还设计用于与Presto/Spark等非Hive引擎合作，并计划引入除parquet以外的文件格式。   HBase   尽管HBase最终是OLTP工作负载的键值存储层，但由于与Hadoop的相似性，用户通常倾向于将HBase与分析相关联。 鉴于HBase经过严格的写优化，它支持开箱即用的亚秒级更新，Hive-on-HBase允许用户查询该数据。 但是，就分析工作负载的实际性能而言，Parquet/ORC之类的混合列式存储格式可以轻松击败HBase，因为这些工作负载主要是读取繁重的工作。 Hudi弥补了更快的数据与分析存储格式之间的差距。从运营的角度来看，与管理分析使用的HBase region服务器集群相比，为用户提供可更快给出数据的库更具可扩展性。 最终，HBase不像Hudi这样重点支持提交时间、增量拉取之类的增量处理原语。   流式处理   一个普遍的问题：”Hudi与流处理系统有何关系？”，我们将在这里尝试回答。简而言之，Hudi可以与当今的批处理(写时复制存储)和流处理(读时合并存储)作业集成，以将计算结果存储在Hadoop中。 对于Spark应用程序，这可以通过将Hudi库与Spark/Spark流式DAG直接集成来实现。在非Spark处理系统(例如Flink、Hive)情况下，可以在相应的系统中进行处理，然后通过Kafka主题/DFS中间文件将其发送到Hudi表中。从概念上讲，数据处理 管道仅由三个部分组成：输入，处理，输出，用户最终针对输出运行查询以便使用管道的结果。Hudi可以充当将数据存储在DFS上的输入或输出。Hudi在给定流处理管道上的适用性最终归结为你的查询在Presto/SparkSQL/Hive的适用性。   更高级的用例围绕增量处理的概念展开， 甚至在处理引擎内部也使用Hudi来加速典型的批处理管道。例如：Hudi可用作DAG内的状态存储(类似Flink使用的[rocksDB(https://ci.apache.org/projects/flink/flink-docs-release-1.2/ops/state_backends.html#the-rocksdbstatebackend))。 这是路线图上的一个项目并将最终以Beam Runner的形式呈现。  ","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/0.5.1-comparison.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Comparison",
        "excerpt":"Apache Hudi fills a big void for processing data on top of DFS, and thus mostly co-exists nicely with these technologies. However, it would be useful to understand how Hudi fits into the current big data ecosystem, contrasting it with a few related systems and bring out the different tradeoffs...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/0.5.1-comparison.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "概念",
        "excerpt":"Apache Hudi(发音为“Hudi”)在DFS的数据集上提供以下流原语 插入更新 (如何改变数据集?) 增量拉取 (如何获取变更的数据?) 在本节中，我们将讨论重要的概念和术语，这些概念和术语有助于理解并有效使用这些原语。 时间轴 在它的核心，Hudi维护一条包含在不同的即时时间所有对数据集操作的时间轴，从而提供，从不同时间点出发得到不同的视图下的数据集。Hudi即时包含以下组件 操作类型 : 对数据集执行的操作类型 即时时间 : 即时时间通常是一个时间戳(例如：20190117010349)，该时间戳按操作开始时间的顺序单调增加。 状态 : 即时的状态 Hudi保证在时间轴上执行的操作的原子性和基于即时时间的时间轴一致性。 执行的关键操作包括 COMMITS - 一次提交表示将一组记录原子写入到数据集中。 CLEANS - 删除数据集中不再需要的旧文件版本的后台活动。 DELTA_COMMIT - 增量提交是指将一批记录原子写入到MergeOnRead存储类型的数据集中，其中一些/所有数据都可以只写到增量日志中。 COMPACTION - 协调Hudi中差异数据结构的后台活动，例如：将更新从基于行的日志文件变成列格式。在内部，压缩表现为时间轴上的特殊提交。 ROLLBACK - 表示提交/增量提交不成功且已回滚，删除在写入过程中产生的所有部分文件。 SAVEPOINT - 将某些文件组标记为”已保存”，以便清理程序不会将其删除。在发生灾难/数据恢复的情况下，它有助于将数据集还原到时间轴上的某个点。 任何给定的即时都可以处于以下状态之一 REQUESTED - 表示已调度但尚未启动的操作。 INFLIGHT - 表示当前正在执行该操作。 COMPLETED - 表示在时间轴上完成了该操作。 上面的示例显示了在Hudi数据集上大约10:00到10:20之间发生的更新事件，大约每5分钟一次，将提交元数据以及其他后台清理/压缩保留在Hudi时间轴上。 观察的关键点是：提交时间指示数据的到达时间（上午10:20），而实际数据组织则反映了实际时间或事件时间，即数据所反映的（从07:00开始的每小时时段）。在权衡数据延迟和完整性时，这是两个关键概念。...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/0.5.1-concepts.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Concepts",
        "excerpt":"Apache Hudi (pronounced “Hudi”) provides the following streaming primitives over hadoop compatible storages Update/Delete Records (how do I change records in a table?) Change Streams (how do I fetch records that changed?) In this section, we will discuss key concepts &amp; terminologies that are important to understand, to be able...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/0.5.1-concepts.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "写入 Hudi 数据集",
        "excerpt":"这一节我们将介绍使用DeltaStreamer工具从外部源甚至其他Hudi数据集摄取新更改的方法， 以及通过使用Hudi数据源的upserts加快大型Spark作业的方法。 对于此类数据集，我们可以使用各种查询引擎查询它们。 写操作 在此之前，了解Hudi数据源及delta streamer工具提供的三种不同的写操作以及如何最佳利用它们可能会有所帮助。 这些操作可以在针对数据集发出的每个提交/增量提交中进行选择/更改。 UPSERT（插入更新） ：这是默认操作，在该操作中，通过查找索引，首先将输入记录标记为插入或更新。 在运行启发式方法以确定如何最好地将这些记录放到存储上，如优化文件大小之类后，这些记录最终会被写入。 对于诸如数据库更改捕获之类的用例，建议该操作，因为输入几乎肯定包含更新。 INSERT（插入） ：就使用启发式方法确定文件大小而言，此操作与插入更新（UPSERT）非常相似，但此操作完全跳过了索引查找步骤。 因此，对于日志重复数据删除等用例（结合下面提到的过滤重复项的选项），它可以比插入更新快得多。 插入也适用于这种用例，这种情况数据集可以允许重复项，但只需要Hudi的事务写/增量提取/存储管理功能。 BULK_INSERT（批插入） ：插入更新和插入操作都将输入记录保存在内存中，以加快存储优化启发式计算的速度（以及其它未提及的方面）。 所以对Hudi数据集进行初始加载/引导时这两种操作会很低效。批量插入提供与插入相同的语义，但同时实现了基于排序的数据写入算法， 该算法可以很好地扩展数百TB的初始负载。但是，相比于插入和插入更新能保证文件大小，批插入在调整文件大小上只能尽力而为。 DeltaStreamer HoodieDeltaStreamer实用工具 (hudi-utilities-bundle中的一部分) 提供了从DFS或Kafka等不同来源进行摄取的方式，并具有以下功能。 从Kafka单次摄取新事件，从Sqoop、HiveIncrementalPuller输出或DFS文件夹中的多个文件 增量导入 支持json、avro或自定义记录类型的传入数据 管理检查点，回滚和恢复 利用DFS或Confluent schema注册表的Avro模式。 支持自定义转换操作 命令行选项更详细地描述了这些功能： [hoodie]$ spark-submit --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer `ls packaging/hudi-utilities-bundle/target/hudi-utilities-bundle-*.jar` --help Usage: &lt;main class&gt; [options] Options: --commit-on-errors Commit even when some records failed...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/0.5.1-writing_data.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Writing Hudi Tables",
        "excerpt":"In this section, we will cover ways to ingest new changes from external sources or even other Hudi tables using the DeltaStreamer tool, as well as speeding up large Spark jobs via upserts using the Hudi datasource. Such tables can then be queried using various query engines. Write Operations Before...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/0.5.1-writing_data.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "查询 Hudi 数据集",
        "excerpt":"从概念上讲，Hudi物理存储一次数据到DFS上，同时在其上提供三个逻辑视图，如之前所述。 数据集同步到Hive Metastore后，它将提供由Hudi的自定义输入格式支持的Hive外部表。一旦提供了适当的Hudi捆绑包， 就可以通过Hive、Spark和Presto之类的常用查询引擎来查询数据集。 具体来说，在写入过程中传递了两个由table name命名的Hive表。 例如，如果table name = hudi_tbl，我们得到 hudi_tbl 实现了由 HoodieParquetInputFormat 支持的数据集的读优化视图，从而提供了纯列式数据。 hudi_tbl_rt 实现了由 HoodieParquetRealtimeInputFormat 支持的数据集的实时视图，从而提供了基础数据和日志数据的合并视图。 如概念部分所述，增量处理所需要的 一个关键原语是增量拉取（以从数据集中获取更改流/日志）。您可以增量提取Hudi数据集，这意味着自指定的即时时间起， 您可以只获得全部更新和新行。 这与插入更新一起使用，对于构建某些数据管道尤其有用，包括将1个或多个源Hudi表（数据流/事实）以增量方式拉出（流/事实） 并与其他表（数据集/维度）结合以写出增量到目标Hudi数据集。增量视图是通过查询上表之一实现的，并具有特殊配置， 该特殊配置指示查询计划仅需要从数据集中获取增量数据。 接下来，我们将详细讨论在每个查询引擎上如何访问所有三个视图。 Hive 为了使Hive能够识别Hudi数据集并正确查询， HiveServer2需要在其辅助jars路径中提供hudi-hadoop-mr-bundle-x.y.z-SNAPSHOT.jar。 这将确保输入格式类及其依赖项可用于查询计划和执行。 读优化表 除了上述设置之外，对于beeline cli访问，还需要将hive.input.format变量设置为org.apache.hudi.hadoop.HoodieParquetInputFormat输入格式的完全限定路径名。 对于Tez，还需要将hive.tez.input.format设置为org.apache.hadoop.hive.ql.io.HiveInputFormat。 实时表 除了在HiveServer2上安装Hive捆绑jars之外，还需要将其放在整个集群的hadoop/hive安装中，这样查询也可以使用自定义RecordReader。 增量拉取 HiveIncrementalPuller允许通过HiveQL从大型事实/维表中增量提取更改， 结合了Hive（可靠地处理复杂的SQL查询）和增量原语的好处（通过增量拉取而不是完全扫描来加快查询速度）。 该工具使用Hive JDBC运行hive查询并将其结果保存在临时表中，这个表可以被插入更新。 Upsert实用程序（HoodieDeltaStreamer）具有目录结构所需的所有状态，以了解目标表上的提交时间应为多少。 例如：/app/incremental-hql/intermediate/{source_table_name}_temp/{last_commit_included}。 已注册的Delta Hive表的格式为{tmpdb}.{source_table}_{last_commit_included}。 以下是HiveIncrementalPuller的配置选项 配置 描述 默认值 hiveUrl 要连接的Hive...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/0.5.1-querying_data.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Querying Hudi Tables",
        "excerpt":"Conceptually, Hudi stores data physically once on DFS, while providing 3 different ways of querying, as explained before. Once the table is synced to the Hive metastore, it provides external Hive tables backed by Hudi’s custom inputformats. Once the proper hudi bundle has been installed, the table can be queried...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/0.5.1-querying_data.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "配置",
        "excerpt":"该页面介绍了几种配置写入或读取Hudi数据集的作业的方法。 简而言之，您可以在几个级别上控制行为。 Spark数据源配置 : 这些配置控制Hudi Spark数据源，提供如下功能： 定义键和分区、选择写操作、指定如何合并记录或选择要读取的视图类型。 WriteClient 配置 : 在内部，Hudi数据源使用基于RDD的HoodieWriteClient API 真正执行对存储的写入。 这些配置可对文件大小、压缩（compression）、并行度、压缩（compaction）、写入模式、清理等底层方面进行完全控制。 尽管Hudi提供了合理的默认设置，但在不同情形下，可能需要对这些配置进行调整以针对特定的工作负载进行优化。 RecordPayload 配置 : 这是Hudi提供的最底层的定制。 RecordPayload定义了如何根据传入的新记录和存储的旧记录来产生新值以进行插入更新。 Hudi提供了诸如OverwriteWithLatestAvroPayload的默认实现，该实现仅使用最新或最后写入的记录来更新存储。 在数据源和WriteClient级别，都可以将其重写为扩展HoodieRecordPayload类的自定义类。 与云存储连接 无论使用RDD/WriteClient API还是数据源，以下信息都有助于配置对云存储的访问。 AWS S3 S3和Hudi协同工作所需的配置。 Google Cloud Storage GCS和Hudi协同工作所需的配置。 Spark数据源配置 可以通过将以下选项传递到option(k,v)方法中来配置使用数据源的Spark作业。 实际的数据源级别配置在下面列出。 写选项 另外，您可以使用options()或option(k,v)方法直接传递任何WriteClient级别的配置。 inputDF.write() .format(\"org.apache.hudi\") .options(clientOpts) // 任何Hudi客户端选项都可以传入 .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), \"_row_key\") .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), \"partition\") .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), \"timestamp\") .option(HoodieWriteConfig.TABLE_NAME, tableName)...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/0.5.1-configurations.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Configurations",
        "excerpt":"This page covers the different ways of configuring your job to write/read Hudi tables. At a high level, you can control behaviour at few levels. Spark Datasource Configs : These configs control the Hudi Spark Datasource, providing ability to define keys/partitioning, pick out the write operation, specify how to merge...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/0.5.1-configurations.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "性能",
        "excerpt":"在本节中，我们将介绍一些有关Hudi插入更新、增量提取的实际性能数据，并将其与实现这些任务的其它传统工具进行比较。   插入更新   下面显示了从NoSQL数据库摄取获得的速度提升，这些速度提升数据是通过在写入时复制存储上的Hudi数据集上插入更新而获得的， 数据集包括5个从小到大的表（相对于批量加载表）。           由于Hudi可以通过增量构建数据集，它也为更频繁地调度摄取提供了可能性，从而减少了延迟，并显著节省了总体计算成本。           Hudi插入更新在t1表的一次提交中就进行了高达4TB的压力测试。 有关一些调优技巧，请参见这里。   索引   为了有效地插入更新数据，Hudi需要将要写入的批量数据中的记录分类为插入和更新（并标记它所属的文件组）。 为了加快此操作的速度，Hudi采用了可插拔索引机制，该机制存储了recordKey和它所属的文件组ID之间的映射。 默认情况下，Hudi使用内置索引，该索引使用文件范围和布隆过滤器来完成此任务，相比于Spark Join，其速度最高可提高10倍。   当您将recordKey建模为单调递增时（例如时间戳前缀），Hudi提供了最佳的索引性能，从而进行范围过滤来避免与许多文件进行比较。 即使对于基于UUID的键，也有已知技术来达到同样目的。 例如，在具有80B键、3个分区、11416个文件、10TB数据的事件表上使用100M个时间戳前缀的键（5％的更新，95％的插入）时， 相比于原始Spark Join，Hudi索引速度的提升约为7倍（440秒相比于2880秒）。 即使对于具有挑战性的工作负载，如使用300个核对3.25B UUID键、30个分区、6180个文件的“100％更新”的数据库摄取工作负载，Hudi索引也可以提供80-100％的加速。   读优化查询   读优化视图的主要设计目标是在不影响查询的情况下实现上一节中提到的延迟减少和效率提高。 下图比较了对Hudi和非Hudi数据集的Hive、Presto、Spark查询，并对此进行说明。   Hive           Spark           Presto          ","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/0.5.1-performance.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Performance",
        "excerpt":"In this section, we go over some real world performance numbers for Hudi upserts, incremental pull and compare them against the conventional alternatives for achieving these tasks. Upserts Following shows the speed up obtained for NoSQL database ingestion, from incrementally upserting on a Hudi table on the copy-on-write storage, on...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/0.5.1-performance.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "管理 Hudi Pipelines",
        "excerpt":"管理员/运维人员可以通过以下方式了解Hudi数据集/管道 通过Admin CLI进行管理 Graphite指标 Hudi应用程序的Spark UI 本节简要介绍了每一种方法，并提供了有关故障排除的一些常规指南 Admin CLI 一旦构建了hudi，就可以通过cd hudi-cli &amp;&amp; ./hudi-cli.sh启动shell。 一个hudi数据集位于DFS上的basePath位置，我们需要该位置才能连接到Hudi数据集。 Hudi库使用.hoodie子文件夹跟踪所有元数据，从而有效地在内部管理该数据集。 初始化hudi表，可使用如下命令。 18/09/06 15:56:52 INFO annotation.AutowiredAnnotationBeanPostProcessor: JSR-330 'javax.inject.Inject' annotation found and supported for autowiring ============================================ * * * _ _ _ _ * * | | | | | | (_) * * | |__| |...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/0.5.1-deployment.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Deployment Guide",
        "excerpt":"This section provides all the help you need to deploy and operate Hudi tables at scale. Specifically, we will cover the following aspects. Deployment Model : How various Hudi components are deployed and managed. Upgrading Versions : Picking up new releases of Hudi, guidelines and general best-practices. Migrating to Hudi...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/0.5.1-deployment.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Privacy Policy",
        "excerpt":"Information about your use of this website is collected using server access logs and a tracking cookie. The collected information consists of the following: The IP address from which you access the website; The type of browser and operating system you use to access our site; The date and time...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/0.5.1-privacy.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Privacy Policy",
        "excerpt":"Information about your use of this website is collected using server access logs and a tracking cookie. The collected information consists of the following: The IP address from which you access the website; The type of browser and operating system you use to access our site; The date and time...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/0.5.1-privacy.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "文档版本",
        "excerpt":"                                  Latest             英文版             中文版                                      0.5.2             英文版             中文版                                      0.5.1             英文版             中文版                                      0.5.0             英文版             中文版                        ","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/0.5.1-docs-versions.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Docs Versions",
        "excerpt":"                                  Latest             English Version             Chinese Version                                      0.5.2             English Version             Chinese Version                                      0.5.1             English Version             Chinese Version                                      0.5.0             English Version             Chinese Version                       ","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/0.5.1-docs-versions.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "S3 Filesystem",
        "excerpt":"In this page, we explain how to get your Hudi spark job to store into AWS S3. AWS configs There are two configurations required for Hudi-S3 compatibility: Adding AWS Credentials for Hudi Adding required Jars to classpath AWS Credentials Simplest way to use Hudi with S3, is to configure your...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/0.5.2-s3_hoodie.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "S3 Filesystem",
        "excerpt":"In this page, we explain how to get your Hudi spark job to store into AWS S3. AWS configs There are two configurations required for Hudi-S3 compatibility: Adding AWS Credentials for Hudi Adding required Jars to classpath AWS Credentials Simplest way to use Hudi with S3, is to configure your...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/0.5.2-s3_hoodie.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "GCS Filesystem",
        "excerpt":"For Hudi storage on GCS, regional buckets provide an DFS API with strong consistency. GCS Configs There are two configurations required for Hudi GCS compatibility: Adding GCS Credentials for Hudi Adding required jars to classpath GCS Credentials Add the required configs in your core-site.xml from where Hudi can fetch them....","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/0.5.2-gcs_hoodie.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "GCS Filesystem",
        "excerpt":"For Hudi storage on GCS, regional buckets provide an DFS API with strong consistency. GCS Configs There are two configurations required for Hudi GCS compatibility: Adding GCS Credentials for Hudi Adding required jars to classpath GCS Credentials Add the required configs in your core-site.xml from where Hudi can fetch them....","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/0.5.2-gcs_hoodie.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Migration Guide",
        "excerpt":"Hudi maintains metadata such as commit timeline and indexes to manage a dataset. The commit timelines helps to understand the actions happening on a dataset as well as the current state of a dataset. Indexes are used by Hudi to maintain a record key to file id mapping to efficiently...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/0.5.2-migration_guide.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Migration Guide",
        "excerpt":"Hudi maintains metadata such as commit timeline and indexes to manage a table. The commit timelines helps to understand the actions happening on a table as well as the current state of a table. Indexes are used by Hudi to maintain a record key to file id mapping to efficiently...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/0.5.2-migration_guide.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Docker Demo",
        "excerpt":"A Demo using docker containers Lets use a real world example to see how hudi works end to end. For this purpose, a self contained data infrastructure is brought up in a local docker cluster within your computer. The steps have been tested on a Mac laptop Prerequisites Docker Setup...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/0.5.2-docker_demo.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Docker Demo",
        "excerpt":"A Demo using docker containers Lets use a real world example to see how hudi works end to end. For this purpose, a self contained data infrastructure is brought up in a local docker cluster within your computer. The steps have been tested on a Mac laptop Prerequisites Docker Setup...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/0.5.2-docker_demo.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Quick-Start Guide",
        "excerpt":"本指南通过使用spark-shell简要介绍了Hudi功能。使用Spark数据源，我们将通过代码段展示如何插入和更新的Hudi默认存储类型数据集： 写时复制。每次写操作之后，我们还将展示如何读取快照和增量读取数据。 设置spark-shell Hudi适用于Spark-2.x版本。您可以按照此处的说明设置spark。 在提取的目录中，使用spark-shell运行Hudi： bin/spark-shell --packages org.apache.hudi:hudi-spark-bundle:0.5.2-incubating --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' 设置表名、基本路径和数据生成器来为本指南生成记录。 import org.apache.hudi.QuickstartUtils._ import scala.collection.JavaConversions._ import org.apache.spark.sql.SaveMode._ import org.apache.hudi.DataSourceReadOptions._ import org.apache.hudi.DataSourceWriteOptions._ import org.apache.hudi.config.HoodieWriteConfig._ val tableName = \"hudi_cow_table\" val basePath = \"file:///tmp/hudi_cow_table\" val dataGen = new DataGenerator 数据生成器 可以基于行程样本模式 生成插入和更新的样本。 插入数据 生成一些新的行程样本，将其加载到DataFrame中，然后将DataFrame写入Hudi数据集中，如下所示。 val inserts = convertToStringList(dataGen.generateInserts(10)) val df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/0.5.2-quick-start-guide.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Quick-Start Guide",
        "excerpt":"This guide provides a quick peek at Hudi’s capabilities using spark-shell. Using Spark datasources, we will walk through code snippets that allows you to insert and update a Hudi table of default table type: Copy on Write. After each write operation we will also show how to read the data...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/0.5.2-quick-start-guide.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Structure",
        "excerpt":"Hudi (pronounced “Hoodie”) ingests &amp; manages storage of large analytical tables over DFS (HDFS or cloud stores) and provides three types of queries. Read Optimized query - Provides excellent query performance on pure columnar storage, much like plain Parquet tables. Incremental query - Provides a change stream out of the...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/0.5.2-structure.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "使用案例",
        "excerpt":"以下是一些使用Hudi的示例，说明了加快处理速度和提高效率的好处 近实时摄取 将外部源(如事件日志、数据库、外部源)的数据摄取到Hadoop数据湖是一个众所周知的问题。 尽管这些数据对整个组织来说是最有价值的，但不幸的是，在大多数(如果不是全部)Hadoop部署中都使用零散的方式解决，即使用多个不同的摄取工具。 对于RDBMS摄取，Hudi提供 通过更新插入达到更快加载，而不是昂贵且低效的批量加载。例如，您可以读取MySQL BIN日志或Sqoop增量导入并将其应用于 DFS上的等效Hudi表。这比批量合并任务及复杂的手工合并工作流更快/更有效率。 对于NoSQL数据存储，如Cassandra / Voldemort / HBase，即使是中等规模大小也会存储数十亿行。 毫无疑问， 全量加载不可行，如果摄取需要跟上较高的更新量，那么则需要更有效的方法。 即使对于像Kafka这样的不可变数据源，Hudi也可以 强制在HDFS上使用最小文件大小, 这采取了综合方式解决HDFS小文件问题来改善NameNode的健康状况。这对事件流来说更为重要，因为它通常具有较高容量(例如：点击流)，如果管理不当，可能会对Hadoop集群造成严重损害。 在所有源中，通过commits这一概念，Hudi增加了以原子方式向消费者发布新数据的功能，这种功能十分必要。 近实时分析 通常，实时数据集市由专业(实时)数据分析存储提供支持，例如Druid或Memsql或OpenTSDB。 这对于较小规模的数据量来说绝对是完美的(相比于这样安装Hadoop)，这种情况需要在亚秒级响应查询，例如系统监控或交互式实时分析。 但是，由于Hadoop上的数据太陈旧了，通常这些系统会被滥用于非交互式查询，这导致利用率不足和硬件/许可证成本的浪费。 另一方面，Hadoop上的交互式SQL解决方案(如Presto和SparkSQL)表现出色，在 几秒钟内完成查询。 通过将 数据新鲜度提高到几分钟，Hudi可以提供一个更有效的替代方案，并支持存储在DFS中的 数量级更大的数据集 的实时分析。 此外，Hudi没有外部依赖(如专用于实时分析的HBase集群)，因此可以在更新的分析上实现更快的分析，而不会增加操作开销。 增量处理管道 Hadoop提供的一个基本能力是构建一系列数据集，这些数据集通过表示为工作流的DAG相互派生。 工作流通常取决于多个上游工作流输出的新数据，新数据的可用性传统上由新的DFS文件夹/Hive分区指示。 让我们举一个具体的例子来说明这点。上游工作流U可以每小时创建一个Hive分区，在每小时结束时(processing_time)使用该小时的数据(event_time)，提供1小时的有效新鲜度。 然后，下游工作流D在U结束后立即启动，并在下一个小时内自行处理，将有效延迟时间增加到2小时。 上面的示例忽略了迟到的数据，即processing_time和event_time分开时。 不幸的是，在今天的后移动和前物联网世界中，来自间歇性连接的移动设备和传感器的延迟数据是常态，而不是异常。 在这种情况下，保证正确性的唯一补救措施是重新处理最后几个小时的数据， 每小时一遍又一遍，这可能会严重影响整个生态系统的效率。例如; 试想一下，在数百个工作流中每小时重新处理TB数据。 Hudi通过以单个记录为粒度的方式(而不是文件夹/分区)从上游 Hudi数据集HU消费新数据(包括迟到数据)，来解决上面的问题。 应用处理逻辑，并使用下游Hudi数据集HD高效更新/协调迟到数据。在这里，HU和HD可以以更频繁的时间被连续调度 比如15分钟，并且HD提供端到端30分钟的延迟。 为实现这一目标，Hudi采用了类似于Spark Streaming、发布/订阅系统等流处理框架，以及像Kafka 或Oracle XStream等数据库复制技术的类似概念。 如果感兴趣，可以在这里找到有关增量处理(相比于流处理和批处理)好处的更详细解释。 DFS的数据分发...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/0.5.2-use_cases.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Use Cases",
        "excerpt":"Near Real-Time Ingestion Ingesting data from external sources like (event logs, databases, external sources) into a Hadoop Data Lake is a well known problem. In most (if not all) Hadoop deployments, it is unfortunately solved in a piecemeal fashion, using a medley of ingestion tools, even though this data is...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/0.5.2-use_cases.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "演讲 & Hudi 用户",
        "excerpt":"已使用 Uber Hudi最初由Uber开发，用于实现低延迟、高效率的数据库摄取。 Hudi自2016年8月开始在生产环境上线，在Hadoop上驱动约100个非常关键的业务表，支撑约几百TB的数据规模(前10名包括行程、乘客、司机)。 Hudi还支持几个增量的Hive ETL管道，并且目前已集成到Uber的数据分发系统中。 EMIS Health EMIS Health是英国最大的初级保健IT软件提供商，其数据集包括超过5000亿的医疗保健记录。HUDI用于管理生产中的分析数据集，并使其与上游源保持同步。Presto用于查询以HUDI格式写入的数据。 Yields.io Yields.io是第一个使用AI在企业范围内进行自动模型验证和实时监控的金融科技平台。他们的数据湖由Hudi管理，他们还积极使用Hudi为增量式、跨语言/平台机器学习构建基础架构。 Yotpo Hudi在Yotpo有不少用途。首先，在他们的开源ETL框架中集成了Hudi作为CDC管道的输出写入程序，即从数据库binlog生成的事件流到Kafka然后再写入S3。 演讲 &amp; 报告 “Hoodie: Incremental processing on Hadoop at Uber” - By Vinoth Chandar &amp; Prasanna Rajaperumal Mar 2017, Strata + Hadoop World, San Jose, CA “Hoodie: An Open Source Incremental Processing Framework From Uber” -...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/0.5.2-powered_by.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Talks & Powered By",
        "excerpt":"Adoption Uber Apache Hudi was originally developed at Uber, to achieve low latency database ingestion, with high efficiency. It has been in production since Aug 2016, powering the massive 100PB data lake, including highly business critical tables like core trips,riders,partners. It also powers several incremental Hive ETL pipelines and being...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/0.5.2-powered_by.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "对比",
        "excerpt":"Apache Hudi填补了在DFS上处理数据的巨大空白，并可以和这些技术很好地共存。然而， 通过将Hudi与一些相关系统进行对比，来了解Hudi如何适应当前的大数据生态系统，并知晓这些系统在设计中做的不同权衡仍将非常有用。   Kudu   Apache Kudu是一个与Hudi具有相似目标的存储系统，该系统通过对upserts支持来对PB级数据进行实时分析。 一个关键的区别是Kudu还试图充当OLTP工作负载的数据存储，而Hudi并不希望这样做。 因此，Kudu不支持增量拉取(截至2017年初)，而Hudi支持以便进行增量处理。   Kudu与分布式文件系统抽象和HDFS完全不同，它自己的一组存储服务器通过RAFT相互通信。 与之不同的是，Hudi旨在与底层Hadoop兼容的文件系统(HDFS，S3或Ceph)一起使用，并且没有自己的存储服务器群，而是依靠Apache Spark来完成繁重的工作。 因此，Hudi可以像其他Spark作业一样轻松扩展，而Kudu则需要硬件和运营支持，特别是HBase或Vertica等数据存储系统。 到目前为止，我们还没有做任何直接的基准测试来比较Kudu和Hudi(鉴于RTTable正在进行中)。 但是，如果我们要使用CERN， 我们预期Hudi在摄取parquet上有更卓越的性能。   Hive事务   Hive事务/ACID是另一项类似的工作，它试图实现在ORC文件格式之上的存储读取时合并。 可以理解，此功能与Hive以及LLAP之类的其他工作紧密相关。 Hive事务不提供Hudi提供的读取优化存储选项或增量拉取。 在实现选择方面，Hudi充分利用了类似Spark的处理框架的功能，而Hive事务特性则在用户或Hive Metastore启动的Hive任务/查询的下实现。 根据我们的生产经验，与其他方法相比，将Hudi作为库嵌入到现有的Spark管道中要容易得多，并且操作不会太繁琐。 Hudi还设计用于与Presto/Spark等非Hive引擎合作，并计划引入除parquet以外的文件格式。   HBase   尽管HBase最终是OLTP工作负载的键值存储层，但由于与Hadoop的相似性，用户通常倾向于将HBase与分析相关联。 鉴于HBase经过严格的写优化，它支持开箱即用的亚秒级更新，Hive-on-HBase允许用户查询该数据。 但是，就分析工作负载的实际性能而言，Parquet/ORC之类的混合列式存储格式可以轻松击败HBase，因为这些工作负载主要是读取繁重的工作。 Hudi弥补了更快的数据与分析存储格式之间的差距。从运营的角度来看，与管理分析使用的HBase region服务器集群相比，为用户提供可更快给出数据的库更具可扩展性。 最终，HBase不像Hudi这样重点支持提交时间、增量拉取之类的增量处理原语。   流式处理   一个普遍的问题：”Hudi与流处理系统有何关系？”，我们将在这里尝试回答。简而言之，Hudi可以与当今的批处理(写时复制存储)和流处理(读时合并存储)作业集成，以将计算结果存储在Hadoop中。 对于Spark应用程序，这可以通过将Hudi库与Spark/Spark流式DAG直接集成来实现。在非Spark处理系统(例如Flink、Hive)情况下，可以在相应的系统中进行处理，然后通过Kafka主题/DFS中间文件将其发送到Hudi表中。从概念上讲，数据处理 管道仅由三个部分组成：输入，处理，输出，用户最终针对输出运行查询以便使用管道的结果。Hudi可以充当将数据存储在DFS上的输入或输出。Hudi在给定流处理管道上的适用性最终归结为你的查询在Presto/SparkSQL/Hive的适用性。   更高级的用例围绕增量处理的概念展开， 甚至在处理引擎内部也使用Hudi来加速典型的批处理管道。例如：Hudi可用作DAG内的状态存储(类似Flink使用的[rocksDB(https://ci.apache.org/projects/flink/flink-docs-release-1.2/ops/state_backends.html#the-rocksdbstatebackend))。 这是路线图上的一个项目并将最终以Beam Runner的形式呈现。  ","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/0.5.2-comparison.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Comparison",
        "excerpt":"Apache Hudi fills a big void for processing data on top of DFS, and thus mostly co-exists nicely with these technologies. However, it would be useful to understand how Hudi fits into the current big data ecosystem, contrasting it with a few related systems and bring out the different tradeoffs...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/0.5.2-comparison.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "概念",
        "excerpt":"Apache Hudi(发音为“Hudi”)在DFS的数据集上提供以下流原语 插入更新 (如何改变数据集?) 增量拉取 (如何获取变更的数据?) 在本节中，我们将讨论重要的概念和术语，这些概念和术语有助于理解并有效使用这些原语。 时间轴 在它的核心，Hudi维护一条包含在不同的即时时间所有对数据集操作的时间轴，从而提供，从不同时间点出发得到不同的视图下的数据集。Hudi即时包含以下组件 操作类型 : 对数据集执行的操作类型 即时时间 : 即时时间通常是一个时间戳(例如：20190117010349)，该时间戳按操作开始时间的顺序单调增加。 状态 : 即时的状态 Hudi保证在时间轴上执行的操作的原子性和基于即时时间的时间轴一致性。 执行的关键操作包括 COMMITS - 一次提交表示将一组记录原子写入到数据集中。 CLEANS - 删除数据集中不再需要的旧文件版本的后台活动。 DELTA_COMMIT - 增量提交是指将一批记录原子写入到MergeOnRead存储类型的数据集中，其中一些/所有数据都可以只写到增量日志中。 COMPACTION - 协调Hudi中差异数据结构的后台活动，例如：将更新从基于行的日志文件变成列格式。在内部，压缩表现为时间轴上的特殊提交。 ROLLBACK - 表示提交/增量提交不成功且已回滚，删除在写入过程中产生的所有部分文件。 SAVEPOINT - 将某些文件组标记为”已保存”，以便清理程序不会将其删除。在发生灾难/数据恢复的情况下，它有助于将数据集还原到时间轴上的某个点。 任何给定的即时都可以处于以下状态之一 REQUESTED - 表示已调度但尚未启动的操作。 INFLIGHT - 表示当前正在执行该操作。 COMPLETED - 表示在时间轴上完成了该操作。 上面的示例显示了在Hudi数据集上大约10:00到10:20之间发生的更新事件，大约每5分钟一次，将提交元数据以及其他后台清理/压缩保留在Hudi时间轴上。 观察的关键点是：提交时间指示数据的到达时间（上午10:20），而实际数据组织则反映了实际时间或事件时间，即数据所反映的（从07:00开始的每小时时段）。在权衡数据延迟和完整性时，这是两个关键概念。...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/0.5.2-concepts.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Concepts",
        "excerpt":"Apache Hudi (pronounced “Hudi”) provides the following streaming primitives over hadoop compatible storages Update/Delete Records (how do I change records in a table?) Change Streams (how do I fetch records that changed?) In this section, we will discuss key concepts &amp; terminologies that are important to understand, to be able...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/0.5.2-concepts.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "写入 Hudi 数据集",
        "excerpt":"这一节我们将介绍使用DeltaStreamer工具从外部源甚至其他Hudi数据集摄取新更改的方法， 以及通过使用Hudi数据源的upserts加快大型Spark作业的方法。 对于此类数据集，我们可以使用各种查询引擎查询它们。 写操作 在此之前，了解Hudi数据源及delta streamer工具提供的三种不同的写操作以及如何最佳利用它们可能会有所帮助。 这些操作可以在针对数据集发出的每个提交/增量提交中进行选择/更改。 UPSERT（插入更新） ：这是默认操作，在该操作中，通过查找索引，首先将输入记录标记为插入或更新。 在运行启发式方法以确定如何最好地将这些记录放到存储上，如优化文件大小之类后，这些记录最终会被写入。 对于诸如数据库更改捕获之类的用例，建议该操作，因为输入几乎肯定包含更新。 INSERT（插入） ：就使用启发式方法确定文件大小而言，此操作与插入更新（UPSERT）非常相似，但此操作完全跳过了索引查找步骤。 因此，对于日志重复数据删除等用例（结合下面提到的过滤重复项的选项），它可以比插入更新快得多。 插入也适用于这种用例，这种情况数据集可以允许重复项，但只需要Hudi的事务写/增量提取/存储管理功能。 BULK_INSERT（批插入） ：插入更新和插入操作都将输入记录保存在内存中，以加快存储优化启发式计算的速度（以及其它未提及的方面）。 所以对Hudi数据集进行初始加载/引导时这两种操作会很低效。批量插入提供与插入相同的语义，但同时实现了基于排序的数据写入算法， 该算法可以很好地扩展数百TB的初始负载。但是，相比于插入和插入更新能保证文件大小，批插入在调整文件大小上只能尽力而为。 DeltaStreamer HoodieDeltaStreamer实用工具 (hudi-utilities-bundle中的一部分) 提供了从DFS或Kafka等不同来源进行摄取的方式，并具有以下功能。 从Kafka单次摄取新事件，从Sqoop、HiveIncrementalPuller输出或DFS文件夹中的多个文件 增量导入 支持json、avro或自定义记录类型的传入数据 管理检查点，回滚和恢复 利用DFS或Confluent schema注册表的Avro模式。 支持自定义转换操作 命令行选项更详细地描述了这些功能： [hoodie]$ spark-submit --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer `ls packaging/hudi-utilities-bundle/target/hudi-utilities-bundle-*.jar` --help Usage: &lt;main class&gt; [options] Options: --commit-on-errors Commit even when some records failed...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/0.5.2-writing_data.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Writing Hudi Tables",
        "excerpt":"In this section, we will cover ways to ingest new changes from external sources or even other Hudi tables using the DeltaStreamer tool, as well as speeding up large Spark jobs via upserts using the Hudi datasource. Such tables can then be queried using various query engines. Write Operations Before...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/0.5.2-writing_data.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "查询 Hudi 数据集",
        "excerpt":"从概念上讲，Hudi物理存储一次数据到DFS上，同时在其上提供三个逻辑视图，如之前所述。 数据集同步到Hive Metastore后，它将提供由Hudi的自定义输入格式支持的Hive外部表。一旦提供了适当的Hudi捆绑包， 就可以通过Hive、Spark和Presto之类的常用查询引擎来查询数据集。 具体来说，在写入过程中传递了两个由table name命名的Hive表。 例如，如果table name = hudi_tbl，我们得到 hudi_tbl 实现了由 HoodieParquetInputFormat 支持的数据集的读优化视图，从而提供了纯列式数据。 hudi_tbl_rt 实现了由 HoodieParquetRealtimeInputFormat 支持的数据集的实时视图，从而提供了基础数据和日志数据的合并视图。 如概念部分所述，增量处理所需要的 一个关键原语是增量拉取（以从数据集中获取更改流/日志）。您可以增量提取Hudi数据集，这意味着自指定的即时时间起， 您可以只获得全部更新和新行。 这与插入更新一起使用，对于构建某些数据管道尤其有用，包括将1个或多个源Hudi表（数据流/事实）以增量方式拉出（流/事实） 并与其他表（数据集/维度）结合以写出增量到目标Hudi数据集。增量视图是通过查询上表之一实现的，并具有特殊配置， 该特殊配置指示查询计划仅需要从数据集中获取增量数据。 查询引擎支持列表 下面的表格展示了各查询引擎是否支持Hudi格式 读优化表 查询引擎 实时视图 增量拉取 Hive Y Y Spark SQL Y Y Spark Datasource Y Y Presto Y N Impala Y N 实时表 查询引擎 实时视图...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/0.5.2-querying_data.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Querying Hudi Tables",
        "excerpt":"Conceptually, Hudi stores data physically once on DFS, while providing 3 different ways of querying, as explained before. Once the table is synced to the Hive metastore, it provides external Hive tables backed by Hudi’s custom inputformats. Once the proper hudi bundle has been installed, the table can be queried...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/0.5.2-querying_data.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "配置",
        "excerpt":"该页面介绍了几种配置写入或读取Hudi数据集的作业的方法。 简而言之，您可以在几个级别上控制行为。 Spark数据源配置 : 这些配置控制Hudi Spark数据源，提供如下功能： 定义键和分区、选择写操作、指定如何合并记录或选择要读取的视图类型。 WriteClient 配置 : 在内部，Hudi数据源使用基于RDD的HoodieWriteClient API 真正执行对存储的写入。 这些配置可对文件大小、压缩（compression）、并行度、压缩（compaction）、写入模式、清理等底层方面进行完全控制。 尽管Hudi提供了合理的默认设置，但在不同情形下，可能需要对这些配置进行调整以针对特定的工作负载进行优化。 RecordPayload 配置 : 这是Hudi提供的最底层的定制。 RecordPayload定义了如何根据传入的新记录和存储的旧记录来产生新值以进行插入更新。 Hudi提供了诸如OverwriteWithLatestAvroPayload的默认实现，该实现仅使用最新或最后写入的记录来更新存储。 在数据源和WriteClient级别，都可以将其重写为扩展HoodieRecordPayload类的自定义类。 与云存储连接 无论使用RDD/WriteClient API还是数据源，以下信息都有助于配置对云存储的访问。 AWS S3 S3和Hudi协同工作所需的配置。 Google Cloud Storage GCS和Hudi协同工作所需的配置。 Spark数据源配置 可以通过将以下选项传递到option(k,v)方法中来配置使用数据源的Spark作业。 实际的数据源级别配置在下面列出。 写选项 另外，您可以使用options()或option(k,v)方法直接传递任何WriteClient级别的配置。 inputDF.write() .format(\"org.apache.hudi\") .options(clientOpts) // 任何Hudi客户端选项都可以传入 .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), \"_row_key\") .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), \"partition\") .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), \"timestamp\") .option(HoodieWriteConfig.TABLE_NAME, tableName)...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/0.5.2-configurations.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Configurations",
        "excerpt":"This page covers the different ways of configuring your job to write/read Hudi tables. At a high level, you can control behaviour at few levels. Spark Datasource Configs : These configs control the Hudi Spark Datasource, providing ability to define keys/partitioning, pick out the write operation, specify how to merge...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/0.5.2-configurations.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "性能",
        "excerpt":"在本节中，我们将介绍一些有关Hudi插入更新、增量提取的实际性能数据，并将其与实现这些任务的其它传统工具进行比较。   插入更新   下面显示了从NoSQL数据库摄取获得的速度提升，这些速度提升数据是通过在写入时复制存储上的Hudi数据集上插入更新而获得的， 数据集包括5个从小到大的表（相对于批量加载表）。           由于Hudi可以通过增量构建数据集，它也为更频繁地调度摄取提供了可能性，从而减少了延迟，并显著节省了总体计算成本。           Hudi插入更新在t1表的一次提交中就进行了高达4TB的压力测试。 有关一些调优技巧，请参见这里。   索引   为了有效地插入更新数据，Hudi需要将要写入的批量数据中的记录分类为插入和更新（并标记它所属的文件组）。 为了加快此操作的速度，Hudi采用了可插拔索引机制，该机制存储了recordKey和它所属的文件组ID之间的映射。 默认情况下，Hudi使用内置索引，该索引使用文件范围和布隆过滤器来完成此任务，相比于Spark Join，其速度最高可提高10倍。   当您将recordKey建模为单调递增时（例如时间戳前缀），Hudi提供了最佳的索引性能，从而进行范围过滤来避免与许多文件进行比较。 即使对于基于UUID的键，也有已知技术来达到同样目的。 例如，在具有80B键、3个分区、11416个文件、10TB数据的事件表上使用100M个时间戳前缀的键（5％的更新，95％的插入）时， 相比于原始Spark Join，Hudi索引速度的提升约为7倍（440秒相比于2880秒）。 即使对于具有挑战性的工作负载，如使用300个核对3.25B UUID键、30个分区、6180个文件的“100％更新”的数据库摄取工作负载，Hudi索引也可以提供80-100％的加速。   读优化查询   读优化视图的主要设计目标是在不影响查询的情况下实现上一节中提到的延迟减少和效率提高。 下图比较了对Hudi和非Hudi数据集的Hive、Presto、Spark查询，并对此进行说明。   Hive           Spark           Presto          ","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/0.5.2-performance.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Performance",
        "excerpt":"In this section, we go over some real world performance numbers for Hudi upserts, incremental pull and compare them against the conventional alternatives for achieving these tasks. Upserts Following shows the speed up obtained for NoSQL database ingestion, from incrementally upserting on a Hudi table on the copy-on-write storage, on...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/0.5.2-performance.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "管理 Hudi Pipelines",
        "excerpt":"管理员/运维人员可以通过以下方式了解Hudi数据集/管道 通过Admin CLI进行管理 Graphite指标 Hudi应用程序的Spark UI 本节简要介绍了每一种方法，并提供了有关故障排除的一些常规指南 Admin CLI 一旦构建了hudi，就可以通过cd hudi-cli &amp;&amp; ./hudi-cli.sh启动shell。 一个hudi数据集位于DFS上的basePath位置，我们需要该位置才能连接到Hudi数据集。 Hudi库使用.hoodie子文件夹跟踪所有元数据，从而有效地在内部管理该数据集。 初始化hudi表，可使用如下命令。 18/09/06 15:56:52 INFO annotation.AutowiredAnnotationBeanPostProcessor: JSR-330 'javax.inject.Inject' annotation found and supported for autowiring ============================================ * * * _ _ _ _ * * | | | | | | (_) * * | |__| |...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/0.5.2-deployment.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Deployment Guide",
        "excerpt":"This section provides all the help you need to deploy and operate Hudi tables at scale. Specifically, we will cover the following aspects. Deployment Model : How various Hudi components are deployed and managed. Upgrading Versions : Picking up new releases of Hudi, guidelines and general best-practices. Migrating to Hudi...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/0.5.2-deployment.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Privacy Policy",
        "excerpt":"Information about your use of this website is collected using server access logs and a tracking cookie. The collected information consists of the following: The IP address from which you access the website; The type of browser and operating system you use to access our site; The date and time...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/0.5.2-privacy.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Privacy Policy",
        "excerpt":"Information about your use of this website is collected using server access logs and a tracking cookie. The collected information consists of the following: The IP address from which you access the website; The type of browser and operating system you use to access our site; The date and time...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/0.5.2-privacy.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "文档版本",
        "excerpt":"                                  Latest             英文版             中文版                                      0.5.2             英文版             中文版                                      0.5.1             英文版             中文版                                      0.5.0             英文版             中文版                        ","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/0.5.2-docs-versions.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Docs Versions",
        "excerpt":"                                  Latest             English Version             Chinese Version                                      0.5.2             English Version             Chinese Version                                      0.5.1             English Version             Chinese Version                                      0.5.0             English Version             Chinese Version                       ","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/0.5.2-docs-versions.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "S3 Filesystem",
        "excerpt":"In this page, we explain how to get your Hudi spark job to store into AWS S3. AWS configs There are two configurations required for Hudi-S3 compatibility: Adding AWS Credentials for Hudi Adding required Jars to classpath AWS Credentials Simplest way to use Hudi with S3, is to configure your...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/s3_hoodie.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "S3 Filesystem",
        "excerpt":"In this page, we explain how to get your Hudi spark job to store into AWS S3. AWS configs There are two configurations required for Hudi-S3 compatibility: Adding AWS Credentials for Hudi Adding required Jars to classpath AWS Credentials Simplest way to use Hudi with S3, is to configure your...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/s3_hoodie.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "GCS Filesystem",
        "excerpt":"For Hudi storage on GCS, regional buckets provide an DFS API with strong consistency. GCS Configs There are two configurations required for Hudi GCS compatibility: Adding GCS Credentials for Hudi Adding required jars to classpath GCS Credentials Add the required configs in your core-site.xml from where Hudi can fetch them....","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/gcs_hoodie.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "GCS Filesystem",
        "excerpt":"For Hudi storage on GCS, regional buckets provide an DFS API with strong consistency. GCS Configs There are two configurations required for Hudi GCS compatibility: Adding GCS Credentials for Hudi Adding required jars to classpath GCS Credentials Add the required configs in your core-site.xml from where Hudi can fetch them....","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/gcs_hoodie.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Migration Guide",
        "excerpt":"Hudi maintains metadata such as commit timeline and indexes to manage a dataset. The commit timelines helps to understand the actions happening on a dataset as well as the current state of a dataset. Indexes are used by Hudi to maintain a record key to file id mapping to efficiently...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/migration_guide.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Migration Guide",
        "excerpt":"Hudi maintains metadata such as commit timeline and indexes to manage a table. The commit timelines helps to understand the actions happening on a table as well as the current state of a table. Indexes are used by Hudi to maintain a record key to file id mapping to efficiently...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/migration_guide.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Docker Demo",
        "excerpt":"A Demo using docker containers Lets use a real world example to see how hudi works end to end. For this purpose, a self contained data infrastructure is brought up in a local docker cluster within your computer. The steps have been tested on a Mac laptop Prerequisites Docker Setup...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/docker_demo.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Docker Demo",
        "excerpt":"A Demo using docker containers Lets use a real world example to see how hudi works end to end. For this purpose, a self contained data infrastructure is brought up in a local docker cluster within your computer. The steps have been tested on a Mac laptop Prerequisites Docker Setup...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/docker_demo.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "OSS Filesystem",
        "excerpt":"这个页面描述了如何让你的Hudi spark任务使用Aliyun OSS存储。 Aliyun OSS 部署 为了让Hudi使用OSS，需要增加两部分的配置: 为Hidi增加Aliyun OSS的相关配置 增加Jar包的MVN依赖 Aliyun OSS 相关的配置 新增下面的配置到你的Hudi能访问的core-site.xml文件。使用你的OSS bucket name替换掉fs.defaultFS，使用OSS endpoint地址替换fs.oss.endpoint，使用OSS的key和secret分别替换fs.oss.accessKeyId和fs.oss.accessKeySecret。主要Hudi就能读写相应的bucket。 &lt;property&gt; &lt;name&gt;fs.defaultFS&lt;/name&gt; &lt;value&gt;oss://bucketname/&lt;/value&gt; &lt;/property&gt; &lt;property&gt; &lt;name&gt;fs.oss.endpoint&lt;/name&gt; &lt;value&gt;oss-endpoint-address&lt;/value&gt; &lt;description&gt;Aliyun OSS endpoint to connect to.&lt;/description&gt; &lt;/property&gt; &lt;property&gt; &lt;name&gt;fs.oss.accessKeyId&lt;/name&gt; &lt;value&gt;oss_key&lt;/value&gt; &lt;description&gt;Aliyun access key ID&lt;/description&gt; &lt;/property&gt; &lt;property&gt; &lt;name&gt;fs.oss.accessKeySecret&lt;/name&gt; &lt;value&gt;oss-secret&lt;/value&gt; &lt;description&gt;Aliyun access key secret&lt;/description&gt; &lt;/property&gt; &lt;property&gt; &lt;name&gt;fs.oss.impl&lt;/name&gt; &lt;value&gt;org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem&lt;/value&gt;...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/oss_hoodie.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "OSS Filesystem",
        "excerpt":"In this page, we explain how to get your Hudi spark job to store into Aliyun OSS. Aliyun OSS configs There are two configurations required for Hudi-OSS compatibility: Adding Aliyun OSS Credentials for Hudi Adding required Jars to classpath Aliyun OSS Credentials Add the required configs in your core-site.xml from...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/oss_hoodie.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Quick-Start Guide",
        "excerpt":"本指南通过使用spark-shell简要介绍了Hudi功能。使用Spark数据源，我们将通过代码段展示如何插入和更新Hudi的默认存储类型数据集： 写时复制。每次写操作之后，我们还将展示如何读取快照和增量数据。 设置spark-shell Hudi适用于Spark-2.x版本。您可以按照此处的说明设置spark。 在提取的目录中，使用spark-shell运行Hudi： bin/spark-shell --packages org.apache.hudi:hudi-spark-bundle:0.5.0-incubating --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' 设置表名、基本路径和数据生成器来为本指南生成记录。 import org.apache.hudi.QuickstartUtils._ import scala.collection.JavaConversions._ import org.apache.spark.sql.SaveMode._ import org.apache.hudi.DataSourceReadOptions._ import org.apache.hudi.DataSourceWriteOptions._ import org.apache.hudi.config.HoodieWriteConfig._ val tableName = \"hudi_cow_table\" val basePath = \"file:///tmp/hudi_cow_table\" val dataGen = new DataGenerator 数据生成器 可以基于行程样本模式 生成插入和更新的样本。 插入数据 生成一些新的行程样本，将其加载到DataFrame中，然后将DataFrame写入Hudi数据集中，如下所示。 val inserts = convertToStringList(dataGen.generateInserts(10)) val df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/quick-start-guide.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Quick-Start Guide",
        "excerpt":"This guide provides a quick peek at Hudi’s capabilities using spark-shell. Using Spark datasources, we will walk through code snippets that allows you to insert and update a Hudi table of default table type: Copy on Write. After each write operation we will also show how to read the data...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/quick-start-guide.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Structure",
        "excerpt":"Hudi (pronounced “Hoodie”) ingests &amp; manages storage of large analytical tables over DFS (HDFS or cloud stores) and provides three types of queries. Read Optimized query - Provides excellent query performance on pure columnar storage, much like plain Parquet tables. Incremental query - Provides a change stream out of the...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/structure.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "使用案例",
        "excerpt":"以下是一些使用Hudi的示例，说明了加快处理速度和提高效率的好处 近实时摄取 将外部源(如事件日志、数据库、外部源)的数据摄取到Hadoop数据湖是一个众所周知的问题。 尽管这些数据对整个组织来说是最有价值的，但不幸的是，在大多数(如果不是全部)Hadoop部署中都使用零散的方式解决，即使用多个不同的摄取工具。 对于RDBMS摄取，Hudi提供 通过更新插入达到更快加载，而不是昂贵且低效的批量加载。例如，您可以读取MySQL BIN日志或Sqoop增量导入并将其应用于 DFS上的等效Hudi表。这比批量合并任务及复杂的手工合并工作流更快/更有效率。 对于NoSQL数据存储，如Cassandra / Voldemort / HBase，即使是中等规模大小也会存储数十亿行。 毫无疑问， 全量加载不可行，如果摄取需要跟上较高的更新量，那么则需要更有效的方法。 即使对于像Kafka这样的不可变数据源，Hudi也可以 强制在HDFS上使用最小文件大小, 这采取了综合方式解决HDFS小文件问题来改善NameNode的健康状况。这对事件流来说更为重要，因为它通常具有较高容量(例如：点击流)，如果管理不当，可能会对Hadoop集群造成严重损害。 在所有源中，通过commits这一概念，Hudi增加了以原子方式向消费者发布新数据的功能，这种功能十分必要。 近实时分析 通常，实时数据集市由专业(实时)数据分析存储提供支持，例如Druid或Memsql或OpenTSDB。 这对于较小规模的数据量来说绝对是完美的(相比于这样安装Hadoop)，这种情况需要在亚秒级响应查询，例如系统监控或交互式实时分析。 但是，由于Hadoop上的数据太陈旧了，通常这些系统会被滥用于非交互式查询，这导致利用率不足和硬件/许可证成本的浪费。 另一方面，Hadoop上的交互式SQL解决方案(如Presto和SparkSQL)表现出色，在 几秒钟内完成查询。 通过将 数据新鲜度提高到几分钟，Hudi可以提供一个更有效的替代方案，并支持存储在DFS中的 数量级更大的数据集 的实时分析。 此外，Hudi没有外部依赖(如专用于实时分析的HBase集群)，因此可以在更新的分析上实现更快的分析，而不会增加操作开销。 增量处理管道 Hadoop提供的一个基本能力是构建一系列数据集，这些数据集通过表示为工作流的DAG相互派生。 工作流通常取决于多个上游工作流输出的新数据，新数据的可用性传统上由新的DFS文件夹/Hive分区指示。 让我们举一个具体的例子来说明这点。上游工作流U可以每小时创建一个Hive分区，在每小时结束时(processing_time)使用该小时的数据(event_time)，提供1小时的有效新鲜度。 然后，下游工作流D在U结束后立即启动，并在下一个小时内自行处理，将有效延迟时间增加到2小时。 上面的示例忽略了迟到的数据，即processing_time和event_time分开时。 不幸的是，在今天的后移动和前物联网世界中，来自间歇性连接的移动设备和传感器的延迟数据是常态，而不是异常。 在这种情况下，保证正确性的唯一补救措施是重新处理最后几个小时的数据， 每小时一遍又一遍，这可能会严重影响整个生态系统的效率。例如; 试想一下，在数百个工作流中每小时重新处理TB数据。 Hudi通过以单个记录为粒度的方式(而不是文件夹/分区)从上游 Hudi数据集HU消费新数据(包括迟到数据)，来解决上面的问题。 应用处理逻辑，并使用下游Hudi数据集HD高效更新/协调迟到数据。在这里，HU和HD可以以更频繁的时间被连续调度 比如15分钟，并且HD提供端到端30分钟的延迟。 为实现这一目标，Hudi采用了类似于Spark Streaming、发布/订阅系统等流处理框架，以及像Kafka 或Oracle XStream等数据库复制技术的类似概念。 如果感兴趣，可以在这里找到有关增量处理(相比于流处理和批处理)好处的更详细解释。 DFS的数据分发...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/use_cases.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Use Cases",
        "excerpt":"Near Real-Time Ingestion Ingesting data from external sources like (event logs, databases, external sources) into a Hadoop Data Lake is a well known problem. In most (if not all) Hadoop deployments, it is unfortunately solved in a piecemeal fashion, using a medley of ingestion tools, even though this data is...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/use_cases.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "演讲 & Hudi 用户",
        "excerpt":"已使用 Uber Hudi最初由Uber开发，用于实现低延迟、高效率的数据库摄取。 Hudi自2016年8月开始在生产环境上线，在Hadoop上驱动约100个非常关键的业务表，支撑约几百TB的数据规模(前10名包括行程、乘客、司机)。 Hudi还支持几个增量的Hive ETL管道，并且目前已集成到Uber的数据分发系统中。 EMIS Health EMIS Health是英国最大的初级保健IT软件提供商，其数据集包括超过5000亿的医疗保健记录。HUDI用于管理生产中的分析数据集，并使其与上游源保持同步。Presto用于查询以HUDI格式写入的数据。 Yields.io Yields.io是第一个使用AI在企业范围内进行自动模型验证和实时监控的金融科技平台。他们的数据湖由Hudi管理，他们还积极使用Hudi为增量式、跨语言/平台机器学习构建基础架构。 Yotpo Hudi在Yotpo有不少用途。首先，在他们的开源ETL框架中集成了Hudi作为CDC管道的输出写入程序，即从数据库binlog生成的事件流到Kafka然后再写入S3。 演讲 &amp; 报告 “Hoodie: Incremental processing on Hadoop at Uber” - By Vinoth Chandar &amp; Prasanna Rajaperumal Mar 2017, Strata + Hadoop World, San Jose, CA “Hoodie: An Open Source Incremental Processing Framework From Uber” -...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/powered_by.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Talks & Powered By",
        "excerpt":"Adoption Uber Apache Hudi was originally developed at Uber, to achieve low latency database ingestion, with high efficiency. It has been in production since Aug 2016, powering the massive 100PB data lake, including highly business critical tables like core trips,riders,partners. It also powers several incremental Hive ETL pipelines and being...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/powered_by.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "对比",
        "excerpt":"Apache Hudi填补了在DFS上处理数据的巨大空白，并可以和这些技术很好地共存。然而， 通过将Hudi与一些相关系统进行对比，来了解Hudi如何适应当前的大数据生态系统，并知晓这些系统在设计中做的不同权衡仍将非常有用。   Kudu   Apache Kudu是一个与Hudi具有相似目标的存储系统，该系统通过对upserts支持来对PB级数据进行实时分析。 一个关键的区别是Kudu还试图充当OLTP工作负载的数据存储，而Hudi并不希望这样做。 因此，Kudu不支持增量拉取(截至2017年初)，而Hudi支持以便进行增量处理。   Kudu与分布式文件系统抽象和HDFS完全不同，它自己的一组存储服务器通过RAFT相互通信。 与之不同的是，Hudi旨在与底层Hadoop兼容的文件系统(HDFS，S3或Ceph)一起使用，并且没有自己的存储服务器群，而是依靠Apache Spark来完成繁重的工作。 因此，Hudi可以像其他Spark作业一样轻松扩展，而Kudu则需要硬件和运营支持，特别是HBase或Vertica等数据存储系统。 到目前为止，我们还没有做任何直接的基准测试来比较Kudu和Hudi(鉴于RTTable正在进行中)。 但是，如果我们要使用CERN， 我们预期Hudi在摄取parquet上有更卓越的性能。   Hive事务   Hive事务/ACID是另一项类似的工作，它试图实现在ORC文件格式之上的存储读取时合并。 可以理解，此功能与Hive以及LLAP之类的其他工作紧密相关。 Hive事务不提供Hudi提供的读取优化存储选项或增量拉取。 在实现选择方面，Hudi充分利用了类似Spark的处理框架的功能，而Hive事务特性则在用户或Hive Metastore启动的Hive任务/查询的下实现。 根据我们的生产经验，与其他方法相比，将Hudi作为库嵌入到现有的Spark管道中要容易得多，并且操作不会太繁琐。 Hudi还设计用于与Presto/Spark等非Hive引擎合作，并计划引入除parquet以外的文件格式。   HBase   尽管HBase最终是OLTP工作负载的键值存储层，但由于与Hadoop的相似性，用户通常倾向于将HBase与分析相关联。 鉴于HBase经过严格的写优化，它支持开箱即用的亚秒级更新，Hive-on-HBase允许用户查询该数据。 但是，就分析工作负载的实际性能而言，Parquet/ORC之类的混合列式存储格式可以轻松击败HBase，因为这些工作负载主要是读取繁重的工作。 Hudi弥补了更快的数据与分析存储格式之间的差距。从运营的角度来看，与管理分析使用的HBase region服务器集群相比，为用户提供可更快给出数据的库更具可扩展性。 最终，HBase不像Hudi这样重点支持提交时间、增量拉取之类的增量处理原语。   流式处理   一个普遍的问题：”Hudi与流处理系统有何关系？”，我们将在这里尝试回答。简而言之，Hudi可以与当今的批处理(写时复制存储)和流处理(读时合并存储)作业集成，以将计算结果存储在Hadoop中。 对于Spark应用程序，这可以通过将Hudi库与Spark/Spark流式DAG直接集成来实现。在非Spark处理系统(例如Flink、Hive)情况下，可以在相应的系统中进行处理，然后通过Kafka主题/DFS中间文件将其发送到Hudi表中。从概念上讲，数据处理 管道仅由三个部分组成：输入，处理，输出，用户最终针对输出运行查询以便使用管道的结果。Hudi可以充当将数据存储在DFS上的输入或输出。Hudi在给定流处理管道上的适用性最终归结为你的查询在Presto/SparkSQL/Hive的适用性。   更高级的用例围绕增量处理的概念展开， 甚至在处理引擎内部也使用Hudi来加速典型的批处理管道。例如：Hudi可用作DAG内的状态存储(类似Flink使用的[rocksDB(https://ci.apache.org/projects/flink/flink-docs-release-1.2/ops/state_backends.html#the-rocksdbstatebackend))。 这是路线图上的一个项目并将最终以Beam Runner的形式呈现。  ","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/comparison.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Comparison",
        "excerpt":"Apache Hudi fills a big void for processing data on top of DFS, and thus mostly co-exists nicely with these technologies. However, it would be useful to understand how Hudi fits into the current big data ecosystem, contrasting it with a few related systems and bring out the different tradeoffs...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/comparison.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "概念",
        "excerpt":"Apache Hudi(发音为“Hudi”)在DFS的数据集上提供以下流原语 插入更新 (如何改变数据集?) 增量拉取 (如何获取变更的数据?) 在本节中，我们将讨论重要的概念和术语，这些概念和术语有助于理解并有效使用这些原语。 时间轴 在它的核心，Hudi维护一条包含在不同的即时时间所有对数据集操作的时间轴，从而提供，从不同时间点出发得到不同的视图下的数据集。Hudi即时包含以下组件 操作类型 : 对数据集执行的操作类型 即时时间 : 即时时间通常是一个时间戳(例如：20190117010349)，该时间戳按操作开始时间的顺序单调增加。 状态 : 即时的状态 Hudi保证在时间轴上执行的操作的原子性和基于即时时间的时间轴一致性。 执行的关键操作包括 COMMITS - 一次提交表示将一组记录原子写入到数据集中。 CLEANS - 删除数据集中不再需要的旧文件版本的后台活动。 DELTA_COMMIT - 增量提交是指将一批记录原子写入到MergeOnRead存储类型的数据集中，其中一些/所有数据都可以只写到增量日志中。 COMPACTION - 协调Hudi中差异数据结构的后台活动，例如：将更新从基于行的日志文件变成列格式。在内部，压缩表现为时间轴上的特殊提交。 ROLLBACK - 表示提交/增量提交不成功且已回滚，删除在写入过程中产生的所有部分文件。 SAVEPOINT - 将某些文件组标记为”已保存”，以便清理程序不会将其删除。在发生灾难/数据恢复的情况下，它有助于将数据集还原到时间轴上的某个点。 任何给定的即时都可以处于以下状态之一 REQUESTED - 表示已调度但尚未启动的操作。 INFLIGHT - 表示当前正在执行该操作。 COMPLETED - 表示在时间轴上完成了该操作。 上面的示例显示了在Hudi数据集上大约10:00到10:20之间发生的更新事件，大约每5分钟一次，将提交元数据以及其他后台清理/压缩保留在Hudi时间轴上。 观察的关键点是：提交时间指示数据的到达时间（上午10:20），而实际数据组织则反映了实际时间或事件时间，即数据所反映的（从07:00开始的每小时时段）。在权衡数据延迟和完整性时，这是两个关键概念。...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/concepts.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Concepts",
        "excerpt":"Apache Hudi (pronounced “Hudi”) provides the following streaming primitives over hadoop compatible storages Update/Delete Records (how do I change records in a table?) Change Streams (how do I fetch records that changed?) In this section, we will discuss key concepts &amp; terminologies that are important to understand, to be able...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/concepts.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "写入 Hudi 数据集",
        "excerpt":"这一节我们将介绍使用DeltaStreamer工具从外部源甚至其他Hudi数据集摄取新更改的方法， 以及通过使用Hudi数据源的upserts加快大型Spark作业的方法。 对于此类数据集，我们可以使用各种查询引擎查询它们。 写操作 在此之前，了解Hudi数据源及delta streamer工具提供的三种不同的写操作以及如何最佳利用它们可能会有所帮助。 这些操作可以在针对数据集发出的每个提交/增量提交中进行选择/更改。 UPSERT（插入更新） ：这是默认操作，在该操作中，通过查找索引，首先将输入记录标记为插入或更新。 在运行启发式方法以确定如何最好地将这些记录放到存储上，如优化文件大小之后，这些记录最终会被写入。 对于诸如数据库更改捕获之类的用例，建议该操作，因为输入几乎肯定包含更新。 INSERT（插入） ：就使用启发式方法确定文件大小而言，此操作与插入更新（UPSERT）非常相似，但此操作完全跳过了索引查找步骤。 因此，对于日志重复数据删除等用例（结合下面提到的过滤重复项的选项），它可以比插入更新快得多。 插入也适用于这种用例，这种情况数据集可以允许重复项，但只需要Hudi的事务写/增量提取/存储管理功能。 BULK_INSERT（批插入） ：插入更新和插入操作都将输入记录保存在内存中，以加快存储优化启发式计算的速度（以及其它未提及的方面）。 所以对Hudi数据集进行初始加载/引导时这两种操作会很低效。批量插入提供与插入相同的语义，但同时实现了基于排序的数据写入算法， 该算法可以很好地扩展数百TB的初始负载。但是，相比于插入和插入更新能保证文件大小，批插入在调整文件大小上只能尽力而为。 DeltaStreamer HoodieDeltaStreamer实用工具 (hudi-utilities-bundle中的一部分) 提供了从DFS或Kafka等不同来源进行摄取的方式，并具有以下功能。 从Kafka单次摄取新事件，从Sqoop、HiveIncrementalPuller输出或DFS文件夹中的多个文件 增量导入 支持json、avro或自定义记录类型的传入数据 管理检查点，回滚和恢复 利用DFS或Confluent schema注册表的Avro模式。 支持自定义转换操作 命令行选项更详细地描述了这些功能： [hoodie]$ spark-submit --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer `ls packaging/hudi-utilities-bundle/target/hudi-utilities-bundle-*.jar` --help Usage: &lt;main class&gt; [options] Options: --commit-on-errors Commit even when some records failed...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/writing_data.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Writing Hudi Tables",
        "excerpt":"In this section, we will cover ways to ingest new changes from external sources or even other Hudi tables using the DeltaStreamer tool, as well as speeding up large Spark jobs via upserts using the Hudi datasource. Such tables can then be queried using various query engines. Write Operations Before...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/writing_data.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "查询 Hudi 数据集",
        "excerpt":"从概念上讲，Hudi物理存储一次数据到DFS上，同时在其上提供三个逻辑视图，如之前所述。 数据集同步到Hive Metastore后，它将提供由Hudi的自定义输入格式支持的Hive外部表。一旦提供了适当的Hudi捆绑包， 就可以通过Hive、Spark和Presto之类的常用查询引擎来查询数据集。 具体来说，在写入过程中传递了两个由table name命名的Hive表。 例如，如果table name = hudi_tbl，我们得到 hudi_tbl 实现了由 HoodieParquetInputFormat 支持的数据集的读优化视图，从而提供了纯列式数据。 hudi_tbl_rt 实现了由 HoodieParquetRealtimeInputFormat 支持的数据集的实时视图，从而提供了基础数据和日志数据的合并视图。 如概念部分所述，增量处理所需要的 一个关键原语是增量拉取（以从数据集中获取更改流/日志）。您可以增量提取Hudi数据集，这意味着自指定的即时时间起， 您可以只获得全部更新和新行。 这与插入更新一起使用，对于构建某些数据管道尤其有用，包括将1个或多个源Hudi表（数据流/事实）以增量方式拉出（流/事实） 并与其他表（数据集/维度）结合以写出增量到目标Hudi数据集。增量视图是通过查询上表之一实现的，并具有特殊配置， 该特殊配置指示查询计划仅需要从数据集中获取增量数据。 查询引擎支持列表 下面的表格展示了各查询引擎是否支持Hudi格式 读优化表 查询引擎 实时视图 增量拉取 Hive Y Y Spark SQL Y Y Spark Datasource Y Y Presto Y N Impala Y N 实时表 查询引擎 实时视图...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/querying_data.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Querying Hudi Tables",
        "excerpt":"Conceptually, Hudi stores data physically once on DFS, while providing 3 different ways of querying, as explained before. Once the table is synced to the Hive metastore, it provides external Hive tables backed by Hudi’s custom inputformats. Once the proper hudi bundle has been installed, the table can be queried...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/querying_data.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "配置",
        "excerpt":"该页面介绍了几种配置写入或读取Hudi数据集的作业的方法。 简而言之，您可以在几个级别上控制行为。 Spark数据源配置 : 这些配置控制Hudi Spark数据源，提供如下功能： 定义键和分区、选择写操作、指定如何合并记录或选择要读取的视图类型。 WriteClient 配置 : 在内部，Hudi数据源使用基于RDD的HoodieWriteClient API 真正执行对存储的写入。 这些配置可对文件大小、压缩（compression）、并行度、压缩（compaction）、写入模式、清理等底层方面进行完全控制。 尽管Hudi提供了合理的默认设置，但在不同情形下，可能需要对这些配置进行调整以针对特定的工作负载进行优化。 RecordPayload 配置 : 这是Hudi提供的最底层的定制。 RecordPayload定义了如何根据传入的新记录和存储的旧记录来产生新值以进行插入更新。 Hudi提供了诸如OverwriteWithLatestAvroPayload的默认实现，该实现仅使用最新或最后写入的记录来更新存储。 在数据源和WriteClient级别，都可以将其重写为扩展HoodieRecordPayload类的自定义类。 与云存储连接 无论使用RDD/WriteClient API还是数据源，以下信息都有助于配置对云存储的访问。 AWS S3 S3和Hudi协同工作所需的配置。 Google Cloud Storage GCS和Hudi协同工作所需的配置。 Spark数据源配置 可以通过将以下选项传递到option(k,v)方法中来配置使用数据源的Spark作业。 实际的数据源级别配置在下面列出。 写选项 另外，您可以使用options()或option(k,v)方法直接传递任何WriteClient级别的配置。 inputDF.write() .format(\"org.apache.hudi\") .options(clientOpts) // 任何Hudi客户端选项都可以传入 .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), \"_row_key\") .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), \"partition\") .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), \"timestamp\") .option(HoodieWriteConfig.TABLE_NAME, tableName)...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/configurations.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Configurations",
        "excerpt":"This page covers the different ways of configuring your job to write/read Hudi tables. At a high level, you can control behaviour at few levels. Spark Datasource Configs : These configs control the Hudi Spark Datasource, providing ability to define keys/partitioning, pick out the write operation, specify how to merge...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/configurations.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "性能",
        "excerpt":"在本节中，我们将介绍一些有关Hudi插入更新、增量提取的实际性能数据，并将其与实现这些任务的其它传统工具进行比较。   插入更新   下面显示了从NoSQL数据库摄取获得的速度提升，这些速度提升数据是通过在写入时复制存储上的Hudi数据集上插入更新而获得的， 数据集包括5个从小到大的表（相对于批量加载表）。           由于Hudi可以通过增量构建数据集，它也为更频繁地调度摄取提供了可能性，从而减少了延迟，并显著节省了总体计算成本。           Hudi插入更新在t1表的一次提交中就进行了高达4TB的压力测试。 有关一些调优技巧，请参见这里。   索引   为了有效地插入更新数据，Hudi需要将要写入的批量数据中的记录分类为插入和更新（并标记它所属的文件组）。 为了加快此操作的速度，Hudi采用了可插拔索引机制，该机制存储了recordKey和它所属的文件组ID之间的映射。 默认情况下，Hudi使用内置索引，该索引使用文件范围和布隆过滤器来完成此任务，相比于Spark Join，其速度最高可提高10倍。   当您将recordKey建模为单调递增时（例如时间戳前缀），Hudi提供了最佳的索引性能，从而进行范围过滤来避免与许多文件进行比较。 即使对于基于UUID的键，也有已知技术来达到同样目的。 例如，在具有80B键、3个分区、11416个文件、10TB数据的事件表上使用100M个时间戳前缀的键（5％的更新，95％的插入）时， 相比于原始Spark Join，Hudi索引速度的提升约为7倍（440秒相比于2880秒）。 即使对于具有挑战性的工作负载，如使用300个核对3.25B UUID键、30个分区、6180个文件的“100％更新”的数据库摄取工作负载，Hudi索引也可以提供80-100％的加速。   读优化查询   读优化视图的主要设计目标是在不影响查询的情况下实现上一节中提到的延迟减少和效率提高。 下图比较了对Hudi和非Hudi数据集的Hive、Presto、Spark查询，并对此进行说明。   Hive           Spark           Presto          ","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/performance.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Performance",
        "excerpt":"In this section, we go over some real world performance numbers for Hudi upserts, incremental pull and compare them against the conventional alternatives for achieving these tasks. Upserts Following shows the speed up obtained for NoSQL database ingestion, from incrementally upserting on a Hudi table on the copy-on-write storage, on...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/performance.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "管理 Hudi Pipelines",
        "excerpt":"管理员/运维人员可以通过以下方式了解Hudi数据集/管道 通过Admin CLI进行管理 Graphite指标 Hudi应用程序的Spark UI 本节简要介绍了每一种方法，并提供了有关故障排除的一些常规指南 Admin CLI 一旦构建了hudi，就可以通过cd hudi-cli &amp;&amp; ./hudi-cli.sh启动shell。 一个hudi数据集位于DFS上的basePath位置，我们需要该位置才能连接到Hudi数据集。 Hudi库使用.hoodie子文件夹跟踪所有元数据，从而有效地在内部管理该数据集。 初始化hudi表，可使用如下命令。 18/09/06 15:56:52 INFO annotation.AutowiredAnnotationBeanPostProcessor: JSR-330 'javax.inject.Inject' annotation found and supported for autowiring ============================================ * * * _ _ _ _ * * | | | | | | (_) * * | |__| |...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/deployment.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Deployment Guide",
        "excerpt":"This section provides all the help you need to deploy and operate Hudi tables at scale. Specifically, we will cover the following aspects. Deployment Model : How various Hudi components are deployed and managed. Upgrading Versions : Picking up new releases of Hudi, guidelines and general best-practices. Migrating to Hudi...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/deployment.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Privacy Policy",
        "excerpt":"Information about your use of this website is collected using server access logs and a tracking cookie. The collected information consists of the following: The IP address from which you access the website; The type of browser and operating system you use to access our site; The date and time...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/privacy.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Privacy Policy",
        "excerpt":"Information about your use of this website is collected using server access logs and a tracking cookie. The collected information consists of the following: The IP address from which you access the website; The type of browser and operating system you use to access our site; The date and time...","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/privacy.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "文档版本",
        "excerpt":"                                  Latest             英文版             中文版                                      0.5.2             英文版             中文版                                      0.5.1             英文版             中文版                                      0.5.0             英文版             中文版                        ","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/cn/docs/docs-versions.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Docs Versions",
        "excerpt":"                                  Latest             English Version             Chinese Version                                      0.5.2             English Version             Chinese Version                                      0.5.1             English Version             Chinese Version                                      0.5.0             English Version             Chinese Version                       ","categories": [],
        "tags": [],
        "url": "https://hudi.apache.org/docs/docs-versions.html",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Connect with us at Strata San Jose March 2017",
        "excerpt":"We will be presenting Hudi &amp; general concepts around how incremental processing works at Uber. Catch our talk “Incremental Processing on Hadoop At Uber”   ","categories": ["blog"],
        "tags": [],
        "url": "https://hudi.apache.org/blog/strata-talk-2017/",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Hudi entered Apache Incubator",
        "excerpt":"In the coming weeks, we will be moving in our new home on the Apache Incubator.   ","categories": ["blog"],
        "tags": [],
        "url": "https://hudi.apache.org/blog/asf-incubation/",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Big Batch vs Incremental Processing",
        "excerpt":"   ","categories": ["blog"],
        "tags": [],
        "url": "https://hudi.apache.org/blog/batch-vs-incremental/",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Registering sample dataset to Hive via beeline",
        "excerpt":"Hudi hive sync tool typically handles registration of the dataset into Hive metastore. In case, there are issues with quickstart around this, following page shows commands that can be used to do this manually via beeline. Add in the packaging/hoodie-hive-bundle/target/hoodie-hive-bundle-0.4.6-SNAPSHOT.jar, so that Hive can read the Hudi dataset and answer...","categories": ["blog"],
        "tags": [],
        "url": "https://hudi.apache.org/blog/registering-dataset-to-hive/",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Ingesting Database changes via Sqoop/Hudi",
        "excerpt":"Very simple in just 2 steps. Step 1: Extract new changes to users table in MySQL, as avro data files on DFS // Command to extract incrementals using sqoop bin/sqoop import \\ -Dmapreduce.job.user.classpath.first=true \\ --connect jdbc:mysql://localhost/users \\ --username root \\ --password ******* \\ --table users \\ --as-avrodatafile \\ --target-dir \\...","categories": ["blog"],
        "tags": [],
        "url": "https://hudi.apache.org/blog/ingesting-database-changes/",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Delete support in Hudi",
        "excerpt":"Deletes are supported at a record level in Hudi with 0.5.1 release. This blog is a “how to” blog on how to delete records in hudi. Deletes can be done with 3 flavors: Hudi RDD APIs, with Spark data source and with DeltaStreamer. Delete using RDD Level APIs If you...","categories": ["blog"],
        "tags": [],
        "url": "https://hudi.apache.org/blog/delete-support-in-hudi/",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Change Capture Using AWS Database Migration Service and Hudi",
        "excerpt":"One of the core use-cases for Apache Hudi is enabling seamless, efficient database ingestion to your data lake. Even though a lot has been talked about and even users already adopting this model, content on how to go about this is sparse. In this blog, we will build an end-end...","categories": ["blog"],
        "tags": [],
        "url": "https://hudi.apache.org/blog/change-capture-using-aws/",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Export Hudi datasets as a copy or as different formats",
        "excerpt":"Copy to Hudi dataset Similar to the existing HoodieSnapshotCopier, the Exporter scans the source dataset and then makes a copy of it to the target output path. spark-submit \\ --jars \"packaging/hudi-spark-bundle/target/hudi-spark-bundle_2.11-0.6.0-SNAPSHOT.jar\" \\ --deploy-mode \"client\" \\ --class \"org.apache.hudi.utilities.HoodieSnapshotExporter\" \\ packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_2.11-0.6.0-SNAPSHOT.jar \\ --source-base-path \"/tmp/\" \\ --target-output-path \"/tmp/exported/hudi/\" \\ --output-format \"hudi\" Export to...","categories": ["blog"],
        "tags": [],
        "url": "https://hudi.apache.org/blog/exporting-hudi-datasets/",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},{
        "title": "Apache Hudi (Incubating) Support on Apache Zeppelin",
        "excerpt":"1. Introduction Apache Zeppelin is a web-based notebook that provides interactive data analysis. It is convenient for you to make beautiful documents that can be data-driven, interactive, and collaborative, and supports multiple languages, including Scala (using Apache Spark), Python (Apache Spark), SparkSQL, Hive, Markdown, Shell, and so on. Hive and...","categories": ["blog"],
        "tags": [],
        "url": "https://hudi.apache.org/blog/apache-hudi-apache-zepplin/",
        "teaser":"https://hudi.apache.org/assets/images/500x300.png"},]
