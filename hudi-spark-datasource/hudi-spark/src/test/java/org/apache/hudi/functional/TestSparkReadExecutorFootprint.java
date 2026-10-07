/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.functional;

import org.apache.hudi.DataSourceReadOptions;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.InProcessTimeGenerator;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.fs.RecordingLocalFileSystem;
import org.apache.hudi.hadoop.fs.RecordingLocalFileSystem.Call;
import org.apache.hudi.keygen.SimpleKeyGenerator;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.testutils.SparkExecutorGuards;
import org.apache.hudi.testutils.TaskDeserializationRecorder;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import scala.util.Properties;

import static org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;
import static org.apache.hudi.common.model.HoodieTableType.MERGE_ON_READ;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Guards what Spark tasks do on the executors when reading a Hudi table through the file group
 * reader: they must not access the table's {@code .hoodie} folder, they must not deserialize heavy
 * driver-side objects (meta client, timeline, Hadoop configuration, the file format itself) with
 * every task, what every task deserializes must stay within a size budget, and they must not parse
 * the Hadoop default resources for every file they read. These costs scale with the number of tasks
 * or files, not with the data.
 *
 * <p>Each table is written once per class and read by every case that needs it. The table has
 * several partitions so that a read runs several tasks; on MERGE_ON_READ the second commit
 * updates half of the keys so that every file group has log files to merge. The two commits
 * follow a history of empty commits, so that every read plans against a timeline of tens of
 * instants and per-task state that grows with the timeline shows in the guards.
 */
@Slf4j
@Tag("functional")
class TestSparkReadExecutorFootprint extends SparkClientFunctionalTestHarness {

  private static final int CURRENT_VERSION = HoodieTableVersion.current().versionCode();
  private static final int NUM_RECORDS = 200;
  private static final int NUM_UPDATED_RECORDS = 100;
  private static final int NUM_PARTITIONS = 4;
  private static final int NUM_HISTORY_COMMITS = 48;

  /**
   * Driver-side classes that neither a read task nor the broadcast scan state may deserialize.
   */
  private static final List<String> HEAVY_DRIVER_CLASSES = Arrays.asList(
      "org.apache.hudi.common.table.HoodieTableMetaClient",
      "org.apache.hudi.common.table.timeline.HoodieTimeline",
      "org.apache.hudi.storage.StorageConfiguration",
      "org.apache.spark.util.SerializableConfiguration",
      "org.apache.spark.sql.execution.datasources.parquet.HoodieFileGroupReaderBasedFileFormat",
      "org.apache.hudi.config.HoodieWriteConfig");

  /**
   * Classes that a read task must not deserialize with its closure or partition: the heavy driver-side
   * ones, and the table config and committed instants, which reach the tasks in the broadcast scan
   * state that every executor deserializes once. The instants grow with the timeline.
   */
  private static final List<String> CLASSES_NOT_DESERIALIZED_PER_TASK = Stream.concat(HEAVY_DRIVER_CLASSES.stream(), Stream.of(
      "org.apache.hudi.common.table.timeline.HoodieInstant",
      "org.apache.hudi.common.table.read.CommittedInstants",
      "org.apache.hudi.common.table.HoodieTableConfig")).collect(Collectors.toList());

  /**
   * Hadoop default resource parses allowed in the tasks of a read. A base file read converts the table
   * schema in the scan state with a new configuration once per JVM instance of the state, so once per
   * executor, and local mode runs one executor; a merging read does not parse them.
   */
  private static final int MAX_HADOOP_DEFAULT_RESOURCE_LOADS = 1;

  private static final StructType SCHEMA = DataTypes.createStructType(new StructField[] {
      DataTypes.createStructField("key", DataTypes.StringType, false),
      DataTypes.createStructField("part", DataTypes.StringType, false),
      DataTypes.createStructField("ts", DataTypes.LongType, false),
      DataTypes.createStructField("value", DataTypes.StringType, true)});

  private static final String SCAN_STATE_CLASS = "org.apache.spark.sql.execution.datasources.parquet.HoodieFileGroupReadState";
  private static final String VECTORIZED_READER_ENABLED = "spark.sql.parquet.enableVectorizedReader";
  private static final String DATA_SKIPPING_FAILURE_MODE = "hoodie.fileIndex.dataSkippingFailureMode";

  private static final Map<String, TestTable> TABLES = new HashMap<>();

  @TempDir
  static Path tablesDir;

  enum TableKind {
    PLAIN, CDC, PARQUET_LOG_BLOCKS
  }

  /**
   * The kind of read a task size budget applies to: a read that returns columnar batches of base
   * files measures larger than a read that returns rows.
   */
  enum ReadShape {
    COLUMNAR, ROW
  }

  /**
   * What every task of a read may deserialize, by Spark and Scala binary version: the task binary,
   * the Java-serialized closure, and the largest task stream, which is the larger of the binary and
   * the task with its partition. Any state added to the closure or to a task's partition is paid
   * again by every task, whatever the state is. Spark and Scala versions serialize the plan
   * differently, so each budget is about 1.1x the largest size measured on that version, given as
   * task binary / task stream bytes.
   */
  private static final Map<String, TaskBudgets> TASK_BUDGETS = new HashMap<>();

  static {
    // measured: columnar 15587 / 16679, row 9787 / 12233
    TASK_BUDGETS.put("3.3_2.12", new TaskBudgets(17152, 18432, 11008, 13568));
    // measured: columnar 15968 / 17060, row 10040 / 12367
    TASK_BUDGETS.put("3.4_2.12", new TaskBudgets(17664, 18944, 11264, 13824));
    // measured: columnar 17555 / 18647, row 11051 / 12488
    TASK_BUDGETS.put("3.5_2.12", new TaskBudgets(19456, 20736, 12288, 13824));
    // measured: columnar 17766 / 18989, row 11212 / 12793
    TASK_BUDGETS.put("3.5_2.13", new TaskBudgets(19712, 20992, 12544, 14080));
    // measured: columnar 18524 / 19457, row 12434 / 13618
    TASK_BUDGETS.put("4.0_2.13", new TaskBudgets(20480, 21504, 13824, 15104));
    // measured: columnar 18491 / 19424, row 12401 / 13618
    TASK_BUDGETS.put("4.1_2.13", new TaskBudgets(20480, 21504, 13824, 15104));
    // measured: columnar 18778 / 19712, row 12706 / 13812
    TASK_BUDGETS.put("4.2_2.13", new TaskBudgets(20736, 21760, 14080, 15360));
  }

  enum ReadQuery {
    SNAPSHOT, READ_OPTIMIZED, INCREMENTAL, TIME_TRAVEL, CDC
  }

  @Override
  public SparkConf conf() {
    return conf(Collections.singletonMap("spark.plugins", SparkExecutorGuards.TASK_START_HOOK_PLUGIN));
  }

  @BeforeEach
  void enableRecording() {
    // Until #20090, a read can leave the session's vectorized reader flag changed; reset it so that
    // every case plans its scan the same way whatever ran before it.
    spark().conf().unset(VECTORIZED_READER_ENABLED);
    SparkExecutorGuards.enableFileSystemCallRecording(jsc().hadoopConfiguration());
  }

  @AfterEach
  void disableRecording() {
    SparkExecutorGuards.disableFileSystemCallRecording(jsc().hadoopConfiguration());
  }

  @AfterAll
  static void forgetTables() {
    TABLES.clear();
  }

  static Stream<Arguments> tableVersionsAndTypes() {
    return Stream.of(6, CURRENT_VERSION).flatMap(version ->
        Stream.of(COPY_ON_WRITE, MERGE_ON_READ).map(type -> Arguments.of(version, type)));
  }

  /**
   * With the metadata table off, a query other than an incremental one lists partitions from the
   * file system with a Spark job, whose tasks are guarded too.
   */
  static Stream<Arguments> readQueries() {
    List<Arguments> args = new ArrayList<>();
    for (int version : new int[] {6, CURRENT_VERSION}) {
      for (HoodieTableType type : HoodieTableType.values()) {
        for (ReadQuery query : Arrays.asList(ReadQuery.SNAPSHOT, ReadQuery.READ_OPTIMIZED, ReadQuery.INCREMENTAL, ReadQuery.TIME_TRAVEL)) {
          if (query == ReadQuery.READ_OPTIMIZED && type == COPY_ON_WRITE) {
            continue;
          }
          for (String metadataOnRead : new String[] {"default", "false"}) {
            args.add(Arguments.of(version, type, query, metadataOnRead));
          }
        }
      }
    }
    return args.stream();
  }

  /**
   * A CDC read of a version 6 MERGE_ON_READ table fails on the driver, so it is left out.
   */
  static Stream<Arguments> cdcTableVersionsAndTypes() {
    return tableVersionsAndTypes().filter(args -> !isVersion6MergeOnRead(args));
  }

  private static boolean isVersion6MergeOnRead(Arguments args) {
    Object[] values = args.get();
    return (int) values[0] == 6 && values[1] == MERGE_ON_READ;
  }

  static Stream<Arguments> readsForHadoopDefaultResources() {
    return Stream.of(6, CURRENT_VERSION).flatMap(version -> Stream.of(
        Arguments.of(version, COPY_ON_WRITE, TableKind.PLAIN),
        Arguments.of(version, MERGE_ON_READ, TableKind.PLAIN),
        Arguments.of(version, MERGE_ON_READ, TableKind.PARQUET_LOG_BLOCKS)));
  }

  /**
   * Every query on both table versions and types, except the CDC read of a version 6 MERGE_ON_READ
   * table, see {@link #cdcTableVersionsAndTypes}.
   */
  static Stream<Arguments> readsForDeserialization() {
    List<Arguments> args = new ArrayList<>();
    for (int version : new int[] {6, CURRENT_VERSION}) {
      for (HoodieTableType type : HoodieTableType.values()) {
        for (ReadQuery query : ReadQuery.values()) {
          if ((query == ReadQuery.READ_OPTIMIZED && type == COPY_ON_WRITE)
              || (query == ReadQuery.CDC && version == 6 && type == MERGE_ON_READ)) {
            continue;
          }
          TableKind kind = query == ReadQuery.CDC ? TableKind.CDC : TableKind.PLAIN;
          args.add(Arguments.of(version, type, kind, query, readShape(type, query)));
        }
      }
    }
    return args.stream();
  }

  /**
   * A read returns columnar batches when it scans base files without merging: snapshot and time
   * travel reads of COPY_ON_WRITE tables, and read optimized reads.
   */
  private static ReadShape readShape(HoodieTableType type, ReadQuery query) {
    switch (query) {
      case READ_OPTIMIZED:
        return ReadShape.COLUMNAR;
      case SNAPSHOT:
      case TIME_TRAVEL:
        return type == COPY_ON_WRITE ? ReadShape.COLUMNAR : ReadShape.ROW;
      default:
        return ReadShape.ROW;
    }
  }

  @ParameterizedTest(name = "[{index}] version={0}, type={1}, query={2}, metadata={3}")
  @MethodSource("readQueries")
  void testNoExecutorMetaFolderAccess(int tableVersion, HoodieTableType tableType, ReadQuery query, String metadataOnRead) {
    TestTable table = getOrWriteTable(tableVersion, tableType, TableKind.PLAIN);
    Map<String, String> options = new HashMap<>();
    if (!"default".equals(metadataOnRead)) {
      options.put(HoodieMetadataConfig.ENABLE.key(), metadataOnRead);
    }
    List<Row> rows = SparkExecutorGuards.assertNoExecutorMetaFolderAccess(
        table.name + " " + query + " read with metadata " + metadataOnRead,
        () -> read(table, query, options).collectAsList());
    assertFalse(rows.isEmpty(), "The read must return rows for the guard to be meaningful");
    if (query == ReadQuery.TIME_TRAVEL) {
      assertTrue(rows.stream().allMatch(row -> "v1".equals(row.getAs("value"))),
          table.name + ": time travel to the first commit must not see the updates of the second");
    }
  }

  @ParameterizedTest(name = "[{index}] version={0}, type={1}")
  @MethodSource("cdcTableVersionsAndTypes")
  void testNoExecutorMetaFolderAccessForCdcQuery(int tableVersion, HoodieTableType tableType) {
    TestTable table = getOrWriteTable(tableVersion, tableType, TableKind.CDC);
    List<Row> rows = SparkExecutorGuards.assertNoExecutorMetaFolderAccess(
        table.name + " CDC read",
        () -> read(table, ReadQuery.CDC, new HashMap<>()).collectAsList());
    assertFalse(rows.isEmpty(), "The CDC read must return rows for the guard to be meaningful");
  }

  /**
   * Tasks must not deserialize the meta client, timeline, Hadoop configuration, write config or the
   * file format with their closure, and what every task deserializes must stay within budget.
   */
  @ParameterizedTest(name = "[{index}] version={0}, type={1}, table={2}, query={3}, shape={4}")
  @MethodSource("readsForDeserialization")
  void testTaskDeserializationFootprint(int tableVersion, HoodieTableType tableType, TableKind kind, ReadQuery query,
                                        ReadShape shape) {
    String sparkVersion = sparkAndScalaBinaryVersion();
    TaskBudgets budgets = TASK_BUDGETS.get(sparkVersion);
    assertNotNull(budgets, "No task size budget for Spark and Scala " + sparkVersion
        + "; add one from the task sizes this test logs for each read");
    TestTable table = getOrWriteTable(tableVersion, tableType, kind);
    Dataset<Row> df = read(table, query, new HashMap<>());
    // Plan and list files on the driver first, so that the recorded window holds only the scan.
    df.queryExecution().executedPlan().execute();
    List<Row> rows = new ArrayList<>();
    TaskDeserializationRecorder.Result result = SparkExecutorGuards.recordTaskDeserialization(
        spark().sparkContext(), () -> rows.addAll(df.collectAsList()));
    assertFalse(rows.isEmpty(), "The read must return rows for the guard to be meaningful");
    SparkExecutorGuards.TaskBinary taskBinary = SparkExecutorGuards.inspectTaskBinary(df);
    log.info("{} read of {}: task binary {} bytes, largest task stream seen {} bytes, stages kept {}, ignored {}",
        query, table.name, taskBinary.getBytes(), result.getMaxStreamBytes(), result.getKeptScopes(), result.getIgnoredScopes());
    SparkExecutorGuards.assertTaskDeserializationFootprint(
        table.name + " " + query + " read on Spark and Scala " + sparkVersion, result, taskBinary,
        CLASSES_NOT_DESERIALIZED_PER_TASK, HEAVY_DRIVER_CLASSES, budgets.maxTaskBinaryBytes(shape), budgets.maxTaskStreamBytes(shape));
    // Local mode runs one executor, so the broadcast scan state is deserialized at most once.
    assertTrue(result.getExemptDeserializations(SparkExecutorGuards.SCAN_STATE, SCAN_STATE_CLASS) <= 1, table.name + " " + query
        + " read: the scan state must be deserialized once per executor, not per task or file, but was deserialized "
        + result.getExemptDeserializations(SparkExecutorGuards.SCAN_STATE, SCAN_STATE_CLASS) + " times");
  }

  /**
   * Tasks must read base files and log blocks with the configuration they are given rather than
   * create one per file, which parses the Hadoop default resources every time.
   */
  @ParameterizedTest(name = "[{index}] version={0}, type={1}, table={2}")
  @MethodSource("readsForHadoopDefaultResources")
  void testNoHadoopDefaultResourceLoadsPerFile(int tableVersion, HoodieTableType tableType, TableKind kind) {
    TestTable table = getOrWriteTable(tableVersion, tableType, kind);
    Dataset<Row> df = read(table, ReadQuery.SNAPSHOT, new HashMap<>());
    List<Row> rows = SparkExecutorGuards.assertTaskHadoopDefaultResourceLoadsAtMost(
        table.name + " snapshot read", spark().sparkContext(), MAX_HADOOP_DEFAULT_RESOURCE_LOADS, df::collectAsList);
    assertEquals(NUM_RECORDS, rows.size());
  }

  /**
   * With data skipping, file pruning consults the column stats in the metadata table on the driver.
   * The tasks of the scan must still not access {@code .hoodie} of either table. Every partition holds
   * the keys {@code i} with the same {@code i % NUM_PARTITIONS}, so a lookup of the first key prunes the
   * files of all partitions but the first by their key ranges; strict mode fails the read if data
   * skipping cannot be applied. A column stats lookup forced onto the engine reads the metadata table
   * from the executors by design and is not covered.
   */
  @ParameterizedTest(name = "[{index}] version={0}, type={1}")
  @MethodSource("tableVersionsAndTypes")
  void testNoExecutorMetaFolderAccessWithDataSkipping(int tableVersion, HoodieTableType tableType) {
    TestTable table = getOrWriteTable(tableVersion, tableType, TableKind.PLAIN);
    Map<String, String> options = new HashMap<>();
    options.put(DataSourceReadOptions.ENABLE_DATA_SKIPPING().key(), "true");
    List<Row> rows;
    spark().conf().set(DATA_SKIPPING_FAILURE_MODE, "strict");
    try {
      rows = SparkExecutorGuards.assertNoExecutorMetaFolderAccess(
          table.name + " snapshot read with data skipping",
          () -> read(table, ReadQuery.SNAPSHOT, options).filter("key = '" + key(0) + "'").collectAsList());
    } finally {
      spark().conf().unset(DATA_SKIPPING_FAILURE_MODE);
    }
    assertEquals(1, rows.size());
    assertTrue(RecordingLocalFileSystem.count(Call.inScope().negate().and(Call.pathContains("/.hoodie/metadata/column_stats/"))) > 0,
        table.name + ": the driver must read the column stats index for the read to exercise data skipping");
    assertTrue(RecordingLocalFileSystem.count(Call.inScope().and(Call.pathContains("/p0/"))) > 0,
        table.name + ": the tasks must read the files of the partition that holds the key");
    Predicate<Call> prunedFileAccess = Call.inScope().and(
        IntStream.range(1, NUM_PARTITIONS).mapToObj(i -> Call.pathContains("/p" + i + "/")).reduce(call -> false, Predicate::or));
    assertEquals(0, RecordingLocalFileSystem.count(prunedFileAccess),
        table.name + ": data skipping must prune the files of the partitions without the key, but the tasks read:\n"
            + RecordingLocalFileSystem.describe(prunedFileAccess));
  }

  private Dataset<Row> read(TestTable table, ReadQuery query, Map<String, String> options) {
    DataFrameReader reader = spark().read().format("hudi").options(options);
    switch (query) {
      case SNAPSHOT:
        reader.option(DataSourceReadOptions.QUERY_TYPE().key(), DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL());
        break;
      case READ_OPTIMIZED:
        reader.option(DataSourceReadOptions.QUERY_TYPE().key(), DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL());
        break;
      case TIME_TRAVEL:
        reader.option(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT().key(), table.firstInstant.requestedTime());
        break;
      case INCREMENTAL:
      case CDC:
        reader.option(DataSourceReadOptions.QUERY_TYPE().key(), DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL())
            .option(DataSourceReadOptions.START_COMMIT().key(), table.incrementalTime(table.firstInstant))
            .option(DataSourceReadOptions.END_COMMIT().key(), table.incrementalTime(table.lastInstant));
        if (query == ReadQuery.CDC) {
          reader.option(DataSourceReadOptions.INCREMENTAL_FORMAT().key(), DataSourceReadOptions.INCREMENTAL_FORMAT_CDC_VAL());
        }
        break;
      default:
        throw new IllegalArgumentException("Unknown query " + query);
    }
    return reader.load(table.basePath);
  }

  private TestTable getOrWriteTable(int tableVersion, HoodieTableType tableType, TableKind kind) {
    String name = kind.name().toLowerCase() + "_" + tableType.name().toLowerCase() + "_v" + tableVersion;
    return TABLES.computeIfAbsent(name, n -> writeTable(n, tableVersion, tableType, kind));
  }

  private TestTable writeTable(String name, int tableVersion, HoodieTableType tableType, TableKind kind) {
    String basePath = tablesDir.resolve(name).toUri().toString();
    Map<String, String> options = new HashMap<>();
    options.put(HoodieTableConfig.NAME.key(), name);
    options.put(DataSourceWriteOptions.TABLE_TYPE().key(), tableType.name());
    options.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "key");
    options.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "part");
    options.put(DataSourceWriteOptions.ORDERING_FIELDS().key(), "ts");
    options.put(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), String.valueOf(tableVersion));
    options.put(HoodieWriteConfig.AUTO_UPGRADE_VERSION.key(), "false");
    options.put("hoodie.insert.shuffle.parallelism", "2");
    options.put("hoodie.upsert.shuffle.parallelism", "2");
    // Keep the whole history on the active timeline and the log files of MERGE_ON_READ uncompacted.
    options.put(HoodieArchivalConfig.MIN_COMMITS_TO_KEEP.key(), String.valueOf(NUM_HISTORY_COMMITS + 10));
    options.put(HoodieArchivalConfig.MAX_COMMITS_TO_KEEP.key(), String.valueOf(NUM_HISTORY_COMMITS + 20));
    options.put(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(), String.valueOf(NUM_HISTORY_COMMITS + 10));
    // Index column stats, so that data skipping has an index to consult.
    options.put(HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key(), "true");
    if (kind == TableKind.CDC) {
      options.put(HoodieTableConfig.CDC_ENABLED.key(), "true");
    }
    if (kind == TableKind.PARQUET_LOG_BLOCKS) {
      options.put(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key(), "parquet");
    }

    initTableWithHistory(name, basePath, tableVersion, tableType, kind);
    List<Row> inserts = IntStream.range(0, NUM_RECORDS)
        .mapToObj(i -> RowFactory.create(key(i), "p" + (i % NUM_PARTITIONS), 1L, "v1"))
        .collect(Collectors.toList());
    write(inserts, SCHEMA, options, basePath);
    // The second commit updates half of the keys.
    List<Row> updates = IntStream.range(0, NUM_UPDATED_RECORDS)
        .mapToObj(i -> RowFactory.create(key(i), "p" + (i % NUM_PARTITIONS), 2L, "v2"))
        .collect(Collectors.toList());
    write(updates, SCHEMA, options, basePath);

    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(storageConf()).build();
    assertEquals(tableVersion, metaClient.getTableConfig().getTableVersion().versionCode());
    List<HoodieInstant> commits = metaClient.getCommitsTimeline().filterCompletedInstants().getInstants();
    assertEquals(NUM_HISTORY_COMMITS + 2, commits.size(), "Expected the history and two completed commits in " + name);
    if (tableType == MERGE_ON_READ) {
      assertTrue(countLogFiles(tablesDir.resolve(name)) >= NUM_PARTITIONS,
          "The updates of " + name + " should write log files in every partition");
    }
    return new TestTable(name, basePath, tableVersion, commits.get(NUM_HISTORY_COMMITS), commits.get(NUM_HISTORY_COMMITS + 1));
  }

  /**
   * Creates the table with the configuration the writes use and {@link #NUM_HISTORY_COMMITS} empty
   * completed commits.
   */
  private void initTableWithHistory(String name, String basePath, int tableVersion, HoodieTableType tableType,
                                    TableKind kind) {
    try {
      HoodieTableMetaClient metaClient = HoodieTableMetaClient.newTableBuilder()
          .setTableType(tableType)
          .setTableName(name)
          .setTableVersion(tableVersion)
          .setRecordKeyFields("key")
          .setPartitionFields("part")
          .setOrderingFields("ts")
          .setKeyGeneratorClassProp(SimpleKeyGenerator.class.getName())
          .setCDCEnabled(kind == TableKind.CDC)
          .initTable(storageConf(), basePath);
      HoodieTestTable history = HoodieTestTable.of(metaClient);
      for (int i = 0; i < NUM_HISTORY_COMMITS; i++) {
        String instantTime = InProcessTimeGenerator.createNewInstantTime();
        if (tableType == COPY_ON_WRITE) {
          history.addCommit(instantTime);
        } else {
          history.addDeltaCommit(instantTime);
        }
      }
    } catch (Exception e) {
      throw new IllegalStateException("Cannot create the history of " + name, e);
    }
  }

  private void write(List<Row> rows, StructType schema, Map<String, String> options, String basePath) {
    spark().createDataset(rows, SparkAdapterSupport$.MODULE$.sparkAdapter().getCatalystExpressionUtils().getEncoder(schema))
        .write()
        .format("hudi")
        .options(options)
        .mode(SaveMode.Append)
        .save(basePath);
  }

  private static long countLogFiles(Path tableDir) {
    try (Stream<Path> files = Files.walk(tableDir)) {
      return files.filter(p -> !p.toString().contains("/.hoodie/") && p.getFileName().toString().contains(".log.")).count();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * The running Spark version and Scala binary version, such as {@code 3.5_2.12}.
   */
  private String sparkAndScalaBinaryVersion() {
    String[] spark = spark().version().split("\\.");
    String[] scala = Properties.versionNumberString().split("\\.");
    return spark[0] + "." + spark[1] + "_" + scala[0] + "." + scala[1];
  }

  private static String key(int i) {
    return String.format("key%03d", i);
  }

  private static final class TaskBudgets {
    private final long columnarTaskBinaryBytes;
    private final long columnarTaskStreamBytes;
    private final long rowTaskBinaryBytes;
    private final long rowTaskStreamBytes;

    private TaskBudgets(long columnarTaskBinaryBytes, long columnarTaskStreamBytes, long rowTaskBinaryBytes,
                        long rowTaskStreamBytes) {
      this.columnarTaskBinaryBytes = columnarTaskBinaryBytes;
      this.columnarTaskStreamBytes = columnarTaskStreamBytes;
      this.rowTaskBinaryBytes = rowTaskBinaryBytes;
      this.rowTaskStreamBytes = rowTaskStreamBytes;
    }

    private long maxTaskBinaryBytes(ReadShape shape) {
      return shape == ReadShape.COLUMNAR ? columnarTaskBinaryBytes : rowTaskBinaryBytes;
    }

    private long maxTaskStreamBytes(ReadShape shape) {
      return shape == ReadShape.COLUMNAR ? columnarTaskStreamBytes : rowTaskStreamBytes;
    }
  }

  private static final class TestTable {
    private final String name;
    private final String basePath;
    private final int tableVersion;
    private final HoodieInstant firstInstant;
    private final HoodieInstant lastInstant;

    private TestTable(String name, String basePath, int tableVersion, HoodieInstant firstInstant, HoodieInstant lastInstant) {
      this.name = name;
      this.basePath = basePath;
      this.tableVersion = tableVersion;
      this.firstInstant = firstInstant;
      this.lastInstant = lastInstant;
    }

    /**
     * Incremental queries range over requested times before table version 8 and over completion
     * times from version 8 on.
     */
    private String incrementalTime(HoodieInstant instant) {
      return tableVersion < 8 ? instant.requestedTime() : instant.getCompletionTime();
    }
  }
}
