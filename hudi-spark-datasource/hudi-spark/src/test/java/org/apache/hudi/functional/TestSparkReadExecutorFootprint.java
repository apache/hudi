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
import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.InternalSchemaCache;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.testutils.SparkExecutorGuards;
import org.apache.hudi.testutils.TaskDeserializationRecorder;

import com.github.benmanes.caffeine.cache.Cache;
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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;
import static org.apache.hudi.common.model.HoodieTableType.MERGE_ON_READ;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Guards what Spark tasks do on the executors when reading a Hudi table through the file group
 * reader: they must not access the table's {@code .hoodie} folder, and they must not deserialize
 * heavy driver-side objects (meta client, timeline, Hadoop configuration, the file format itself)
 * with every task closure. Both costs scale with the number of tasks, not with the data.
 *
 * <p>Each table is written once per class and read by every case that needs it. The table has
 * several partitions so that a read runs several tasks; on MERGE_ON_READ the second commit
 * updates half of the keys so that every file group has log files to merge.
 */
@Slf4j
@Tag("functional")
class TestSparkReadExecutorFootprint extends SparkClientFunctionalTestHarness {

  private static final int CURRENT_VERSION = HoodieTableVersion.current().versionCode();
  private static final int NUM_RECORDS = 200;
  private static final int NUM_UPDATED_RECORDS = 100;
  private static final int NUM_PARTITIONS = 4;

  /**
   * Budget for the task binary, the Java-serialized closure every task deserializes.
   */
  private static final long MAX_TASK_BINARY_BYTES = 24 * 1024;
  private static final String TASK_BINARY_BUDGET_BASIS =
      "the budget is about 1.4x the largest task binary measured for these reads with the scan state broadcast:"
          + " 17544 bytes for the base file only reads, 10194 to 11046 bytes for the others";

  /**
   * Driver-side classes that a read task must not deserialize with its closure.
   */
  private static final List<String> CLASSES_NOT_DESERIALIZED_PER_TASK = Arrays.asList(
      "org.apache.hudi.common.table.HoodieTableMetaClient",
      "org.apache.hudi.common.table.timeline.HoodieActiveTimeline",
      "org.apache.hudi.storage.StorageConfiguration",
      "org.apache.spark.util.SerializableConfiguration",
      "org.apache.spark.sql.execution.datasources.parquet.HoodieFileGroupReaderBasedFileFormat",
      "org.apache.hudi.config.HoodieWriteConfig");

  private static final StructType SCHEMA = DataTypes.createStructType(new StructField[] {
      DataTypes.createStructField("key", DataTypes.StringType, false),
      DataTypes.createStructField("part", DataTypes.StringType, false),
      DataTypes.createStructField("ts", DataTypes.LongType, false),
      DataTypes.createStructField("value", DataTypes.StringType, true)});

  private static final StructType EVOLVED_SCHEMA = SCHEMA.add(
      DataTypes.createStructField("extra", DataTypes.StringType, true));

  private static final String VECTORIZED_READER_ENABLED = "spark.sql.parquet.enableVectorizedReader";

  private static final Map<String, TestTable> TABLES = new HashMap<>();

  @TempDir
  static Path tablesDir;

  enum TableKind {
    PLAIN, SCHEMA_ON_READ, CDC
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

  static Stream<Arguments> readQueries() {
    return allReadQueries().filter(args -> !listsPartitionsFromFileSystem(args));
  }

  static Stream<Arguments> readQueriesListingPartitionsFromFileSystem() {
    return allReadQueries().filter(TestSparkReadExecutorFootprint::listsPartitionsFromFileSystem);
  }

  /**
   * Whether the read lists partitions from the file system with a Spark job, which a query does
   * when the metadata table is off, except an incremental one, which takes them from the timeline.
   */
  private static boolean listsPartitionsFromFileSystem(Arguments args) {
    Object[] values = args.get();
    return "false".equals(values[3]) && values[2] != ReadQuery.INCREMENTAL;
  }

  private static Stream<Arguments> allReadQueries() {
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

  static Stream<Arguments> cdcTableVersionsAndTypes() {
    return tableVersionsAndTypes().filter(args -> !isVersion6MergeOnRead(args));
  }

  private static boolean isVersion6MergeOnRead(Arguments args) {
    Object[] values = args.get();
    return (int) values[0] == 6 && values[1] == MERGE_ON_READ;
  }

  static Stream<Arguments> readsForDeserialization() {
    return Stream.of(
        Arguments.of(CURRENT_VERSION, COPY_ON_WRITE, TableKind.PLAIN, ReadQuery.SNAPSHOT),
        Arguments.of(CURRENT_VERSION, MERGE_ON_READ, TableKind.PLAIN, ReadQuery.SNAPSHOT),
        Arguments.of(6, MERGE_ON_READ, TableKind.PLAIN, ReadQuery.SNAPSHOT),
        Arguments.of(CURRENT_VERSION, MERGE_ON_READ, TableKind.PLAIN, ReadQuery.READ_OPTIMIZED),
        Arguments.of(CURRENT_VERSION, MERGE_ON_READ, TableKind.PLAIN, ReadQuery.INCREMENTAL),
        Arguments.of(6, MERGE_ON_READ, TableKind.PLAIN, ReadQuery.INCREMENTAL),
        Arguments.of(CURRENT_VERSION, MERGE_ON_READ, TableKind.PLAIN, ReadQuery.TIME_TRAVEL),
        Arguments.of(CURRENT_VERSION, MERGE_ON_READ, TableKind.CDC, ReadQuery.CDC));
  }

  @ParameterizedTest(name = "[{index}] version={0}, type={1}, query={2}, metadata={3}")
  @MethodSource("readQueries")
  void testNoExecutorMetaFolderAccess(int tableVersion, HoodieTableType tableType, ReadQuery query, String metadataOnRead) {
    assertNoExecutorMetaFolderAccessForRead(tableVersion, tableType, query, metadataOnRead);
  }

  @Disabled("Enabled by #20080: the partition listing job probes .hoodie for partition metadata on the executors")
  @ParameterizedTest(name = "[{index}] version={0}, type={1}, query={2}, metadata={3}")
  @MethodSource("readQueriesListingPartitionsFromFileSystem")
  void testNoExecutorMetaFolderAccessListingPartitionsFromFileSystem(int tableVersion, HoodieTableType tableType,
                                                                    ReadQuery query, String metadataOnRead) {
    assertNoExecutorMetaFolderAccessForRead(tableVersion, tableType, query, metadataOnRead);
  }

  private void assertNoExecutorMetaFolderAccessForRead(int tableVersion, HoodieTableType tableType, ReadQuery query,
                                                       String metadataOnRead) {
    TestTable table = getOrWriteTable(tableVersion, tableType, TableKind.PLAIN);
    Map<String, String> options = new HashMap<>();
    if (!"default".equals(metadataOnRead)) {
      options.put(HoodieMetadataConfig.ENABLE.key(), metadataOnRead);
    }
    List<Row> rows = SparkExecutorGuards.assertNoExecutorMetaFolderAccess(
        table.name + " " + query + " read with metadata " + metadataOnRead,
        () -> read(table, query, options).collectAsList());
    assertFalse(rows.isEmpty(), "The read must return rows for the guard to be meaningful");
  }

  /**
   * With schema on read, every base and log file needs the internal schema of the commit that wrote
   * it. The schema history has to reach the tasks from the driver rather than be looked up in
   * {@code .hoodie} by each task.
   */
  @Disabled("Enabled by #20085: tasks look up the schema history in .hoodie for every file")
  @ParameterizedTest(name = "[{index}] version={0}, type={1}")
  @MethodSource("tableVersionsAndTypes")
  void testNoExecutorMetaFolderAccessWithSchemaOnRead(int tableVersion, HoodieTableType tableType) {
    TestTable table = getOrWriteTable(tableVersion, tableType, TableKind.SCHEMA_ON_READ);
    Map<String, String> options = new HashMap<>();
    options.put(HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.key(), "true");
    AtomicInteger tasksStarted = new AtomicInteger();
    SparkExecutorGuards.setTaskStartHook(() -> {
      tasksStarted.incrementAndGet();
      clearHistoricalSchemaCache();
    });
    List<Row> rows;
    try {
      rows = SparkExecutorGuards.assertNoExecutorMetaFolderAccess(
          table.name + " schema-on-read snapshot read",
          () -> read(table, ReadQuery.SNAPSHOT, options).collectAsList());
    } finally {
      SparkExecutorGuards.setTaskStartHook(() -> { });
    }
    assertTrue(tasksStarted.get() > 0, "The task start hook must run so that every task starts with an empty"
        + " schema history cache, as it does on an executor that the driver does not share a JVM with");
    assertEquals(NUM_RECORDS, rows.size());
  }

  @ParameterizedTest(name = "[{index}] version={0}, type={1}")
  @MethodSource("cdcTableVersionsAndTypes")
  void testNoExecutorMetaFolderAccessForCdcQuery(int tableVersion, HoodieTableType tableType) {
    assertNoExecutorMetaFolderAccessForCdcQuery(tableVersion, tableType);
  }

  @Disabled("Enabled by #20071: a CDC read of a version 6 MERGE_ON_READ table fails on the driver")
  @Test
  void testVersion6MergeOnReadCdcQuery() {
    assertNoExecutorMetaFolderAccessForCdcQuery(6, MERGE_ON_READ);
    assertTaskDeserializationFootprint(6, MERGE_ON_READ, TableKind.CDC, ReadQuery.CDC);
  }

  private void assertNoExecutorMetaFolderAccessForCdcQuery(int tableVersion, HoodieTableType tableType) {
    TestTable table = getOrWriteTable(tableVersion, tableType, TableKind.CDC);
    List<Row> rows = SparkExecutorGuards.assertNoExecutorMetaFolderAccess(
        table.name + " CDC read",
        () -> read(table, ReadQuery.CDC, new HashMap<>()).collectAsList());
    assertFalse(rows.isEmpty(), "The CDC read must return rows for the guard to be meaningful");
  }

  /**
   * Tasks must not deserialize the meta client, timeline, Hadoop configuration, write config or the
   * file format with their closure, and the task binary must stay within budget.
   */
  @ParameterizedTest(name = "[{index}] version={0}, type={1}, table={2}, query={3}")
  @MethodSource("readsForDeserialization")
  void testTaskDeserializationFootprint(int tableVersion, HoodieTableType tableType, TableKind kind, ReadQuery query) {
    assertTaskDeserializationFootprint(tableVersion, tableType, kind, query);
  }

  private void assertTaskDeserializationFootprint(int tableVersion, HoodieTableType tableType, TableKind kind, ReadQuery query) {
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
        table.name + " " + query + " read (" + TASK_BINARY_BUDGET_BASIS + ")", result, taskBinary,
        CLASSES_NOT_DESERIALIZED_PER_TASK, MAX_TASK_BINARY_BYTES);
  }

  /**
   * With data skipping, file pruning consults the column stats in the metadata table. The tasks of
   * the scan must still not read the timeline or table config of either table.
   */
  @ParameterizedTest(name = "[{index}] version={0}, type={1}")
  @MethodSource("tableVersionsAndTypes")
  void testNoExecutorMetaFolderAccessWithDataSkipping(int tableVersion, HoodieTableType tableType) {
    TestTable table = getOrWriteTable(tableVersion, tableType, TableKind.PLAIN);
    Map<String, String> options = new HashMap<>();
    options.put(DataSourceReadOptions.ENABLE_DATA_SKIPPING().key(), "true");
    List<Row> rows = SparkExecutorGuards.assertNoExecutorMetaFolderAccess(
        table.name + " snapshot read with data skipping",
        () -> read(table, ReadQuery.SNAPSHOT, options).filter("value = 'v2'").collectAsList());
    assertEquals(NUM_UPDATED_RECORDS, rows.size());
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
        reader.option(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT().key(), table.lastInstant.requestedTime());
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
    if (kind == TableKind.SCHEMA_ON_READ) {
      // Reconciling makes the writer record an internal schema in the commit metadata.
      options.put(HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.key(), "true");
      options.put(DataSourceWriteOptions.RECONCILE_SCHEMA().key(), "true");
    }
    if (kind == TableKind.CDC) {
      options.put(HoodieTableConfig.CDC_ENABLED.key(), "true");
    }

    List<Row> inserts = IntStream.range(0, NUM_RECORDS)
        .mapToObj(i -> RowFactory.create(key(i), "p" + (i % NUM_PARTITIONS), 1L, "v1"))
        .collect(Collectors.toList());
    write(inserts, SCHEMA, options, basePath);
    // The second commit updates half of the keys; with schema on read it also adds a column, so
    // that the files of the two commits carry different schema versions.
    if (kind == TableKind.SCHEMA_ON_READ) {
      List<Row> updates = IntStream.range(0, NUM_UPDATED_RECORDS)
          .mapToObj(i -> RowFactory.create(key(i), "p" + (i % NUM_PARTITIONS), 2L, "v2", "e2"))
          .collect(Collectors.toList());
      write(updates, EVOLVED_SCHEMA, options, basePath);
    } else {
      List<Row> updates = IntStream.range(0, NUM_UPDATED_RECORDS)
          .mapToObj(i -> RowFactory.create(key(i), "p" + (i % NUM_PARTITIONS), 2L, "v2"))
          .collect(Collectors.toList());
      write(updates, SCHEMA, options, basePath);
    }

    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(storageConf()).build();
    assertEquals(tableVersion, metaClient.getTableConfig().getTableVersion().versionCode());
    List<HoodieInstant> commits = metaClient.getCommitsTimeline().filterCompletedInstants().getInstants();
    assertEquals(2, commits.size(), "Expected two completed commits in " + name);
    if (kind == TableKind.SCHEMA_ON_READ) {
      assertTrue(new TableSchemaResolver(metaClient).getTableInternalSchemaFromCommitMetadata().isPresent(),
          "The schema-on-read table " + name + " should carry an internal schema");
    }
    if (tableType == MERGE_ON_READ) {
      assertTrue(countLogFiles(tablesDir.resolve(name)) >= NUM_PARTITIONS,
          "Every file group of " + name + " should have log files to merge");
    }
    return new TestTable(name, basePath, tableVersion, commits.get(0), commits.get(1));
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
   * Empties the JVM-wide cache of historical internal schemas. In local mode the driver and the
   * executors share it, which would hide executor-side schema history reads.
   */
  private static void clearHistoricalSchemaCache() {
    try {
      Field field = InternalSchemaCache.class.getDeclaredField("HISTORICAL_SCHEMA_CACHE");
      field.setAccessible(true);
      ((Cache<?, ?>) field.get(null)).invalidateAll();
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Cannot clear the historical schema cache", e);
    }
  }

  private static String key(int i) {
    return String.format("key%03d", i);
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
