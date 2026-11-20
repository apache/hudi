/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.utilities.streamer;

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.checkpoint.Checkpoint;
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV1;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.collection.Triple;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.metrics.Metrics;
import org.apache.hudi.utilities.config.HoodieIncrSourceConfig;
import org.apache.hudi.utilities.ingestion.HoodieIngestionMetrics;
import org.apache.hudi.utilities.sources.SourceContract;
import org.apache.hudi.utilities.sources.UnstructuredDataSource;
import org.apache.hudi.utilities.sources.unstructured.UnstructuredSourceConfig;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The streamer builds the source's {@link SourceContract} from the job's own properties and merges the settings it declares into a
 * copy of them first, before the key generator, the write config, the metrics, the error table and the table. The source is built
 * last, from the merged properties, after the contract's checks: a job they refuse never builds its source. A refusal before the
 * metrics are built (a settings conflict) leaves no metrics registered; a later one leaves them, as upstream does.
 */
public class TestStreamSyncSourceSettingsOrder extends UtilitiesTestBase {

  private static final String RECORD_KEY = KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key();
  private static final String REQUIRED_KEY = "source_path_sha,source_modification_time,source_file_size";
  /** The blob job's codec default, to see where the prepared settings reach. */
  private static final String DECLARED_ONLY = HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME.key();
  private static final String DECLARED_VALUE = "uncompressed";

  @TempDir
  Path tempDir;

  @BeforeAll
  public static void initClass() throws Exception {
    UtilitiesTestBase.initTestServices();
  }

  @AfterAll
  public static void cleanupClass() throws Exception {
    UtilitiesTestBase.cleanUpUtilitiesTestServices();
  }

  @BeforeEach
  public void resetSource() {
    ContractTestSource.SEEN.set(null);
    ContractTestSource.BUILT.set(0);
  }

  private String table() {
    return tempDir.resolve("table").toUri().toString();
  }

  private HoodieStreamer.Config config() {
    HoodieStreamer.Config cfg = new HoodieStreamer.Config();
    cfg.targetBasePath = table();
    cfg.targetTableName = "settings_order";
    cfg.tableType = HoodieTableType.COPY_ON_WRITE.name();
    cfg.sourceClassName = ContractTestSource.class.getName();
    Triple<RecordMergeMode, String, String> merging = HoodieTableConfig.inferMergingConfigsForWrites(
        null, cfg.payloadClassName, cfg.recordMergeStrategyId, null, HoodieTableVersion.current());
    cfg.recordMergeMode = merging.getLeft();
    cfg.payloadClassName = merging.getMiddle();
    cfg.recordMergeStrategyId = merging.getRight();
    return cfg;
  }

  private static TypedProperties jobProps() {
    TypedProperties props = new TypedProperties();
    props.setProperty("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.NonpartitionedKeyGenerator");
    props.setProperty(UnstructuredSourceConfig.MODE.key(), "blob");
    props.setProperty(UnstructuredSourceConfig.CLOUD.key(), "s3");
    props.setProperty(HoodieIncrSourceConfig.HOODIE_SRC_BASE_PATH.key(), "file:/unused-events");
    return props;
  }

  private StreamSync streamSync(TypedProperties props) throws Exception {
    return new StreamSync(config(), sparkSession, props, new HoodieSparkEngineContext(jsc), fs, jsc.hadoopConfiguration(),
        client -> true, new DefaultStreamContext(null, Option.empty()));
  }

  private HoodieTableConfig tableConfig() {
    return HoodieTableMetaClient.builder().setBasePath(table()).setConf(HadoopFSUtils.getStorageConf(jsc.hadoopConfiguration()))
        .build().getTableConfig();
  }

  /** Creates the table with another record key, as an earlier job would have. */
  private void createTableWithKey(String recordKey) throws Exception {
    HoodieTableMetaClient.newTableBuilder().setTableType(HoodieTableType.COPY_ON_WRITE).setTableName("settings_order")
        .setRecordKeyFields(recordKey).setKeyGeneratorClassProp("org.apache.hudi.keygen.NonpartitionedKeyGenerator")
        .setAdditionalProperties(new org.apache.hudi.utilities.sources.unstructured.UnstructuredSourceContract(jobProps()).tableProperties())
        .initTable(HadoopFSUtils.getStorageConf(jsc.hadoopConfiguration()), table());
  }

  @Test
  void settingsReachWriteConfigAndNewTable() throws Exception {
    try (StreamSync sync = streamSync(jobProps())) {
      assertEquals(REQUIRED_KEY, sync.getProps().getString(RECORD_KEY));
      assertEquals(DECLARED_VALUE, sync.getProps().getString(DECLARED_ONLY));
    }
    assertEquals(REQUIRED_KEY, tableConfig().getString(HoodieTableConfig.RECORDKEY_FIELDS));
  }

  @ParameterizedTest
  @ValueSource(strings = {"blob", "embedding"})
  void streamCreationPersistsKeyEncodingAndTablePins(String mode) throws Exception {
    TypedProperties props = jobProps();
    props.setProperty(UnstructuredSourceConfig.MODE.key(), mode);
    if ("embedding".equals(mode)) {
      props.setProperty("hoodie.unstructured.embedding.mode", "prep_only");
    }
    try (StreamSync ignored = streamSync(props)) {
      // Construction creates the table before it builds the source; it fetches no input.
    }
    HoodieTableConfig stored = tableConfig();
    assertEquals("1", stored.getProps().getProperty("hoodie.table.unstructured.key.encoding.version"));
    assertEquals("1", stored.getProps().getProperty("hoodie.table.blob.layout.version"));
    assertEquals(mode, stored.getProps().getProperty("hoodie.table.unstructured.kind"));
    assertEquals("1", stored.getProps().getProperty("hoodie.table.unstructured.schema.version"));
  }

  @ParameterizedTest
  @CsvSource({"blob,missing", "blob,2", "embedding,missing", "embedding,2"})
  void streamRefusesKeyEncodingBeforeConstructingSource(String mode, String version) throws Exception {
    TypedProperties props = jobProps();
    props.setProperty(UnstructuredSourceConfig.MODE.key(), mode);
    if ("embedding".equals(mode)) {
      props.setProperty("hoodie.unstructured.embedding.mode", "prep_only");
    }
    try (StreamSync ignored = streamSync(props)) {
      // Establish a table carrying this build's other valid pins.
    }
    ContractTestSource.BUILT.set(0);
    String key = "hoodie.table.unstructured.key.encoding.version";
    org.apache.hudi.common.table.HoodieTableMetaClient meta = org.apache.hudi.common.table.HoodieTableMetaClient.builder()
        .setBasePath(table()).setConf(HadoopFSUtils.getStorageConf(jsc.hadoopConfiguration())).build();
    if ("missing".equals(version)) {
      HoodieTableConfig.delete(meta.getStorage(), meta.getMetaPath(), Collections.singleton(key));
    } else {
      java.util.Properties changed = new java.util.Properties();
      changed.setProperty(key, version);
      HoodieTableConfig.update(meta.getStorage(), meta.getMetaPath(), changed);
    }
    IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> streamSync(props));
    assertTrue(error.getMessage().contains(key + "=1"), error.getMessage());
    assertEquals(0, ContractTestSource.BUILT.get());
  }

  /** The source is built from the merged properties, so it sees the settings the writer uses; the job's own are not changed. */
  @Test
  void sourceConstructorSeesMergedProps() throws Exception {
    TypedProperties props = jobProps();
    TypedProperties before = TypedProperties.copy(props);
    try (StreamSync sync = streamSync(props)) {
      TypedProperties seen = ContractTestSource.SEEN.get();
      assertNotNull(seen, "the source was not built");
      assertEquals(REQUIRED_KEY, seen.getString(RECORD_KEY));
      assertEquals(DECLARED_VALUE, seen.getString(DECLARED_ONLY));
      assertEquals(before, props, "the job's properties changed");
    }
  }

  @Test
  void conflictOnNewTableRefusedNamingSource() {
    TypedProperties props = jobProps();
    props.setProperty(RECORD_KEY, "b,a");
    IllegalArgumentException refused = assertThrows(IllegalArgumentException.class, () -> streamSync(props));
    assertEquals(ContractTestSource.NAME + " requires " + RECORD_KEY + "=" + REQUIRED_KEY + " (these fields, in this order), configured: b,a",
        refused.getMessage());
    assertEquals(0, ContractTestSource.BUILT.get(), "the refused job built its source");
  }

  @Test
  void existingTableWithOtherKeyRefusedByCheckTable() throws Exception {
    createTableWithKey("x");
    IllegalArgumentException refused = assertThrows(IllegalArgumentException.class, () -> streamSync(jobProps()));
    assertTrue(refused.getMessage().startsWith(ContractTestSource.NAME + " requires the table's "
        + HoodieTableConfig.RECORDKEY_FIELDS.key() + "=" + REQUIRED_KEY), refused.getMessage());
    assertEquals(0, ContractTestSource.BUILT.get(), "the refused job built its source");
  }

  /**
   * On an existing table, the streamer's stock table check runs on the properties the job runs with, after the source's own checks:
   * a key the job sets against the source's is refused naming the source, and a conflict only the stock check knows (the partition
   * field) by the stock check.
   */
  @Test
  void conflictOnExistingTableRefusedByStockCheck() throws Exception {
    createTableWithKey(REQUIRED_KEY);
    TypedProperties conflicting = jobProps();
    conflicting.setProperty(RECORD_KEY, "y");
    String sourceRefusal = causes(assertThrows(Exception.class, () -> new HoodieStreamer(config(), jsc, Option.of(conflicting))));
    assertTrue(sourceRefusal.contains(ContractTestSource.NAME + " requires " + RECORD_KEY + "=" + REQUIRED_KEY), sourceRefusal);

    TypedProperties partitioned = jobProps();
    partitioned.setProperty(RECORD_KEY, " " + REQUIRED_KEY.replace(",", " , "));
    partitioned.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "p");
    partitioned.setProperty("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.ComplexKeyGenerator");
    String stockRefusal = causes(assertThrows(Exception.class, () -> new HoodieStreamer(config(), jsc, Option.of(partitioned))));
    assertTrue(stockRefusal.contains("Config conflict"), stockRefusal);
    assertFalse(stockRefusal.contains("RecordKey"), "the job's key is compared in its required form: " + stockRefusal);
    assertFalse(stockRefusal.contains(ContractTestSource.NAME), stockRefusal);

    TypedProperties same = jobProps();
    same.setProperty(RECORD_KEY, " " + REQUIRED_KEY.replace(",", " , "));
    new HoodieStreamer(config(), jsc, Option.of(same)).shutdownGracefully();
  }

  /** With metrics on, the ingestion metrics come from the write config of the merged properties, as upstream builds them. */
  @Test
  void metricsOnFromTheWriteConfig() throws Exception {
    TypedProperties props = jobProps();
    props.setProperty("hoodie.metrics.on", "true");
    props.setProperty("hoodie.metrics.reporter.type", "INMEMORY");
    try (StreamSync sync = streamSync(props)) {
      assertNotNull(sync.getMetrics());
    }
    assertEquals(REQUIRED_KEY, tableConfig().getString(HoodieTableConfig.RECORDKEY_FIELDS));
  }

  /** A job its contract refuses before the metrics are built leaves no metrics registered for its table: a retry starts clean. */
  @Test
  void refusedJobBuildsNeitherItsMetricsNorItsSource() throws Exception {
    TypedProperties props = jobProps();
    props.setProperty("hoodie.metrics.on", "true");
    props.setProperty("hoodie.metrics.reporter.type", "INMEMORY");
    props.setProperty(RECORD_KEY, "b,a");
    assertThrows(IllegalArgumentException.class, () -> streamSync(props));
    assertFalse(Metrics.isInitialized(table()), "the refused job's metrics are registered");
    assertEquals(0, ContractTestSource.BUILT.get(), "the refused job built its source");
    props.remove(RECORD_KEY);
    try (StreamSync sync = streamSync(props)) {
      assertNotNull(sync.getMetrics(), "the retry builds its metrics");
      assertEquals(1, ContractTestSource.BUILT.get());
    }
  }

  /** HoodieStreamer checks an existing table against the contract's settings first, naming the source, before StreamSync. */
  @Test
  void existingTableWithOtherKeyRefusedByTheStreamerNamingTheSource() throws Exception {
    createTableWithKey("x");
    String refusal = causes(assertThrows(Exception.class, () -> new HoodieStreamer(config(), jsc, Option.of(jobProps()))));
    assertTrue(refusal.contains(ContractTestSource.NAME + " requires the table's " + HoodieTableConfig.RECORDKEY_FIELDS.key()
        + "=" + REQUIRED_KEY), refusal);
    assertFalse(refusal.contains("Config conflict"), "the source's check runs before the stock one: " + refusal);
    assertEquals(0, ContractTestSource.BUILT.get(), "the refused job built its source");
  }

  private static String causes(Throwable failure) {
    StringBuilder messages = new StringBuilder();
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      messages.append(cause.getClass().getSimpleName()).append(": ").append(cause.getMessage()).append('\n');
    }
    return messages.toString();
  }

  /** A registered source subclass that records construction without reading source data. */
  public static class ContractTestSource extends UnstructuredDataSource {
    static final String NAME = "UnstructuredDataSource (mode blob)";
    static final AtomicReference<TypedProperties> SEEN = new AtomicReference<>();
    static final AtomicInteger BUILT = new AtomicInteger();

    public ContractTestSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
                              HoodieIngestionMetrics metrics, StreamContext streamContext) {
      super(props, sparkContext, sparkSession, metrics, streamContext);
      SEEN.set(TypedProperties.copy(props));
      BUILT.incrementAndGet();
    }

    @Override
    public Pair<Option<Dataset<Row>>, Checkpoint> fetchNextBatch(Option<Checkpoint> checkpoint, long sourceLimit) {
      return Pair.of(Option.empty(), new StreamerCheckpointV1("0"));
    }
  }
}
