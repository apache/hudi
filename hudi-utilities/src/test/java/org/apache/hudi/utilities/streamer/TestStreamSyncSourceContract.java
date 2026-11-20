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
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.checkpoint.Checkpoint;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.collection.Triple;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.utilities.sources.RowSource;
import org.apache.hudi.utilities.sources.SourceContract;
import org.apache.hudi.utilities.sources.SourceContracts;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;
import org.apache.hudi.utilities.transform.Transformer;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** StreamSync applies a source's contract at start, before the source is built, with a test source and a test contract. */
@Tag("functional")
class TestStreamSyncSourceContract extends UtilitiesTestBase {

  private static final String TABLE_KEY = "hoodie.test.contract.table.key";

  @TempDir
  Path tempDir;

  @BeforeAll
  static void initClass() throws Exception {
    UtilitiesTestBase.initTestServices();
  }

  @AfterAll
  static void cleanupClass() {
    UtilitiesTestBase.cleanUpUtilitiesTestServices();
  }

  @BeforeEach
  void reset() {
    RecordingSource.SEEN.set(null);
    RecordingSource.EVENTS.clear();
  }

  private HoodieStreamer.Config config() {
    HoodieStreamer.Config cfg = new HoodieStreamer.Config();
    cfg.targetBasePath = tempDir.resolve("table").toUri().toString();
    cfg.targetTableName = "contract_source";
    cfg.tableType = HoodieTableType.COPY_ON_WRITE.name();
    cfg.sourceClassName = RecordingSource.class.getName();
    cfg.transformerClassNames = Collections.singletonList(ConfiguredTransformer.class.getName());
    Triple<RecordMergeMode, String, String> merging = HoodieTableConfig.inferMergingConfigsForWrites(
        null, cfg.payloadClassName, cfg.recordMergeStrategyId, null, HoodieTableVersion.current());
    cfg.recordMergeMode = merging.getLeft();
    cfg.payloadClassName = merging.getMiddle();
    cfg.recordMergeStrategyId = merging.getRight();
    return cfg;
  }

  private static TypedProperties props() {
    TypedProperties props = new TypedProperties();
    props.setProperty("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.NonpartitionedKeyGenerator");
    props.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "id");
    return props;
  }

  private StreamSync streamSync(HoodieStreamer.Config cfg, SourceContract contract) throws Exception {
    return new StreamSync(cfg, sparkSession, props(), new HoodieSparkEngineContext(jsc), fs, jsc.hadoopConfiguration(),
        client -> true, new DefaultStreamContext(null, Option.empty()), contract);
  }

  @Test
  void contractChecksRunBeforeTheSourceIsBuiltAndTheSourceSeesTheContract() throws Exception {
    HoodieStreamer.Config cfg = config();
    RecordingContract contract = new RecordingContract(false);
    try (StreamSync ignored = streamSync(cfg, contract)) {
      assertEquals(Arrays.asList("checkTable", "checkJob", "source"), RecordingSource.EVENTS);
      assertEquals("contract_source", contract.checkedTable.get().getTableName(), "checkTable sees the table's config");
      assertEquals(Arrays.asList(ConfiguredTransformer.class.getName(), RequiredTransformer.class.getName()),
          contract.checkedChain.get(), "checkJob sees the chain the contract returns");
      assertEquals(cfg.targetBasePath, contract.checkedProps.get().getString(HoodieWriteConfig.BASE_PATH.key()));
      assertSame(contract, RecordingSource.SEEN.get().getSourceContract(), "the source is told its contract");
    }
  }

  @Test
  void newTableCarriesTheContractsTableProperties() throws Exception {
    HoodieStreamer.Config cfg = config();
    try (StreamSync ignored = streamSync(cfg, new RecordingContract(false))) {
      HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
          .setConf(HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration())).setBasePath(cfg.targetBasePath).build();
      assertEquals("from-contract", metaClient.getTableConfig().getString(TABLE_KEY));
    }
  }

  @Test
  void refusedJobNeverBuildsItsSource() {
    HoodieStreamer.Config cfg = config();
    IllegalArgumentException refused = assertThrows(IllegalArgumentException.class, () -> streamSync(cfg, new RecordingContract(true)));
    assertEquals("refused by the test contract", refused.getMessage());
    assertNull(RecordingSource.SEEN.get(), "the source is not built");
  }

  @Test
  void serviceUsesTheJobAsConfiguredWhileNoContractIsRegistered() {
    TypedProperties props = props();
    Pair<SourceContract, TypedProperties> prepared = HoodieStreamer.StreamSyncService.prepareJob(config(), props);
    assertSame(SourceContracts.NONE, prepared.getLeft());
    assertSame(props, prepared.getRight(), "the service keeps the caller's properties");
  }

  /** A test contract that records its start-time checks and requires one transformer after the configured ones. */
  static final class RecordingContract implements SourceContract {
    private final boolean refuseJob;
    final AtomicReference<HoodieTableConfig> checkedTable = new AtomicReference<>();
    final AtomicReference<List<String>> checkedChain = new AtomicReference<>();
    final AtomicReference<TypedProperties> checkedProps = new AtomicReference<>();

    RecordingContract(boolean refuseJob) {
      this.refuseJob = refuseJob;
    }

    @Override
    public TypedProperties prepareJob(TypedProperties props) {
      return props;
    }

    @Override
    public Properties tableProperties() {
      Properties properties = new Properties();
      properties.setProperty(TABLE_KEY, "from-contract");
      return properties;
    }

    @Override
    public List<String> transformerChain(List<String> configured) {
      List<String> chain = new ArrayList<>(configured);
      chain.add(RequiredTransformer.class.getName());
      return chain;
    }

    @Override
    public void checkTable(String basePath, HoodieTableConfig tableConfig) {
      RecordingSource.EVENTS.add("checkTable");
      checkedTable.set(tableConfig);
    }

    @Override
    public void checkJob(JavaSparkContext jsc, SparkSession spark, List<String> chain, TypedProperties props,
                         Option<HoodieSchema> targetSchema, Option<StructType> tableSchema) {
      RecordingSource.EVENTS.add("checkJob");
      checkedChain.set(chain);
      checkedProps.set(props);
      if (refuseJob) {
        throw new IllegalArgumentException("refused by the test contract");
      }
    }
  }

  /** A transformer the job configures. */
  public static class ConfiguredTransformer implements Transformer {
    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties) {
      return rowDataset;
    }
  }

  /** A transformer the test contract requires. */
  public static class RequiredTransformer extends ConfiguredTransformer {
  }

  /** A row source that records the stream context it is built with. */
  public static class RecordingSource extends RowSource {
    static final AtomicReference<StreamContext> SEEN = new AtomicReference<>();
    static final List<String> EVENTS = Collections.synchronizedList(new ArrayList<>());

    public RecordingSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
                           StreamContext streamContext) {
      super(props, sparkContext, sparkSession, streamContext);
      EVENTS.add("source");
      SEEN.set(streamContext);
    }

    @Override
    protected Pair<Option<Dataset<Row>>, Checkpoint> fetchNextBatch(Option<Checkpoint> lastCheckpoint, long sourceLimit) {
      return Pair.of(Option.empty(), null);
    }
  }
}
