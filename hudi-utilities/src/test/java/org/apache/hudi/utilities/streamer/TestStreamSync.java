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

package org.apache.hudi.utilities.streamer;

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.checkpoint.Checkpoint;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.collection.Triple;
import org.apache.hudi.config.HoodieErrorTableConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.InputBatch;
import org.apache.hudi.utilities.transform.Transformer;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.checkpoint.StreamerCheckpointV2.STREAMER_CHECKPOINT_RESET_KEY_V2;
import static org.apache.hudi.config.HoodieErrorTableConfig.ERROR_ENABLE_VALIDATE_TARGET_SCHEMA;
import static org.apache.hudi.utilities.config.HoodieStreamerConfig.CHECKPOINT_FORCE_SKIP;
import static org.apache.hudi.utilities.streamer.HoodieStreamer.CHECKPOINT_KEY;
import static org.apache.hudi.utilities.streamer.HoodieStreamer.CHECKPOINT_RESET_KEY;
import static org.apache.hudi.utilities.streamer.StreamSync.CHECKPOINT_IGNORE_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestStreamSync extends SparkClientFunctionalTestHarness {

  @ParameterizedTest
  @MethodSource("testCasesFetchNextBatchFromSource")
  void testFetchNextBatchFromSource(Boolean useRowWriter, Boolean hasTransformer, Boolean hasSchemaProvider,
                                    Boolean isNullTargetSchema, Boolean hasErrorTable, Boolean shouldTryWriteToErrorTable) {
    //basic deltastreamer inputs
    HoodieSparkEngineContext hoodieSparkEngineContext = mock(HoodieSparkEngineContext.class);
    HoodieStorage storage = HoodieStorageUtils.getStorage(
            HadoopFSUtils.getStorageConf(new Configuration()),
        new Class<?>[] {FileSystem.class}, 
        mock(FileSystem.class));
    SparkSession sparkSession = mock(SparkSession.class);
    Configuration configuration = mock(Configuration.class);
    HoodieStreamer.Config cfg = new HoodieStreamer.Config();
    cfg.targetTableName = "testTableName";
    cfg.targetBasePath = "/fake/table/name";
    cfg.tableType = "MERGE_ON_READ";

    //Source format adapter
    SourceFormatAdapter sourceFormatAdapter = mock(SourceFormatAdapter.class);
    SchemaProvider inputBatchSchemaProvider = getSchemaProvider("InputBatch", false);
    Option<Dataset<Row>> fakeDataFrame = Option.of(mock(Dataset.class));
    InputBatch<Dataset<Row>> fakeRowInputBatch = new InputBatch<>(fakeDataFrame, "chkpt", inputBatchSchemaProvider);
    when(sourceFormatAdapter.fetchNewDataInRowFormat(any(), anyLong())).thenReturn(fakeRowInputBatch);
    //batch is empty because we don't want getBatch().map() to do anything because it calls static method we can't mock
    InputBatch<JavaRDD<GenericRecord>> fakeAvroInputBatch = new InputBatch<>(Option.empty(), "chkpt", inputBatchSchemaProvider);
    when(sourceFormatAdapter.fetchNewDataInAvroFormat(any(),anyLong())).thenReturn(fakeAvroInputBatch);

    //transformer
    //return empty because we don't want .map() to do anything because it calls static method we can't mock
    when(sourceFormatAdapter.processErrorEvents(any(), any())).thenReturn(Option.empty());
    Option<Transformer> transformerOption = Option.empty();
    if (hasTransformer) {
      transformerOption = Option.of(mock(Transformer.class));
    }

    //user provided schema provider
    SchemaProvider schemaProvider = null;
    if (hasSchemaProvider) {
      schemaProvider = getSchemaProvider("UserProvided", isNullTargetSchema);
    }

    //error table
    TypedProperties props = new TypedProperties();
    props.put(DataSourceWriteOptions.RECONCILE_SCHEMA().key(), false);
    Option<BaseErrorTableWriter> errorTableWriterOption = Option.empty();
    if (hasErrorTable) {
      errorTableWriterOption = Option.of(mock(BaseErrorTableWriter.class));
      props.put(ERROR_ENABLE_VALIDATE_TARGET_SCHEMA.key(), true);
    }
    TypedProperties propsSpy = spy(props);


    //Actually create the deltastreamer
    StreamSync streamSync = new StreamSync(cfg, sparkSession, propsSpy, hoodieSparkEngineContext,
        storage, configuration, client -> true, schemaProvider, errorTableWriterOption, sourceFormatAdapter, transformerOption, false);
    StreamSync spy = spy(streamSync);
    doReturn(useRowWriter).when(spy).canUseRowWriter(any());
    doReturn(useRowWriter).when(spy).isRowWriterEnabled();
    SchemaProvider deducedSchemaProvider;
    deducedSchemaProvider = getSchemaProvider("deduced", false);
    doReturn(deducedSchemaProvider).when(spy).getDeducedSchemaProvider(any(), any(), any());

    //run the method we are unit testing:
    Pair<InputBatch, Boolean> batchAndUseRowWriter = spy.fetchNextBatchFromSource(Option.empty(), mock(HoodieTableMetaClient.class));
    InputBatch batch = batchAndUseRowWriter.getLeft();

    //make sure getDeducedSchemaProvider is always called once
    verify(spy, times(1)).getDeducedSchemaProvider(any(), any(), any());

    //make sure the deduced schema is actually used
    assertEquals(deducedSchemaProvider.getTargetHoodieSchema(), batch.getSchemaProvider().getTargetHoodieSchema());

    //make sure we use error table when we should
    verify(propsSpy, shouldTryWriteToErrorTable ? times(1) : never())
        .getBoolean(HoodieErrorTableConfig.ERROR_ENABLE_VALIDATE_TARGET_SCHEMA.key(),
            HoodieErrorTableConfig.ERROR_ENABLE_VALIDATE_TARGET_SCHEMA.defaultValue());
  }

  @ParameterizedTest
  @MethodSource("getMultiTableStreamerCases")
  void testCloneConfigsFromMultiTableStreamer(HoodieMultiTableStreamer.Config cfg) throws IOException {
    Configuration configuration = new Configuration();
    JavaSparkContext jssc = mock(JavaSparkContext.class);

    when(jssc.hadoopConfiguration()).thenReturn(configuration);

    HoodieMultiTableStreamer multiTableStreamer = new HoodieMultiTableStreamer(cfg, jssc);
    List<TableExecutionContext> tableExecutionContextList = multiTableStreamer.getTableExecutionContexts();
    tableExecutionContextList.forEach(it -> {
      // make sure that the global properties are also set in each child streamer
      assertTrue(it.getConfig().configs.containsAll(cfg.configs));

      // make sure that each child streamer obtains the propsFilePath from multiStreamer configs, not the default value
      assertNotEquals(HoodieStreamer.Config.DEFAULT_DFS_SOURCE_PROPERTIES, it.getConfig().propsFilePath);
    });

    verify(jssc).hadoopConfiguration();
  }

  private static Stream<Arguments> getMultiTableStreamerCases() {
    String propFile = "src/test/resources/streamer-config/kafka-source-multi.properties";
    return Stream.of(
        Arguments.of(generateMultiTableStreamerConfig(propFile, Collections.emptyList())),
        Arguments.of(generateMultiTableStreamerConfig(propFile, Collections.singletonList("hoodie.keygen.timebased.output.dateformat=yyyyMMdd")))
    );
  }

  private static HoodieMultiTableStreamer.Config generateMultiTableStreamerConfig(String propsFilePath, List<String> configs) {
    HoodieMultiTableStreamer.Config cfg = new HoodieMultiTableStreamer.Config();
    cfg.basePathPrefix = "src/test/resources/streamer-config";
    cfg.configFolder = "src/test/resources/streamer-config";
    cfg.propsFilePath = propsFilePath;
    cfg.configs = configs;
    cfg.tableType = "MERGE_ON_READ";
    return cfg;
  }

  private static Stream<Arguments> getCheckpointToResumeCases() {
    return Stream.of(
        // Checkpoint has been manually overridden (reset-checkpoint)
        Arguments.of(generateDeltaStreamerConfig("new-reset-checkpoint",null),generateCommitMetadata("old-reset-checkpoint",null,null),Option.of("new-reset-checkpoint")),
        // Checkpoint not reset/ Ignored, continuing from previous run
        Arguments.of(generateDeltaStreamerConfig("old-reset-checkpoint",null),generateCommitMetadata("old-reset-checkpoint",null,"checkpoint-prev-run"),Option.of("checkpoint-prev-run")),
        // Checkpoint not reset/ Ignored, continuing from previous run (ignore checkpoint has not changed)
        Arguments.of(generateDeltaStreamerConfig("old-reset-checkpoint","123445"),generateCommitMetadata("old-reset-checkpoint","123445","checkpoint-prev-run"),Option.of("checkpoint-prev-run")),
        // Ignore checkpoint set, existing checkpoints will be ignored
        Arguments.of(generateDeltaStreamerConfig("old-reset-checkpoint","123445"),generateCommitMetadata("old-reset-checkpoint","123422","checkpoint-prev-run"),Option.empty()),
        // Ignore checkpoint set, existing checkpoints will be ignored (reset-checkpoint ignored)
        Arguments.of(generateDeltaStreamerConfig("new-reset-checkpoint","123445"),generateCommitMetadata("old-reset-checkpoint","123422","checkpoint-prev-run"),Option.empty())
    );
  }

  private static HoodieStreamer.Config generateDeltaStreamerConfig(String checkpoint, String ignoreCheckpoint) {
    HoodieStreamer.Config cfg = new HoodieStreamer.Config();
    cfg.checkpoint = checkpoint;
    cfg.ignoreCheckpoint = ignoreCheckpoint;
    cfg.tableType = "MERGE_ON_READ";
    return cfg;
  }

  private static HoodieCommitMetadata generateCommitMetadata(String resetCheckpointValue, String ignoreCheckpointValue, String checkpointValue) {
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    commitMetadata.addMetadata(CHECKPOINT_RESET_KEY,resetCheckpointValue);
    commitMetadata.addMetadata(CHECKPOINT_IGNORE_KEY,ignoreCheckpointValue);
    commitMetadata.addMetadata(CHECKPOINT_KEY,checkpointValue);
    return commitMetadata;
  }

  private SchemaProvider getSchemaProvider(String name, boolean isNullTargetSchema) {
    SchemaProvider schemaProvider = mock(SchemaProvider.class);
    HoodieSchema sourceSchema = mock(HoodieSchema.class);
    HoodieSchema targetSchema = isNullTargetSchema ? InputBatch.NULL_SCHEMA : mock(HoodieSchema.class);
    when(schemaProvider.getSourceHoodieSchema()).thenReturn(sourceSchema);
    when(schemaProvider.getTargetHoodieSchema()).thenReturn(targetSchema);
    when(sourceSchema.toString()).thenReturn(name + "SourceSchema");
    if (!isNullTargetSchema) {
      when(targetSchema.toString()).thenReturn(name + "TargetSchema");
    }
    return schemaProvider;
  }

  static Stream<Arguments> testCasesFetchNextBatchFromSource() {
    Stream.Builder<Arguments> b = Stream.builder();

    //no transformer
    for (Boolean useRowWriter : new Boolean[]{false, true}) {
      for (Boolean hasErrorTable : new Boolean[]{false, true}) {
        boolean errorTableEnabled = hasErrorTable && !useRowWriter;
        b.add(Arguments.of(useRowWriter, false, false, false,
            hasErrorTable, errorTableEnabled));
      }
    }

    //with transformer
    for (Boolean useRowWriter : new Boolean[]{false, true}) {
      for (Boolean hasSchemaProvider : new Boolean[]{false, true}) {
        for (Boolean isNullTargetSchema : new Boolean[]{false, true}) {
          for (Boolean hasErrorTable : new Boolean[]{false, true}) {
            boolean errorTableEnabled = hasErrorTable && !useRowWriter;
            boolean schemaProviderNullOrMissing = isNullTargetSchema || !hasSchemaProvider;
            boolean shouldTryWriteToErrorTable = errorTableEnabled && !schemaProviderNullOrMissing;
            b.add(Arguments.of(useRowWriter, true, hasSchemaProvider, isNullTargetSchema,
                hasErrorTable, shouldTryWriteToErrorTable));
          }
        }
      }
    }
    return b.build();
  }

  @Test
  public void testInitializeEmptyTable() throws IOException {
    // given
    HoodieStreamer.Config cfg = new HoodieStreamer.Config();
    cfg.targetTableName = "testTableName";
    cfg.targetBasePath = "/fake/table/name";
    cfg.tableType = "MERGE_ON_READ";
    SchemaProvider schemaProvider = getSchemaProvider("InputBatch", false);
    TypedProperties props = new TypedProperties();
    props.put(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), HoodieTableVersion.SIX.versionCode());

    // setup
    HoodieSparkEngineContext hoodieSparkEngineContext = mock(HoodieSparkEngineContext.class);
    HoodieStorage storage = HoodieStorageUtils.getStorage(
        new HadoopStorageConfiguration(new Configuration()), 
        new Class<?>[] {FileSystem.class}, 
        mock(FileSystem.class));
    SparkSession sparkSession = mock(SparkSession.class);
    Configuration configuration = mock(Configuration.class);
    SourceFormatAdapter sourceFormatAdapter = mock(SourceFormatAdapter.class);
    TypedProperties propsSpy = spy(props);
    HoodieTableMetaClient.TableBuilder tableBuilder = spy(HoodieTableMetaClient.newTableBuilder()
        .fromProperties(propsSpy));
    doReturn(null).when(tableBuilder).initTable(any(), anyString());

    StreamSync streamSync = new StreamSync(cfg, sparkSession, propsSpy, hoodieSparkEngineContext,
        storage, configuration, client -> true, schemaProvider, Option.empty(), sourceFormatAdapter, Option.empty(), false);
    StreamSync spy = spy(streamSync);

    // when
    spy.initializeEmptyTable(tableBuilder, "", null);

    // then
    verify(tableBuilder, times(1)).setTableVersion(HoodieTableVersion.SIX);
  }

  private StreamSync setupStreamSync() {
    HoodieStreamer.Config cfg = new HoodieStreamer.Config();
    cfg.checkpoint = "test-checkpoint";
    cfg.ignoreCheckpoint = "test-ignore";
    cfg.sourceClassName = "test-source";
    
    TypedProperties props = new TypedProperties();
    SchemaProvider schemaProvider = mock(SchemaProvider.class);
    
    return new StreamSync(cfg, mock(SparkSession.class), props, 
        mock(HoodieSparkEngineContext.class), mock(HoodieStorage.class), 
        mock(Configuration.class), client -> true, schemaProvider, 
        Option.empty(), mock(SourceFormatAdapter.class), Option.empty(), false);
  }

  @Test
  public void testExtractCheckpointMetadata_WhenForceSkipIsTrue() {
    StreamSync streamSync = setupStreamSync();
    HoodieStreamer.Config cfg = new HoodieStreamer.Config();
    TypedProperties props = new TypedProperties();
    props.put(CHECKPOINT_FORCE_SKIP.key(), "true");
    
    Map<String, String> result = streamSync.extractCheckpointMetadata(
        mock(InputBatch.class), props, HoodieTableVersion.ZERO.versionCode(), cfg);
        
    assertTrue(result.isEmpty(), "Should return empty map when CHECKPOINT_FORCE_SKIP is true");
  }

  @Test
  public void testExtractCheckpointMetadata_WhenCheckpointExists() {
    StreamSync streamSync = setupStreamSync();
    HoodieStreamer.Config cfg = new HoodieStreamer.Config();
    TypedProperties props = new TypedProperties();
    props.put(CHECKPOINT_FORCE_SKIP.key(), "false");
    
    InputBatch inputBatch = mock(InputBatch.class);
    Checkpoint checkpoint = mock(Checkpoint.class);
    Map<String, String> expectedMetadata = new HashMap<>();
    expectedMetadata.put("test", "value");
    
    when(inputBatch.getCheckpointForNextBatch()).thenReturn(checkpoint);
    when(checkpoint.getCheckpointCommitMetadata(cfg.checkpoint, cfg.ignoreCheckpoint))
        .thenReturn(expectedMetadata);
    
    Map<String, String> result = streamSync.extractCheckpointMetadata(
        inputBatch, props, HoodieTableVersion.ZERO.versionCode(), cfg);
        
    assertEquals(expectedMetadata, result, "Should return checkpoint metadata when checkpoint exists");
  }

  @Test
  public void testExtractCheckpointMetadata_WhenCheckpointIsNullV2() {
    StreamSync streamSync = setupStreamSync();
    HoodieStreamer.Config cfg = new HoodieStreamer.Config();
    cfg.checkpoint = "test-checkpoint";
    cfg.ignoreCheckpoint = "test-ignore";
    TypedProperties props = new TypedProperties();

    InputBatch inputBatch = mock(InputBatch.class);
    when(inputBatch.getCheckpointForNextBatch()).thenReturn(null);

    Map<String, String> result = streamSync.extractCheckpointMetadata(
        inputBatch, props, HoodieTableVersion.EIGHT.versionCode(), cfg);

    Map<String, String> expected = new HashMap<>();
    expected.put(CHECKPOINT_IGNORE_KEY, "test-ignore");
    expected.put(STREAMER_CHECKPOINT_RESET_KEY_V2, "test-checkpoint");
    assertEquals(expected, result, "Should return default metadata when checkpoint is null");
  }

  @Test
  public void testExtractCheckpointMetadata_WhenCheckpointIsNullV1() {
    StreamSync streamSync = setupStreamSync();
    HoodieStreamer.Config cfg = new HoodieStreamer.Config();
    cfg.checkpoint = "test-checkpoint";
    cfg.ignoreCheckpoint = "test-ignore";
    TypedProperties props = new TypedProperties();

    InputBatch inputBatch = mock(InputBatch.class);
    when(inputBatch.getCheckpointForNextBatch()).thenReturn(null);

    Map<String, String> result = streamSync.extractCheckpointMetadata(
        inputBatch, props, HoodieTableVersion.SIX.versionCode(), cfg);

    Map<String, String> expected = new HashMap<>();
    expected.put(CHECKPOINT_IGNORE_KEY, "test-ignore");
    expected.put(CHECKPOINT_RESET_KEY, "test-checkpoint");
    assertEquals(expected, result, "Should return default metadata when checkpoint is null");
  }

  @Nested
  class TestsWithoutSetup {
    @Test
    void testParseOverridingMergeConfigsWithEmptyConfigs() {
      List<String> configs = Collections.emptyList();
      Option<Triple<RecordMergeMode, String, String>> result = StreamSync.parseOverridingMergeConfigs(configs);
      assertTrue(result.isEmpty());
    }

    @Test
    void testParseOverridingMergeConfigsWithNullConfigs() {
      Option<Triple<RecordMergeMode, String, String>> result = StreamSync.parseOverridingMergeConfigs(null);
      assertTrue(result.isEmpty(), "Should return empty option when configs list is null");
    }

    @Test
    void testParseOverridingMergeConfigsWithNonMergeConfigs() {
      List<String> configs = Arrays.asList(
          "hoodie.datasource.write.table.name=testTable",
          "hoodie.datasource.write.operation=upsert"
      );
      Option<Triple<RecordMergeMode, String, String>> result = StreamSync.parseOverridingMergeConfigs(configs);
      assertTrue(result.isEmpty());
    }

    @Test
    void testParseOverridingMergeConfigsWithSomeMergeConfigs() {
      List<String> configs = Arrays.asList(
          "hoodie.datasource.write.table.name=testTable",
          "hoodie.datasource.write.operation=upsert",
          "hoodie.write.record.merge.mode=COMMIT_TIME_ORDERING"
      );
      Option<Triple<RecordMergeMode, String, String>> result = StreamSync.parseOverridingMergeConfigs(configs);
      assertTrue(result.isPresent());
      assertEquals(RecordMergeMode.COMMIT_TIME_ORDERING, result.get().getLeft());
      assertNull(result.get().getMiddle());
      assertNull(result.get().getRight());
    }

    @Test
    void testParseOverridingMergeConfigsWithMixedConfigs() {
      List<String> configs = Arrays.asList(
          "hoodie.datasource.write.table.name=testTable",
          "hoodie.write.record.merge.mode=COMMIT_TIME_ORDERING",
          "hoodie.datasource.write.operation=upsert",
          "hoodie.write.record.merge.strategy.id=any_id",
          "hoodie.datasource.write.payload.class=org.apache.hudi.common.model.OverwriteWithLatestPayload"
      );
      Option<Triple<RecordMergeMode, String, String>> result = StreamSync.parseOverridingMergeConfigs(configs);
      assertTrue(result.isPresent());
      Triple<RecordMergeMode, String, String> triple = result.get();
      assertEquals(RecordMergeMode.COMMIT_TIME_ORDERING, triple.getLeft());
      assertEquals("org.apache.hudi.common.model.OverwriteWithLatestPayload", triple.getMiddle());
      assertEquals("any_id", triple.getRight());
    }
  }
}
