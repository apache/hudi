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
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieErrorTableConfig;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.InputBatch;
import org.apache.hudi.utilities.transform.Transformer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.stream.Stream;

import static org.apache.hudi.config.HoodieErrorTableConfig.ERROR_ENABLE_VALIDATE_TARGET_SCHEMA;
import static org.apache.hudi.utilities.streamer.HoodieStreamer.CHECKPOINT_KEY;
import static org.apache.hudi.utilities.streamer.HoodieStreamer.CHECKPOINT_RESET_KEY;
import static org.apache.hudi.utilities.streamer.StreamSync.CHECKPOINT_IGNORE_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestStreamSyncUnitTests {
  @ParameterizedTest
  @MethodSource("testCasesFetchNextBatchFromSource")
  void testFetchNextBatchFromSource(Boolean useRowWriter, Boolean hasTransformer, Boolean hasSchemaProvider,
                                    Boolean isNullTargetSchema, Boolean hasErrorTable, Boolean shouldTryWriteToErrorTable) {
    //basic deltastreamer inputs
    HoodieSparkEngineContext hoodieSparkEngineContext = mock(HoodieSparkEngineContext.class);
    HoodieStorage storage = new HoodieHadoopStorage(mock(FileSystem.class));
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
        storage, configuration, client -> true, schemaProvider, errorTableWriterOption, sourceFormatAdapter, transformerOption, useRowWriter, false);
    StreamSync spy = spy(streamSync);
    SchemaProvider deducedSchemaProvider;
    deducedSchemaProvider = getSchemaProvider("deduced", false);
    doReturn(deducedSchemaProvider).when(spy).getDeducedSchemaProvider(any(), any(), any());

    //run the method we are unit testing:
    InputBatch batch = spy.fetchNextBatchFromSource(Option.empty(), mock(HoodieTableMetaClient.class));

    //make sure getDeducedSchemaProvider is always called once
    verify(spy, times(1)).getDeducedSchemaProvider(any(), any(), any());

    //make sure the deduced schema is actually used
    assertEquals(deducedSchemaProvider.getTargetSchema(), batch.getSchemaProvider().getTargetSchema());

    //make sure we use error table when we should
    verify(propsSpy, shouldTryWriteToErrorTable ? times(1) : never())
        .getBoolean(HoodieErrorTableConfig.ERROR_ENABLE_VALIDATE_TARGET_SCHEMA.key(),
            HoodieErrorTableConfig.ERROR_ENABLE_VALIDATE_TARGET_SCHEMA.defaultValue());
  }

  @ParameterizedTest
  @MethodSource("getCheckpointToResumeCases")
  void testGetCheckpointToResume(HoodieStreamer.Config cfg, HoodieCommitMetadata commitMetadata, Option<String> expectedResumeCheckpoint) throws IOException {
    HoodieSparkEngineContext hoodieSparkEngineContext = mock(HoodieSparkEngineContext.class);
    HoodieStorage storage = new HoodieHadoopStorage(mock(FileSystem.class));
    TypedProperties props = new TypedProperties();
    SparkSession sparkSession = mock(SparkSession.class);
    Configuration configuration = mock(Configuration.class);
    HoodieTimeline commitsTimeline = mock(HoodieTimeline.class);
    HoodieInstant hoodieInstant = mock(HoodieInstant.class);

    when(commitsTimeline.filter(any())).thenReturn(commitsTimeline);
    when(commitsTimeline.lastInstant()).thenReturn(Option.of(hoodieInstant));

    StreamSync streamSync = new StreamSync(cfg, sparkSession, props, hoodieSparkEngineContext,
        storage, configuration, client -> true, null,Option.empty(),null,Option.empty(),true,true);
    StreamSync spy = spy(streamSync);
    doReturn(Option.of(commitMetadata)).when(spy).getLatestCommitMetadataWithValidCheckpointInfo(any());

    Option<String> resumeCheckpoint = spy.getCheckpointToResume(Option.of(commitsTimeline));
    assertEquals(expectedResumeCheckpoint,resumeCheckpoint);
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
    Schema sourceSchema = mock(Schema.class);
    Schema targetSchema = isNullTargetSchema ? InputBatch.NULL_SCHEMA : mock(Schema.class);
    when(schemaProvider.getSourceSchema()).thenReturn(sourceSchema);
    when(schemaProvider.getTargetSchema()).thenReturn(targetSchema);
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
}
