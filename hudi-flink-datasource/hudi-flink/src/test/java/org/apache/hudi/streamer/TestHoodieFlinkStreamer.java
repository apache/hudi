/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.streamer;

import org.apache.hudi.client.model.HoodieFlinkInternalRow;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.configuration.OptionsInference;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.sink.transform.Transformer;
import org.apache.hudi.sink.utils.Pipelines;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.StreamerUtils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests the argument and pipeline wiring in {@link HoodieFlinkStreamer}.
 */
class TestHoodieFlinkStreamer {

  private static final String SOURCE_SCHEMA =
      "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}";

  @Test
  void testAppendPipelineWiringWithTransformer() throws Exception {
    StreamExecutionEnvironment env = mockEnvironment();
    DataStream<RowData> source = mock(DataStream.class);
    DataStream<RowData> transformed = mock(DataStream.class);
    DataStream<RowData> pipeline = mock(DataStream.class);
    Transformer transformer = mock(Transformer.class);
    when(transformer.apply(source)).thenReturn(transformed);
    AtomicReference<Configuration> envConf = new AtomicReference<>();

    // Static mocks intercept config inference and resolution so this test only exercises entry-point wiring.
    try (MockedStatic<StreamExecutionEnvironment> environments = mockStatic(StreamExecutionEnvironment.class);
         MockedStatic<StreamerUtils> streamerUtils = mockStatic(StreamerUtils.class);
         MockedStatic<StreamerUtil> streamerUtil = mockStatic(StreamerUtil.class, CALLS_REAL_METHODS);
         MockedStatic<OptionsInference> inference = mockStatic(OptionsInference.class);
         MockedStatic<OptionsResolver> resolver = mockStatic(OptionsResolver.class);
         MockedStatic<Pipelines> pipelines = mockStatic(Pipelines.class)) {
      environments.when(() -> StreamExecutionEnvironment.getExecutionEnvironment(any(Configuration.class)))
          .thenAnswer(invocation -> {
            envConf.set(invocation.getArgument(0));
            return env;
          });
      streamerUtils.when(() -> StreamerUtils.createKafkaStream(
          same(env), any(RowType.class), eq("orders"), any())).thenReturn(source);
      streamerUtil.when(() -> StreamerUtil.createTransformer(anyList())).thenReturn(Option.of(transformer));
      resolver.when(() -> OptionsResolver.isAppendMode(any())).thenReturn(true);
      resolver.when(() -> OptionsResolver.needsAsyncClustering(any())).thenReturn(true);
      pipelines.when(() -> Pipelines.append(any(), any(RowType.class), same(transformed))).thenReturn(pipeline);

      HoodieFlinkStreamer.main(args(
          "--table-type", "COPY_ON_WRITE",
          "--op", "INSERT",
          "--transformer-class", "org.example.Transformer",
          "--flink-checkpoint-path", "file:///tmp/checkpoints"));

      assertEquals("HashMapStateBackend", envConf.get().get(StateBackendOptions.STATE_BACKEND));
      assertEquals("filesystem", envConf.get().get(CheckpointingOptions.CHECKPOINT_STORAGE));
      assertEquals("file:///tmp/checkpoints",
          envConf.get().get(CheckpointingOptions.CHECKPOINTS_DIRECTORY));
      pipelines.verify(() -> Pipelines.cluster(any(), any(RowType.class), same(pipeline)));
      verify(env).execute("orders_hudi");
    }
  }

  @Test
  void testAppendPipelineFallbackWiring() throws Exception {
    StreamExecutionEnvironment env = mockEnvironment();
    DataStream<RowData> source = mock(DataStream.class);
    DataStream<RowData> pipeline = mock(DataStream.class);

    try (MockedStatic<StreamExecutionEnvironment> environments = mockStatic(StreamExecutionEnvironment.class);
         MockedStatic<StreamerUtils> streamerUtils = mockStatic(StreamerUtils.class);
         MockedStatic<OptionsInference> inference = mockStatic(OptionsInference.class);
         MockedStatic<OptionsResolver> resolver = mockStatic(OptionsResolver.class);
         MockedStatic<Pipelines> pipelines = mockStatic(Pipelines.class)) {
      environments.when(() -> StreamExecutionEnvironment.getExecutionEnvironment(any(Configuration.class)))
          .thenReturn(env);
      streamerUtils.when(() -> StreamerUtils.createKafkaStream(
          same(env), any(RowType.class), eq("orders"), any())).thenReturn(source);
      resolver.when(() -> OptionsResolver.isAppendMode(any())).thenReturn(true);
      resolver.when(() -> OptionsResolver.needsAsyncClustering(any())).thenReturn(false);
      pipelines.when(() -> Pipelines.append(any(), any(RowType.class), same(source))).thenReturn(pipeline);

      resolver.when(() -> OptionsResolver.isLazyFailedWritesCleaning(any())).thenReturn(true);
      HoodieFlinkStreamer.main(args("--table-type", "COPY_ON_WRITE", "--op", "INSERT"));
      pipelines.verify(() -> Pipelines.clean(any(), same(pipeline)));

      resolver.when(() -> OptionsResolver.isLazyFailedWritesCleaning(any())).thenReturn(false);
      HoodieFlinkStreamer.main(args("--table-type", "COPY_ON_WRITE", "--op", "INSERT"));
      pipelines.verify(() -> Pipelines.dummySink(same(pipeline)));
    }
  }

  @Test
  void testUpsertPipelineWiringWithCompaction() throws Exception {
    StreamExecutionEnvironment env = mockEnvironment();
    DataStream<RowData> source = mock(DataStream.class);
    DataStream<HoodieFlinkInternalRow> bootstrapped = mock(DataStream.class);
    DataStream<RowData> pipeline = mock(DataStream.class);

    // Static mocks intercept config inference and resolution so this test only exercises entry-point wiring.
    try (MockedStatic<StreamExecutionEnvironment> environments = mockStatic(StreamExecutionEnvironment.class);
         MockedStatic<StreamerUtils> streamerUtils = mockStatic(StreamerUtils.class);
         MockedStatic<OptionsInference> inference = mockStatic(OptionsInference.class);
         MockedStatic<OptionsResolver> resolver = mockStatic(OptionsResolver.class);
         MockedStatic<Pipelines> pipelines = mockStatic(Pipelines.class)) {
      environments.when(() -> StreamExecutionEnvironment.getExecutionEnvironment(any(Configuration.class)))
          .thenReturn(env);
      streamerUtils.when(() -> StreamerUtils.createKafkaStream(
          same(env), any(RowType.class), eq("orders"), any())).thenReturn(source);
      resolver.when(() -> OptionsResolver.isAppendMode(any())).thenReturn(false);
      resolver.when(() -> OptionsResolver.needsAsyncCompaction(any())).thenReturn(true);
      pipelines.when(() -> Pipelines.bootstrap(any(), any(RowType.class), same(source)))
          .thenReturn(bootstrapped);
      pipelines.when(() -> Pipelines.hoodieStreamWrite(any(), any(RowType.class), same(bootstrapped)))
          .thenReturn(pipeline);

      HoodieFlinkStreamer.main(args("--table-type", "MERGE_ON_READ", "--op", "UPSERT"));

      pipelines.verify(() -> Pipelines.compact(any(), same(pipeline)));
      verify(env).execute("orders_hudi");
    }
  }

  @Test
  void testUpsertPipelineFallbackWiring() throws Exception {
    StreamExecutionEnvironment env = mockEnvironment();
    DataStream<RowData> source = mock(DataStream.class);
    DataStream<HoodieFlinkInternalRow> bootstrapped = mock(DataStream.class);
    DataStream<RowData> pipeline = mock(DataStream.class);

    try (MockedStatic<StreamExecutionEnvironment> environments = mockStatic(StreamExecutionEnvironment.class);
         MockedStatic<StreamerUtils> streamerUtils = mockStatic(StreamerUtils.class);
         MockedStatic<OptionsInference> inference = mockStatic(OptionsInference.class);
         MockedStatic<OptionsResolver> resolver = mockStatic(OptionsResolver.class);
         MockedStatic<Pipelines> pipelines = mockStatic(Pipelines.class)) {
      environments.when(() -> StreamExecutionEnvironment.getExecutionEnvironment(any(Configuration.class)))
          .thenReturn(env);
      streamerUtils.when(() -> StreamerUtils.createKafkaStream(
          same(env), any(RowType.class), eq("orders"), any())).thenReturn(source);
      resolver.when(() -> OptionsResolver.isAppendMode(any())).thenReturn(false);
      resolver.when(() -> OptionsResolver.needsAsyncCompaction(any())).thenReturn(false);
      pipelines.when(() -> Pipelines.bootstrap(any(), any(RowType.class), same(source)))
          .thenReturn(bootstrapped);
      pipelines.when(() -> Pipelines.hoodieStreamWrite(any(), any(RowType.class), same(bootstrapped)))
          .thenReturn(pipeline);

      resolver.when(() -> OptionsResolver.needsAsyncCleaning(any())).thenReturn(true);
      HoodieFlinkStreamer.main(args("--table-type", "MERGE_ON_READ", "--op", "UPSERT"));
      pipelines.verify(() -> Pipelines.clean(any(), same(pipeline)));

      resolver.when(() -> OptionsResolver.needsAsyncCleaning(any())).thenReturn(false);
      HoodieFlinkStreamer.main(args("--table-type", "MERGE_ON_READ", "--op", "UPSERT"));
      pipelines.verify(() -> Pipelines.dummySink(same(pipeline)));
    }
  }

  private static StreamExecutionEnvironment mockEnvironment() {
    StreamExecutionEnvironment env = mock(StreamExecutionEnvironment.class);
    CheckpointConfig checkpointConfig = mock(CheckpointConfig.class);
    ExecutionConfig executionConfig = mock(ExecutionConfig.class);
    when(env.getCheckpointConfig()).thenReturn(checkpointConfig);
    when(env.getConfig()).thenReturn(executionConfig);
    when(checkpointConfig.getCheckpointTimeout()).thenReturn(600_000L);
    return env;
  }

  private static String[] args(String... additionalArgs) {
    String[] requiredArgs = {
        "--kafka-topic", "orders",
        "--kafka-group-id", "flink-writers",
        "--kafka-bootstrap-servers", "broker:9092",
        "--target-base-path", "file:///tmp/orders",
        "--target-table", "orders_hudi",
        "--source-avro-schema", SOURCE_SCHEMA
    };
    String[] args = new String[requiredArgs.length + additionalArgs.length];
    System.arraycopy(requiredArgs, 0, args, 0, requiredArgs.length);
    System.arraycopy(additionalArgs, 0, args, requiredArgs.length, additionalArgs.length);
    return args;
  }
}
