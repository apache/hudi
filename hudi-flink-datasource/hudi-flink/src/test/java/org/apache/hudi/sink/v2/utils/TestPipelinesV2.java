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

package org.apache.hudi.sink.v2.utils;

import org.apache.hudi.client.model.HoodieFlinkInternalRow;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.sink.utils.Pipelines;
import org.apache.hudi.utils.TestConfigurations;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.runtime.partitioner.CustomPartitionerWrapper;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

/** Tests topology construction and routing in {@link PipelinesV2}. */
class TestPipelinesV2 {

  private Configuration conf;
  private DataStream<RowData> input;

  @BeforeEach
  void setUp() {
    conf = new Configuration();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(7);
    input = env.fromCollection(
        Collections.<RowData>emptyList(),
        TypeInformation.of(RowData.class));
  }

  @Test
  void testSinkUsesModeSpecificParallelismAndStableIdentity() {
    conf.set(FlinkOptions.OPERATION, "bulk_insert");
    conf.set(FlinkOptions.TABLE_NAME, "sink_v2_test");
    conf.set(FlinkOptions.WRITE_TASKS, 4);

    DataStreamSink<RowData> sink = PipelinesV2.sink(
        input, conf, TestConfigurations.ROW_TYPE, false, true);

    assertEquals(4, sink.getTransformation().getParallelism());
    assertEquals("sink_v2", sink.getTransformation().getName());
    assertTrue(sink.getTransformation().getUid()
        .matches("uid_sink_v2(?:_\\d+)?_sink_v2_test"));
  }

  @Test
  void testServiceTopologiesUseExpectedSingletonOperatorsAndPartitioners() throws Exception {
    conf.set(FlinkOptions.CLUSTERING_TASKS, 3);
    conf.set(FlinkOptions.COMPACTION_TASKS, 2);

    DataStream<RowData> clean = PipelinesV2.cleanV2(conf, input);
    DataStream<RowData> cluster = PipelinesV2.clusterV2(
        conf, TestConfigurations.ROW_TYPE, input);
    DataStream<RowData> compact = PipelinesV2.compactV2(conf, input);

    assertOperator(clean, "clean_commits", 1, 1);
    assertOperator(cluster, "cluster_plan_generate", 1, 1);
    assertOperator(cluster, "clustering_commit", 1, 1);
    assertOperator(compact, "compact_plan_generate", 1, 1);
    assertOperator(compact, "compact_commit", 1, 1);
    assertEquals(3, transformation(cluster, "clustering_task").getParallelism());
    assertEquals(2, transformation(compact, "compact_task").getParallelism());
    assertEquals(1, countCustomPartitions(cluster, Pipelines.IndexPartitioner.class));
    assertEquals(1, countCustomPartitions(compact, Pipelines.IndexPartitioner.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  void testComposePipelineRoutesEveryWriteMode() {
    DataStream<HoodieFlinkInternalRow> bootstrapped = mock(DataStream.class);
    Configuration bulk = new Configuration();
    Configuration append = new Configuration();
    Configuration appendCluster = new Configuration();
    Configuration appendClean = new Configuration();
    Configuration compact = new Configuration();
    Configuration clean = new Configuration();
    Configuration plain = new Configuration();
    bulk.set(FlinkOptions.PATH, "bulk");
    append.set(FlinkOptions.PATH, "append");
    appendCluster.set(FlinkOptions.PATH, "append-cluster");
    appendClean.set(FlinkOptions.PATH, "append-clean");
    compact.set(FlinkOptions.PATH, "compact");
    clean.set(FlinkOptions.PATH, "clean");
    plain.set(FlinkOptions.PATH, "plain");
    appendCluster.set(FlinkOptions.CLUSTERING_TASKS, 2);
    compact.set(FlinkOptions.COMPACTION_TASKS, 2);

    try (MockedStatic<OptionsResolver> options = mockStatic(OptionsResolver.class);
         MockedStatic<Pipelines> pipelines = mockStatic(Pipelines.class)) {
      options.when(() -> OptionsResolver.isBulkInsertOperation(bulk)).thenReturn(true);
      pipelines.when(() -> Pipelines.bulkInsert(bulk, TestConfigurations.ROW_TYPE, input))
          .thenReturn(input);
      assertSame(input, PipelinesV2.composePipeline(
          input, bulk, TestConfigurations.ROW_TYPE, false, true));
      assertThrows(HoodieException.class, () -> PipelinesV2.composePipeline(
          input, bulk, TestConfigurations.ROW_TYPE, false, false));

      stubAppend(options, pipelines, append);
      assertSame(input, PipelinesV2.composePipeline(
          input, append, TestConfigurations.ROW_TYPE, false, false));
      assertFalse(append.get(FlinkOptions.COMPACTION_SCHEDULE_ENABLED));

      stubAppend(options, pipelines, appendCluster);
      options.when(() -> OptionsResolver.needsAsyncClustering(appendCluster)).thenReturn(true);
      assertOperator(PipelinesV2.composePipeline(
          input, appendCluster, TestConfigurations.ROW_TYPE, false, false),
          "clustering_commit", 1, 1);

      stubAppend(options, pipelines, appendClean);
      options.when(() -> OptionsResolver.isLazyFailedWritesCleaning(appendClean)).thenReturn(true);
      assertOperator(PipelinesV2.composePipeline(
          input, appendClean, TestConfigurations.ROW_TYPE, false, false),
          "clean_commits", 1, 1);

      stubRegular(pipelines, compact, bootstrapped, true);
      options.when(() -> OptionsResolver.needsAsyncCompaction(compact)).thenReturn(true);
      assertOperator(PipelinesV2.composePipeline(
          input, compact, TestConfigurations.ROW_TYPE, false, true),
          "compact_commit", 1, 1);
      assertFalse(compact.get(FlinkOptions.COMPACTION_OPERATION_EXECUTE_ASYNC_ENABLED));

      stubRegular(pipelines, clean, bootstrapped, false);
      options.when(() -> OptionsResolver.needsAsyncCleaning(clean)).thenReturn(true);
      assertOperator(PipelinesV2.composePipeline(
          input, clean, TestConfigurations.ROW_TYPE, false, false),
          "clean_commits", 1, 1);

      stubRegular(pipelines, plain, bootstrapped, false);
      assertSame(input, PipelinesV2.composePipeline(
          input, plain, TestConfigurations.ROW_TYPE, false, false));
    }
  }

  private void stubAppend(
      MockedStatic<OptionsResolver> options,
      MockedStatic<Pipelines> pipelines,
      Configuration configuration) {
    options.when(() -> OptionsResolver.isAppendMode(configuration)).thenReturn(true);
    pipelines.when(() -> Pipelines.append(
        configuration, TestConfigurations.ROW_TYPE, input)).thenReturn(input);
  }

  private void stubRegular(
      MockedStatic<Pipelines> pipelines,
      Configuration configuration,
      DataStream<HoodieFlinkInternalRow> bootstrapped,
      boolean isBounded) {
    pipelines.when(() -> Pipelines.bootstrap(
        configuration, TestConfigurations.ROW_TYPE, input, isBounded, false))
        .thenReturn(bootstrapped);
    pipelines.when(() -> Pipelines.hoodieStreamWrite(
        configuration, TestConfigurations.ROW_TYPE, bootstrapped)).thenReturn(input);
  }

  private void assertOperator(
      DataStream<?> stream,
      String name,
      int parallelism,
      int maxParallelism) {
    Transformation<?> transformation = transformation(stream, name);
    assertEquals(parallelism, transformation.getParallelism());
    assertEquals(maxParallelism, transformation.getMaxParallelism());
  }

  private Transformation<?> transformation(DataStream<?> stream, String name) {
    List<Transformation<?>> matches = stream.getTransformation().getTransitivePredecessors()
        .stream()
        .filter(candidate -> name.equals(candidate.getName()))
        .collect(Collectors.toList());
    assertEquals(1, matches.size(), "Expected one transformation named " + name);
    return matches.get(0);
  }

  private long countCustomPartitions(DataStream<?> stream, Class<?> partitionerClass)
      throws Exception {
    long count = 0;
    for (Transformation<?> transformation : stream.getTransformation().getTransitivePredecessors()) {
      if (transformation instanceof PartitionTransformation) {
        StreamPartitioner<?> partitioner =
            ((PartitionTransformation<?>) transformation).getPartitioner();
        if (partitioner instanceof CustomPartitionerWrapper
            && partitionerClass.isInstance(
                getCustomPartitioner((CustomPartitionerWrapper<?, ?>) partitioner))) {
          count++;
        }
      }
    }
    return count;
  }

  private Object getCustomPartitioner(CustomPartitionerWrapper<?, ?> partitionerWrapper)
      throws Exception {
    Field partitionerField = CustomPartitionerWrapper.class.getDeclaredField("partitioner");
    partitionerField.setAccessible(true);
    return partitionerField.get(partitionerWrapper);
  }
}
