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

package org.apache.hudi.sink.utils;

import org.apache.hudi.client.model.HoodieFlinkInternalRow;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.sink.bootstrap.TimeBoundedRLIBootstrapOperator;
import org.apache.hudi.sink.partitioner.GlobalRecordIndexPartitioner;
import org.apache.hudi.utils.TestConfigurations;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.runtime.partitioner.CustomPartitionerWrapper;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link Pipelines}.
 */
public class TestPipelines {

  @TempDir
  File tempFile;

  @Test
  void testGlobalRLIShufflesBucketAssignByGlobalRecordIndex() throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.INDEX_TYPE, HoodieIndex.IndexType.GLOBAL_RECORD_LEVEL_INDEX.name());
    conf.set(FlinkOptions.INDEX_BOOTSTRAP_ENABLED, false);
    conf.setString(HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key(), "true");
    conf.setString(HoodieMetadataConfig.STREAMING_WRITE_ENABLED.key(), "true");
    conf.set(FlinkOptions.BUCKET_ASSIGN_TASKS, 4);
    conf.set(FlinkOptions.WRITE_TASKS, 4);
    conf.set(FlinkOptions.INDEX_WRITE_TASKS, 4);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStream<HoodieFlinkInternalRow> inputStream = env.fromCollection(
        Collections.<HoodieFlinkInternalRow>emptyList(), new HoodieFlinkInternalRowTypeInfo(TestConfigurations.ROW_TYPE));
    DataStream<RowData> pipeline = Pipelines.hoodieStreamWrite(conf, TestConfigurations.ROW_TYPE, inputStream);

    assertEquals(2, countCustomPartitions(pipeline, GlobalRecordIndexPartitioner.class));
  }

  @Test
  void testBootstrapPipelineSelection() {
    Configuration conf = defaultConf();
    conf.set(FlinkOptions.INDEX_GLOBAL_ENABLED, false);
    DataStream<RowData> input = rowDataInput();

    DataStream<HoodieFlinkInternalRow> overwrite =
        Pipelines.bootstrap(conf, TestConfigurations.ROW_TYPE, input, false, true);
    assertEquals("row_data_to_hoodie_record", overwrite.getTransformation().getName());

    DataStream<HoodieFlinkInternalRow> bounded =
        Pipelines.bootstrap(conf, TestConfigurations.ROW_TYPE, input, true, false);
    assertEquals("batch_index_bootstrap", bounded.getTransformation().getName());

    conf.set(FlinkOptions.INDEX_BOOTSTRAP_ENABLED, true);
    conf.set(FlinkOptions.INDEX_BOOTSTRAP_TASKS, 3);
    DataStream<HoodieFlinkInternalRow> streaming =
        Pipelines.bootstrap(conf, TestConfigurations.ROW_TYPE, input, false, false);
    assertEquals("index_bootstrap", streaming.getTransformation().getName());
    assertEquals(3, streaming.getParallelism());
  }

  @Test
  void testPartitionedRLIWithRocksDBBackendUsesPartitionedRLIBootstrapOperator() {
    Configuration conf = defaultConf();
    conf.set(FlinkOptions.INDEX_GLOBAL_ENABLED, false);
    conf.set(FlinkOptions.INDEX_TYPE, HoodieIndex.IndexType.RECORD_LEVEL_INDEX.name());
    conf.set(FlinkOptions.INDEX_RLI_BACKEND_TYPE, "rocksdb");
    conf.set(FlinkOptions.INDEX_RLI_CACHE_ROCKSDB_BOOTSTRAP_DAYS, 1);
    conf.set(FlinkOptions.INDEX_BOOTSTRAP_ENABLED, true);
    DataStream<RowData> input = rowDataInput();

    DataStream<HoodieFlinkInternalRow> streaming =
        Pipelines.bootstrap(conf, TestConfigurations.ROW_TYPE, input, false, false);

    assertEquals("index_bootstrap", streaming.getTransformation().getName());
    assertInstanceOf(TimeBoundedRLIBootstrapOperator.class, bootstrapOperator(streaming));
  }

  private Object bootstrapOperator(DataStream<HoodieFlinkInternalRow> stream) {
    OneInputTransformation<?, ?> transformation = (OneInputTransformation<?, ?>) stream.getTransformation();
    return ((SimpleOperatorFactory<?>) transformation.getOperatorFactory()).getOperator();
  }

  @Test
  void testWritePipelineOperatorGraphs() {
    Configuration conf = defaultConf();
    conf.set(FlinkOptions.BUCKET_ASSIGN_TASKS, 2);
    conf.set(FlinkOptions.WRITE_TASKS, 3);
    DataStream<HoodieFlinkInternalRow> input = hoodieRowInput();

    DataStream<RowData> pipeline =
        Pipelines.hoodieStreamWrite(conf, TestConfigurations.ROW_TYPE, input);

    assertEquals(3, pipeline.getParallelism());
    assertTrue(transformationNames(pipeline).stream().anyMatch(name -> name.startsWith("bucket_assigner")));
    assertTrue(transformationNames(pipeline).stream().anyMatch(name -> name.startsWith("stream_write:")));
  }

  @Test
  void testSimpleBucketWritePipeline() {
    Configuration conf = defaultConf();
    conf.set(FlinkOptions.INDEX_TYPE, HoodieIndex.IndexType.BUCKET.name());
    conf.set(FlinkOptions.BUCKET_INDEX_ENGINE_TYPE,
        HoodieIndex.BucketIndexEngineType.SIMPLE.name());
    conf.set(FlinkOptions.WRITE_TASKS, 4);

    DataStream<RowData> pipeline = Pipelines.hoodieStreamWrite(
        conf, TestConfigurations.ROW_TYPE, hoodieRowInput());

    assertEquals(4, pipeline.getParallelism());
    assertTrue(pipeline.getTransformation().getName().startsWith("bucket_write:"));
  }

  @Test
  void testConsistentBucketWritePipeline() {
    Configuration conf = defaultConf();
    conf.set(FlinkOptions.INDEX_TYPE, HoodieIndex.IndexType.BUCKET.name());
    conf.set(FlinkOptions.BUCKET_INDEX_ENGINE_TYPE,
        HoodieIndex.BucketIndexEngineType.CONSISTENT_HASHING.name());
    conf.set(FlinkOptions.BUCKET_ASSIGN_TASKS, 2);
    conf.set(FlinkOptions.WRITE_TASKS, 4);

    DataStream<RowData> pipeline = Pipelines.hoodieStreamWrite(
        conf, TestConfigurations.ROW_TYPE, hoodieRowInput());

    assertEquals(4, pipeline.getParallelism());
    assertTrue(pipeline.getTransformation().getName().startsWith("consistent_bucket_write:"));
    assertTrue(transformationNames(pipeline).stream()
        .anyMatch(name -> name.startsWith("consistent_bucket_assigner:")));
  }

  @Test
  void testBulkInsertAndAppendValidation() {
    Configuration recordIndexConf = defaultConf();
    recordIndexConf.set(FlinkOptions.INDEX_TYPE, HoodieIndex.IndexType.RECORD_LEVEL_INDEX.name());
    assertThrows(HoodieException.class,
        () -> Pipelines.bulkInsert(recordIndexConf, TestConfigurations.ROW_TYPE, rowDataInput()));

    Configuration conf = defaultConf();
    conf.set(FlinkOptions.INDEX_TYPE, HoodieIndex.IndexType.BUCKET.name());
    conf.set(FlinkOptions.BUCKET_INDEX_ENGINE_TYPE,
        HoodieIndex.BucketIndexEngineType.CONSISTENT_HASHING.name());
    Configuration consistentBucketConf = conf;
    assertThrows(HoodieException.class,
        () -> Pipelines.bulkInsert(
            consistentBucketConf, TestConfigurations.ROW_TYPE, rowDataInput()));
    assertThrows(HoodieNotSupportedException.class,
        () -> Pipelines.append(
            consistentBucketConf, TestConfigurations.ROW_TYPE, rowDataInput()));
  }

  @Test
  void testBulkInsertGraphForDefaultAndLsmLayouts() {
    Configuration conf = defaultConf();
    conf.set(FlinkOptions.OPERATION, WriteOperationType.BULK_INSERT.value());
    conf.set(FlinkOptions.WRITE_TASKS, 2);
    DataStream<RowData> lsmPipeline =
        Pipelines.bulkInsert(conf, TestConfigurations.ROW_TYPE, rowDataInput());
    assertEquals(2, lsmPipeline.getParallelism());
    assertTrue(transformationNames(lsmPipeline).contains("lsm_bulk_insert_sort_keys"));
    assertTrue(transformationNames(lsmPipeline).contains("lsm_sorter:(partition_path, record_key)"));

    conf = defaultConf();
    conf.set(FlinkOptions.OPERATION, WriteOperationType.INSERT.value());
    conf.set(FlinkOptions.WRITE_TASKS, 3);
    conf.set(FlinkOptions.WRITE_BULK_INSERT_SORT_INPUT, true);
    DataStream<RowData> defaultPipeline =
        Pipelines.bulkInsert(conf, TestConfigurations.ROW_TYPE, rowDataInput());
    assertEquals(3, defaultPipeline.getParallelism());
    assertTrue(transformationNames(defaultPipeline).contains("sorter:(partition_key)"));
  }

  @Test
  void testBucketBulkInsertGraphs() {
    Configuration conf = defaultConf();
    conf.set(FlinkOptions.INDEX_TYPE, HoodieIndex.IndexType.BUCKET.name());
    conf.set(FlinkOptions.BUCKET_INDEX_ENGINE_TYPE,
        HoodieIndex.BucketIndexEngineType.SIMPLE.name());
    conf.set(FlinkOptions.OPERATION, WriteOperationType.INSERT.value());
    conf.set(FlinkOptions.WRITE_TASKS, 2);
    conf.set(FlinkOptions.WRITE_BULK_INSERT_SORT_INPUT, false);

    DataStream<RowData> unsorted =
        Pipelines.bulkInsert(conf, TestConfigurations.ROW_TYPE, rowDataInput());
    assertEquals(2, unsorted.getParallelism());
    assertTrue(unsorted.getTransformation().getName().startsWith("bucket_bulk_insert"));

    conf.set(FlinkOptions.WRITE_BULK_INSERT_SORT_INPUT, true);
    DataStream<RowData> sorted =
        Pipelines.bulkInsert(conf, TestConfigurations.ROW_TYPE, rowDataInput());
    assertTrue(transformationNames(sorted).contains("file_sorter"));

    Configuration lsmConf = defaultConf();
    lsmConf.set(FlinkOptions.INDEX_TYPE, HoodieIndex.IndexType.BUCKET.name());
    lsmConf.set(FlinkOptions.BUCKET_INDEX_ENGINE_TYPE,
        HoodieIndex.BucketIndexEngineType.SIMPLE.name());
    lsmConf.set(FlinkOptions.OPERATION, WriteOperationType.BULK_INSERT.value());
    lsmConf.set(FlinkOptions.WRITE_TASKS, 3);

    DataStream<RowData> lsm =
        Pipelines.bulkInsert(lsmConf, TestConfigurations.ROW_TYPE, rowDataInput());
    assertEquals(3, lsm.getParallelism());
    assertTrue(transformationNames(lsm).contains("lsm_bulk_insert_sort_keys"));
    assertTrue(transformationNames(lsm).contains("lsm_sorter:(file_group, record_key)"));
  }

  @Test
  void testServiceAndSinkGraphs() {
    Configuration conf = defaultConf();
    conf.set(FlinkOptions.COMPACTION_TASKS, 4);
    conf.set(FlinkOptions.CLUSTERING_TASKS, 3);
    DataStream<RowData> input = rowDataInput();

    DataStreamSink<?> compaction = Pipelines.compact(conf, input);
    assertEquals("compact_commit", compaction.getTransformation().getName());
    assertEquals(1, compaction.getTransformation().getParallelism());
    assertEquals(1, compaction.getTransformation().getMaxParallelism());

    DataStreamSink<?> clustering = Pipelines.cluster(conf, TestConfigurations.ROW_TYPE, input);
    assertEquals("clustering_commit", clustering.getTransformation().getName());
    assertEquals(1, clustering.getTransformation().getParallelism());
    assertEquals(1, clustering.getTransformation().getMaxParallelism());

    DataStreamSink<?> clean = Pipelines.clean(conf, input);
    assertEquals("clean_commits", clean.getTransformation().getName());
    assertEquals(1, clean.getTransformation().getParallelism());
    assertEquals(1, clean.getTransformation().getMaxParallelism());

    DataStreamSink<?> dummy = Pipelines.dummySink(rowDataInput(5));
    assertEquals("dummy", dummy.getTransformation().getName());
    assertEquals(5, dummy.getTransformation().getParallelism());
  }

  @Test
  void testOperatorNamesAndIndexPartitioner() {
    Configuration conf = defaultConf();
    assertEquals("write: analytics.orders", Pipelines.opName("write", conf));
    String firstUid = Pipelines.opUID("unique_test_operator", conf);
    String secondUid = Pipelines.opUID("unique_test_operator", conf);
    assertTrue(firstUid.startsWith("uid_unique_test_operator_analytics.orders"));
    assertTrue(secondUid.startsWith("uid_unique_test_operator_1_analytics.orders"));
    assertNotEquals(firstUid, secondUid);
    assertEquals(2, new Pipelines.IndexPartitioner().partition(8, 3));
  }

  private Configuration defaultConf() {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.DATABASE_NAME, "analytics");
    conf.set(FlinkOptions.TABLE_NAME, "orders");
    return conf;
  }

  private DataStream<RowData> rowDataInput() {
    return rowDataInput(1);
  }

  private DataStream<RowData> rowDataInput(int parallelism) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStreamSource<RowData> source = env.fromCollection(
        Collections.<RowData>emptyList(),
        org.apache.flink.table.runtime.typeutils.InternalTypeInfo.of(TestConfigurations.ROW_TYPE));
    if (parallelism == 1) {
      return source;
    }
    return source.map(
        row -> row,
        org.apache.flink.table.runtime.typeutils.InternalTypeInfo.of(TestConfigurations.ROW_TYPE))
        .setParallelism(parallelism);
  }

  private DataStream<HoodieFlinkInternalRow> hoodieRowInput() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    return env.fromCollection(
        Collections.<HoodieFlinkInternalRow>emptyList(),
        new HoodieFlinkInternalRowTypeInfo(TestConfigurations.ROW_TYPE));
  }

  private List<String> transformationNames(DataStream<?> stream) {
    return stream.getTransformation().getTransitivePredecessors().stream()
        .map(Transformation::getName)
        .collect(Collectors.toList());
  }

  private long countCustomPartitions(DataStream<?> stream, Class<?> partitionerClass) throws Exception {
    long count = 0;
    for (Transformation<?> transformation : stream.getTransformation().getTransitivePredecessors()) {
      if (transformation instanceof PartitionTransformation) {
        StreamPartitioner<?> partitioner = ((PartitionTransformation<?>) transformation).getPartitioner();
        if (partitioner instanceof CustomPartitionerWrapper
            && partitionerClass.isInstance(getCustomPartitioner((CustomPartitionerWrapper<?, ?>) partitioner))) {
          count++;
        }
      }
    }
    return count;
  }

  private Object getCustomPartitioner(CustomPartitionerWrapper<?, ?> partitionerWrapper) throws Exception {
    Field partitionerField = CustomPartitionerWrapper.class.getDeclaredField("partitioner");
    partitionerField.setAccessible(true);
    return partitionerField.get(partitionerWrapper);
  }
}
