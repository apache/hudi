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
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.sink.partitioner.GlobalRecordIndexPartitioner;
import org.apache.hudi.utils.TestConfigurations;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.runtime.partitioner.CustomPartitionerWrapper;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
