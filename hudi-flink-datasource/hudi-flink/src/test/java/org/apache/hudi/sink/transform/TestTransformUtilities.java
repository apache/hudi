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

package org.apache.hudi.sink.transform;

import org.apache.hudi.client.model.HoodieFlinkInternalRow;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.bulk.RowDataKeyGen;
import org.apache.hudi.table.action.commit.BucketInfo;
import org.apache.hudi.table.action.commit.BucketType;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestTransformUtilities {

  @Test
  void testRecordConverterBuildsFlinkRecordFromKeyAndBucket() {
    RowDataKeyGen keyGen = mock(RowDataKeyGen.class);
    RowData row = GenericRowData.of(1);
    when(keyGen.getRecordKey(row)).thenReturn("record-key");
    BucketInfo bucket = new BucketInfo(BucketType.INSERT, "file-id", "partition");

    HoodieRecord record = RecordConverter.getInstance(keyGen).convert(row, bucket);

    assertEquals("record-key", record.getRecordKey());
    assertEquals("partition", record.getPartitionPath());
    assertSame(row, record.getData());
  }

  @Test
  void testRowDataToHoodieFunctionFactoryAndMapping() throws Exception {
    RowType rowType = (RowType) DataTypes.ROW(
        DataTypes.FIELD("id", DataTypes.STRING()),
        DataTypes.FIELD("partition", DataTypes.STRING())).getLogicalType();
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.RECORD_KEY_FIELD, "id");
    conf.set(FlinkOptions.PARTITION_PATH_FIELD, "partition");

    RowDataToHoodieFunction<RowData, HoodieFlinkInternalRow> regular =
        RowDataToHoodieFunctions.create(rowType, conf);
    assertEquals(RowDataToHoodieFunction.class, regular.getClass());

    GenericRowData row = GenericRowData.of(
        StringData.fromString("id-1"), StringData.fromString("p1"));
    HoodieFlinkInternalRow result = regular.map(row);
    assertEquals("id-1", result.getRecordKey());
    assertEquals("p1", result.getPartitionPath());
    assertSame(row, result.getRowData());

    conf.set(FlinkOptions.WRITE_RATE_LIMIT, 10L);
    assertInstanceOf(RowDataToHoodieFunctionWithRateLimit.class,
        RowDataToHoodieFunctions.create(rowType, conf));
  }

  @Test
  @SuppressWarnings("unchecked")
  void testChainedTransformerAppliesInOrderAndReportsNames() {
    Transformer first = mock(Transformer.class);
    Transformer second = mock(Transformer.class);
    DataStream<RowData> source = mock(DataStream.class);
    DataStream<RowData> intermediate = mock(DataStream.class);
    DataStream<RowData> result = mock(DataStream.class);
    when(first.apply(source)).thenReturn(intermediate);
    when(second.apply(intermediate)).thenReturn(result);
    ChainedTransformer chained = new ChainedTransformer(Arrays.asList(first, second));

    assertSame(result, chained.apply(source));
    org.mockito.InOrder order = inOrder(first, second);
    order.verify(first).apply(source);
    order.verify(second).apply(intermediate);
    assertEquals(Arrays.asList(first.getClass().getName(), second.getClass().getName()),
        chained.getTransformersNames());
  }
}
