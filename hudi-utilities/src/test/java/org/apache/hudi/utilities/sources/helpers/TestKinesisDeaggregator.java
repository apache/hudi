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

package org.apache.hudi.utilities.sources.helpers;

import com.amazonaws.kinesis.agg.AggRecord;
import com.amazonaws.kinesis.agg.RecordAggregator;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.model.Record;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for Kinesis KPL de-aggregation.
 */
class TestKinesisDeaggregator {

  @Test
  void testNonAggregatedRecordPassesThrough() {
    String json = "{\"id\":1,\"name\":\"alice\"}";
    Record r = Record.builder()
        .data(SdkBytes.fromUtf8String(json))
        .partitionKey("pk1")
        .sequenceNumber("49590382471490958861609854428592832524486083118")
        .approximateArrivalTimestamp(Instant.now())
        .build();
    List<Record> input = List.of(r);
    List<Record> result = KinesisDeaggregator.deaggregate(input);
    assertEquals(1, result.size());
    assertEquals(json, result.get(0).data().asUtf8String());
    assertEquals("pk1", result.get(0).partitionKey());
  }

  @Test
  void testEmptyListReturnsEmpty() {
    List<Record> result = KinesisDeaggregator.deaggregate(new ArrayList<>());
    assertTrue(result.isEmpty());
  }

  @Test
  void testNullReturnsEmpty() {
    List<Record> result = KinesisDeaggregator.deaggregate(null);
    assertTrue(result.isEmpty());
  }

  @Test
  void testAggregatedRecordsDeaggregatedCorrectly() throws Exception {
    // Create aggregated record using RecordAggregator (same format as KPL)
    RecordAggregator aggregator = new RecordAggregator();
    String[] userRecords = new String[]{
        "{\"id\":1,\"name\":\"alice\"}",
        "{\"id\":2,\"name\":\"bob\"}",
        "{\"id\":3,\"name\":\"carol\"}"
    };
    for (String data : userRecords) {
      aggregator.addUserRecord("pk-" + data, null, data.getBytes("UTF-8"));
    }
    AggRecord aggRecord = aggregator.clearAndGet();
    assertNotNull(aggRecord);
    assertTrue(aggRecord.getNumUserRecords() >= 1);

    // Convert AggRecord to SDK v2 Record (simulating GetRecords response)
    com.amazonaws.services.kinesis.model.PutRecordRequest v1Request = aggRecord.toPutRecordRequest("test-stream");
    ByteBuffer dataBuffer = v1Request.getData();
    byte[] dataBytes = new byte[dataBuffer.remaining()];
    dataBuffer.get(dataBytes);

    Record v2Record = Record.builder()
        .data(SdkBytes.fromByteArray(dataBytes))
        .partitionKey(v1Request.getPartitionKey())
        .sequenceNumber("49590382471490958861609854428592832524486083118")
        .approximateArrivalTimestamp(Instant.now())
        .build();

    List<Record> input = List.of(v2Record);
    List<Record> result = KinesisDeaggregator.deaggregate(input);

    assertEquals(aggRecord.getNumUserRecords(), result.size());
    for (int i = 0; i < result.size(); i++) {
      String data = result.get(i).data().asUtf8String();
      assertTrue(data.contains("\"id\":") || data.contains("name"));
    }
  }
}
