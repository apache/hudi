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

import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.model.Record;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * De-aggregates KPL (Kinesis Producer Library) aggregated records into individual user records.
 * Non-aggregated records are returned unchanged.
 */
public final class KinesisDeaggregator {

  private KinesisDeaggregator() {
  }

  /**
   * De-aggregate SDK v2 Kinesis records. Aggregated records (from KPL) are split into user records.
   * Non-aggregated records pass through unchanged.
   */
  public static List<Record> deaggregate(List<Record> records) {
    if (records == null || records.isEmpty()) {
      return new ArrayList<>();
    }
    List<com.amazonaws.services.kinesis.model.Record> v1Records = new ArrayList<>(records.size());
    for (Record r : records) {
      v1Records.add(toV1Record(r));
    }
    List<UserRecord> userRecords = UserRecord.deaggregate(v1Records);
    List<Record> result = new ArrayList<>(userRecords.size());
    for (UserRecord ur : userRecords) {
      result.add(toV2Record(ur));
    }
    return result;
  }

  private static com.amazonaws.services.kinesis.model.Record toV1Record(Record v2) {
    com.amazonaws.services.kinesis.model.Record v1 = new com.amazonaws.services.kinesis.model.Record();
    v1.withData(ByteBuffer.wrap(v2.data().asByteArray()));
    v1.withPartitionKey(v2.partitionKey());
    v1.withSequenceNumber(v2.sequenceNumber());
    if (v2.approximateArrivalTimestamp() != null) {
      v1.withApproximateArrivalTimestamp(Date.from(v2.approximateArrivalTimestamp()));
    }
    return v1;
  }

  private static Record toV2Record(UserRecord v1) {
    Record.Builder builder = Record.builder()
        .data(SdkBytes.fromByteBuffer(v1.getData()))
        .partitionKey(v1.getPartitionKey())
        .sequenceNumber(v1.getSequenceNumber());
    if (v1.getApproximateArrivalTimestamp() != null) {
      builder.approximateArrivalTimestamp(v1.getApproximateArrivalTimestamp().toInstant());
    }
    return builder.build();
  }
}
