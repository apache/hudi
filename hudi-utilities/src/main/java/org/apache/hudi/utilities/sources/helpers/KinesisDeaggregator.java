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

import com.google.protobuf.CodedInputStream;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.model.Record;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

/**
 * De-aggregates KPL (Kinesis Producer Library) aggregated records into individual user records.
 * Non-aggregated records are returned unchanged.
 *
 * <p>The aggregated record format (a 4-byte magic prefix, a protobuf payload and a trailing MD5
 * digest of that payload) is documented by the KPL and is decoded directly here. This avoids a
 * runtime dependency on the KCL de-aggregation library, which is published under the Amazon
 * Software License and therefore cannot be a required dependency of an Apache project.
 *
 * <p>Semantics match the KCL deaggregator except that where KCL silently drops records of a corrupt
 * aggregate (valid digest, out-of-range key index), this implementation passes the whole raw record
 * through unchanged so corruption fails loudly downstream instead of losing data.
 */
@Slf4j
public final class KinesisDeaggregator {

  private static final byte[] MAGIC = new byte[] {(byte) 0xF3, (byte) 0x89, (byte) 0x9A, (byte) 0xC2};
  private static final int DIGEST_LENGTH = 16;

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
    List<Record> result = new ArrayList<>(records.size());
    for (Record record : records) {
      byte[] data = record.data() == null ? null : record.data().asByteArray();
      if (!isAggregated(data)) {
        result.add(record);
        continue;
      }
      int payloadLength = data.length - MAGIC.length - DIGEST_LENGTH;
      try {
        result.addAll(expand(record, data, MAGIC.length, payloadLength));
      } catch (IOException e) {
        log.warn("Kinesis record with sequence number {} has a matching KPL aggregation digest but could not be "
            + "decoded, passing it through as-is", record.sequenceNumber(), e);
        result.add(record);
      }
    }
    return result;
  }

  private static boolean isAggregated(byte[] data) {
    // Strictly greater: a frame with an empty payload is not an aggregate, matching KCL.
    if (data == null || data.length <= MAGIC.length + DIGEST_LENGTH) {
      return false;
    }
    for (int i = 0; i < MAGIC.length; i++) {
      if (data[i] != MAGIC[i]) {
        return false;
      }
    }
    byte[] expectedDigest = new byte[DIGEST_LENGTH];
    System.arraycopy(data, data.length - DIGEST_LENGTH, expectedDigest, 0, DIGEST_LENGTH);
    byte[] actualDigest = md5(data, MAGIC.length, data.length - MAGIC.length - DIGEST_LENGTH);
    return MessageDigest.isEqual(actualDigest, expectedDigest);
  }

  private static byte[] md5(byte[] data, int offset, int length) {
    try {
      MessageDigest digest = MessageDigest.getInstance("MD5");
      digest.update(data, offset, length);
      return digest.digest();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("MD5 is not available in this JVM", e);
    }
  }

  /**
   * Parses the {@code AggregatedRecord} message: repeated string partition_key_table (field 1),
   * repeated string explicit_hash_key_table (field 2) and repeated Record records (field 3).
   */
  private static List<Record> expand(Record parent, byte[] data, int offset, int length) throws IOException {
    CodedInputStream input = CodedInputStream.newInstance(data, offset, length);
    List<String> partitionKeyTable = new ArrayList<>();
    List<String> explicitHashKeyTable = new ArrayList<>();
    List<byte[]> subMessages = new ArrayList<>();
    while (!input.isAtEnd()) {
      int tag = input.readTag();
      switch (tag) {
        case 10:
          partitionKeyTable.add(input.readStringRequireUtf8());
          break;
        case 18:
          explicitHashKeyTable.add(input.readStringRequireUtf8());
          break;
        case 26:
          subMessages.add(input.readByteArray());
          break;
        default:
          input.skipField(tag);
          break;
      }
    }
    List<Record> expanded = new ArrayList<>(subMessages.size());
    for (byte[] subMessage : subMessages) {
      expanded.add(toRecord(parent, subMessage, partitionKeyTable, explicitHashKeyTable));
    }
    return expanded;
  }

  /**
   * Parses one nested {@code Record} message: required uint64 partition_key_index (field 1),
   * optional uint64 explicit_hash_key_index (field 2), required bytes data (field 3) and repeated
   * Tag tags (field 4, skipped). Explicit hash keys and tags are validated but not propagated.
   */
  private static Record toRecord(Record parent, byte[] subMessage, List<String> partitionKeyTable,
                                 List<String> explicitHashKeyTable) throws IOException {
    CodedInputStream input = CodedInputStream.newInstance(subMessage);
    long partitionKeyIndex = 0;
    boolean hasPartitionKeyIndex = false;
    long explicitHashKeyIndex = 0;
    boolean hasExplicitHashKeyIndex = false;
    byte[] payload = null;
    while (!input.isAtEnd()) {
      int tag = input.readTag();
      switch (tag) {
        case 8:
          partitionKeyIndex = input.readUInt64();
          hasPartitionKeyIndex = true;
          break;
        case 16:
          explicitHashKeyIndex = input.readUInt64();
          hasExplicitHashKeyIndex = true;
          break;
        case 26:
          payload = input.readByteArray();
          break;
        default:
          input.skipField(tag);
          break;
      }
    }
    if (!hasPartitionKeyIndex) {
      throw new IOException("KPL sub-record is missing the required partition key index");
    }
    if (partitionKeyIndex < 0 || partitionKeyIndex >= partitionKeyTable.size()) {
      throw new IOException("KPL sub-record partition key index " + partitionKeyIndex
          + " is out of range for a table of " + partitionKeyTable.size() + " keys");
    }
    if (hasExplicitHashKeyIndex
        && (explicitHashKeyIndex < 0 || explicitHashKeyIndex >= explicitHashKeyTable.size())) {
      throw new IOException("KPL sub-record explicit hash key index " + explicitHashKeyIndex
          + " is out of range for a table of " + explicitHashKeyTable.size() + " keys");
    }
    if (payload == null) {
      throw new IOException("KPL sub-record is missing the required data field");
    }
    Record.Builder builder = Record.builder()
        .data(SdkBytes.fromByteArray(payload))
        .partitionKey(partitionKeyTable.get((int) partitionKeyIndex))
        .sequenceNumber(parent.sequenceNumber());
    if (parent.approximateArrivalTimestamp() != null) {
      builder.approximateArrivalTimestamp(parent.approximateArrivalTimestamp());
    }
    return builder.build();
  }
}
