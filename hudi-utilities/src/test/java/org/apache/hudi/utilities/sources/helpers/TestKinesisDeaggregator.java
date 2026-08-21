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

import com.google.protobuf.CodedOutputStream;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.model.Record;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link KinesisDeaggregator}, covering KPL aggregate decoding and the pass-through
 * paths taken by non-aggregated, corrupt or invalid records.
 */
class TestKinesisDeaggregator {

  private static final byte[] MAGIC = new byte[] {(byte) 0xF3, (byte) 0x89, (byte) 0x9A, (byte) 0xC2};
  private static final String PARENT_SEQ = "49590";
  private static final Instant PARENT_ARRIVAL = Instant.ofEpochMilli(1700000000000L);

  // -------------------------------------------------------------------------
  // Encoding helpers - independent of the decoder under test
  // -------------------------------------------------------------------------

  private static byte[] encodeSubRecord(long pkIndex, Long ehkIndex, byte[] data, String tagKeyOrNull)
      throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    CodedOutputStream stream = CodedOutputStream.newInstance(out);
    stream.writeUInt64(1, pkIndex);
    if (ehkIndex != null) {
      stream.writeUInt64(2, ehkIndex);
    }
    stream.writeByteArray(3, data);
    if (tagKeyOrNull != null) {
      ByteArrayOutputStream tagOut = new ByteArrayOutputStream();
      CodedOutputStream tagStream = CodedOutputStream.newInstance(tagOut);
      tagStream.writeString(1, tagKeyOrNull);
      tagStream.flush();
      stream.writeByteArray(4, tagOut.toByteArray());
    }
    stream.flush();
    return out.toByteArray();
  }

  private static byte[] encodeAggregatedRecord(List<String> pkTable, List<String> ehkTable,
      List<byte[]> subRecords) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    CodedOutputStream stream = CodedOutputStream.newInstance(out);
    for (String pk : pkTable) {
      stream.writeString(1, pk);
    }
    for (String ehk : ehkTable) {
      stream.writeString(2, ehk);
    }
    for (byte[] subRecord : subRecords) {
      stream.writeByteArray(3, subRecord);
    }
    stream.flush();
    return out.toByteArray();
  }

  private static byte[] frame(byte[] payload) throws NoSuchAlgorithmException {
    byte[] digest = MessageDigest.getInstance("MD5").digest(payload);
    return ByteBuffer.allocate(MAGIC.length + payload.length + digest.length)
        .put(MAGIC).put(payload).put(digest).array();
  }

  private static Record kinesisRecord(byte[] data) {
    return Record.builder()
        .data(SdkBytes.fromByteArray(data))
        .partitionKey("parent-pk")
        .sequenceNumber(PARENT_SEQ)
        .approximateArrivalTimestamp(PARENT_ARRIVAL)
        .build();
  }

  private static byte[] utf8(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  // -------------------------------------------------------------------------
  // Tests
  // -------------------------------------------------------------------------

  @Test
  void deaggregatesAggregatedRecord() throws Exception {
    List<byte[]> subRecords = Arrays.asList(
        encodeSubRecord(0, null, utf8("{\"id\":1}"), null),
        encodeSubRecord(1, null, utf8("{\"id\":2}"), null),
        encodeSubRecord(0, null, utf8("{\"id\":3}"), null));
    Record aggregate = kinesisRecord(frame(encodeAggregatedRecord(
        Arrays.asList("pk-a", "pk-b"), Collections.emptyList(), subRecords)));

    List<Record> result = KinesisDeaggregator.deaggregate(Collections.singletonList(aggregate));

    assertEquals(3, result.size());
    assertEquals("{\"id\":1}", result.get(0).data().asUtf8String());
    assertEquals("{\"id\":2}", result.get(1).data().asUtf8String());
    assertEquals("{\"id\":3}", result.get(2).data().asUtf8String());
    assertEquals("pk-a", result.get(0).partitionKey());
    assertEquals("pk-b", result.get(1).partitionKey());
    assertEquals("pk-a", result.get(2).partitionKey());
    for (Record record : result) {
      assertEquals(PARENT_SEQ, record.sequenceNumber());
      assertEquals(PARENT_ARRIVAL, record.approximateArrivalTimestamp());
    }
  }

  @Test
  void passesThroughNonAggregatedRecordUnchanged() {
    Record plain = kinesisRecord(utf8("{\"id\":1,\"name\":\"plain\"}"));

    List<Record> result = KinesisDeaggregator.deaggregate(Collections.singletonList(plain));

    assertEquals(1, result.size());
    assertSame(plain, result.get(0));
  }

  @Test
  void passesThroughOnDigestMismatch() throws Exception {
    byte[] framed = frame(encodeAggregatedRecord(
        Collections.singletonList("pk-a"), Collections.emptyList(),
        Collections.singletonList(encodeSubRecord(0, null, utf8("{\"id\":1}"), null))));
    framed[framed.length - 1] ^= 0x01;
    Record corrupted = kinesisRecord(framed);

    List<Record> result = KinesisDeaggregator.deaggregate(Collections.singletonList(corrupted));

    assertEquals(1, result.size());
    assertSame(corrupted, result.get(0));
  }

  @Test
  void passesThroughOnCorruptPayload() throws Exception {
    // Field 1, wire type 2, declared length 127 but only two bytes follow: valid digest, bad protobuf.
    Record corrupted = kinesisRecord(frame(new byte[] {0x0A, (byte) 0x7F, 0x01, 0x02}));

    List<Record> result = KinesisDeaggregator.deaggregate(Collections.singletonList(corrupted));

    assertEquals(1, result.size());
    assertSame(corrupted, result.get(0));
  }

  @Test
  void passesThroughOnPartitionKeyIndexOutOfRange() throws Exception {
    Record aggregate = kinesisRecord(frame(encodeAggregatedRecord(
        Collections.singletonList("pk-a"), Collections.emptyList(),
        Collections.singletonList(encodeSubRecord(5, null, utf8("{\"id\":1}"), null)))));

    List<Record> result = KinesisDeaggregator.deaggregate(Collections.singletonList(aggregate));

    assertEquals(1, result.size());
    assertSame(aggregate, result.get(0));
  }

  @Test
  void passesThroughOnExplicitHashKeyIndexOutOfRange() throws Exception {
    Record aggregate = kinesisRecord(frame(encodeAggregatedRecord(
        Collections.singletonList("pk-a"), Collections.emptyList(),
        Collections.singletonList(encodeSubRecord(0, 3L, utf8("{\"id\":1}"), null)))));

    List<Record> result = KinesisDeaggregator.deaggregate(Collections.singletonList(aggregate));

    assertEquals(1, result.size());
    assertSame(aggregate, result.get(0));
  }

  @Test
  void decodesRecordWithExplicitHashKeysAndTags() throws Exception {
    Record aggregate = kinesisRecord(frame(encodeAggregatedRecord(
        Collections.singletonList("pk-a"),
        Collections.singletonList("170141183460469231731687303715884105727"),
        Collections.singletonList(encodeSubRecord(0, 0L, utf8("{\"id\":1}"), "tag-key")))));

    List<Record> result = KinesisDeaggregator.deaggregate(Collections.singletonList(aggregate));

    assertEquals(1, result.size());
    assertEquals("{\"id\":1}", result.get(0).data().asUtf8String());
    assertEquals("pk-a", result.get(0).partitionKey());
  }

  @Test
  void partialCorruptAggregatePassesThroughWhole() throws Exception {
    // Second sub-record is out of range: KCL would emit the first and silently drop the rest.
    Record aggregate = kinesisRecord(frame(encodeAggregatedRecord(
        Collections.singletonList("pk-a"), Collections.emptyList(),
        Arrays.asList(
            encodeSubRecord(0, null, utf8("{\"id\":1}"), null),
            encodeSubRecord(5, null, utf8("{\"id\":2}"), null),
            encodeSubRecord(0, null, utf8("{\"id\":3}"), null)))));

    List<Record> result = KinesisDeaggregator.deaggregate(Collections.singletonList(aggregate));

    assertEquals(1, result.size());
    assertSame(aggregate, result.get(0));
  }

  @Test
  void emptyPayloadWithValidDigestPassesThrough() throws Exception {
    Record emptyPayload = kinesisRecord(frame(new byte[0]));

    List<Record> result = KinesisDeaggregator.deaggregate(Collections.singletonList(emptyPayload));

    assertEquals(1, result.size());
    assertSame(emptyPayload, result.get(0));
  }

  @Test
  void tooShortDataPassesThrough() {
    Record magicOnly = kinesisRecord(MAGIC);
    Record tooShort = kinesisRecord(new byte[] {(byte) 0xF3, (byte) 0x89});

    List<Record> result = KinesisDeaggregator.deaggregate(Arrays.asList(magicOnly, tooShort));

    assertEquals(2, result.size());
    assertSame(magicOnly, result.get(0));
    assertSame(tooShort, result.get(1));
  }

  @Test
  void mixedBatchPreservesOrder() throws Exception {
    Record first = kinesisRecord(utf8("{\"id\":\"first\"}"));
    Record aggregate = kinesisRecord(frame(encodeAggregatedRecord(
        Arrays.asList("pk-a", "pk-b"), Collections.emptyList(),
        Arrays.asList(
            encodeSubRecord(0, null, utf8("{\"id\":\"sub1\"}"), null),
            encodeSubRecord(1, null, utf8("{\"id\":\"sub2\"}"), null)))));
    Record last = kinesisRecord(utf8("{\"id\":\"last\"}"));

    List<Record> result = KinesisDeaggregator.deaggregate(Arrays.asList(first, aggregate, last));

    assertEquals(4, result.size());
    assertSame(first, result.get(0));
    assertEquals("{\"id\":\"sub1\"}", result.get(1).data().asUtf8String());
    assertEquals("pk-a", result.get(1).partitionKey());
    assertEquals("{\"id\":\"sub2\"}", result.get(2).data().asUtf8String());
    assertEquals("pk-b", result.get(2).partitionKey());
    assertSame(last, result.get(3));
  }

  @Test
  void emptyAndNullInputReturnEmptyList() {
    assertTrue(KinesisDeaggregator.deaggregate(Collections.emptyList()).isEmpty());
    assertTrue(KinesisDeaggregator.deaggregate(null).isEmpty());
  }
}
