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

import org.apache.hudi.utilities.exception.HoodieReadFromSourceException;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.model.Record;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.hudi.utilities.sources.helpers.KplTestUtils.encodeAggregatedRecord;
import static org.apache.hudi.utilities.sources.helpers.KplTestUtils.encodeSubRecord;
import static org.apache.hudi.utilities.sources.helpers.KplTestUtils.frame;
import static org.apache.hudi.utilities.sources.helpers.KplTestUtils.hexToBytes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link KinesisDeaggregator}: KPL aggregate decoding (including frozen fixtures
 * from AWS's own producer-side aggregation library), the pass-through paths taken by
 * non-aggregated records, and the failure paths taken by corrupt aggregates.
 */
class TestKinesisDeaggregator {

  private static final String PARENT_SEQ = "49590";
  private static final Instant PARENT_ARRIVAL = Instant.ofEpochMilli(1700000000000L);

  // Golden fixtures produced by AWS's producer-side aggregation library
  // (com.amazonaws:amazon-kinesis-aggregator:1.0.3, the Java implementation of the KPL wire
  // format) and reference-decoded with the KCL deaggregator (amazon-kinesis-client:1.8.8) before
  // being frozen here; both ASL-licensed libraries were used at fixture-generation time only.
  // The aggregator derives an explicit hash key for every record, so these frames also populate
  // the explicit hash key table and per-record indexes.

  // Three sub-records: ("pk-a", {"id":1}), ("pk-b" with explicit hash key
  // 170141183460469231731687303715884105727, {"id":2}), ("pk-a", {"id":3}).
  private static final String KPL_PRODUCER_FRAME = "f3899ac20a04706b2d610a04706b2d62122633373733343435363334393532"
      + "3538323733303632313239303034393331353131373531353612273137303134313138333436303436393233313733313638373330"
      + "333731353838343130353732371a0e080010001a087b226964223a317d1a0e080110011a087b226964223a327d1a0e080010001a08"
      + "7b226964223a337d7d3f7b7fcad94b07edeb92fc05a4862d";

  // Two sub-records exercising multi-byte UTF-8 (accented Latin and CJK) in both the partition
  // keys and the payloads; expected values are spelled with unicode escapes in the test below.
  private static final String KPL_PRODUCER_FRAME_UTF8 = "f3899ac20a09706b2dc3bcc3b1c3ae0a08706b2d706c61696e122732"
      + "3333393233303635323234313430363231363337363735303732353237373832373039393731122635303738313230393835353930"
      + "383337363232303036343339313438313630383232393637391a18080010001a127b2263697479223a227ac3bc72696368227d1a1d"
      + "080110011a177b2263697479223a22746f6b796f20e69db1e4baac227dafed575c3266872e207cedb9cc6f3780";

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
  void deaggregatesRealProducerLibraryFrame() {
    Record aggregate = kinesisRecord(hexToBytes(KPL_PRODUCER_FRAME));

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
  void deaggregatesRealProducerLibraryFrameWithMultiByteUtf8() {
    Record aggregate = kinesisRecord(hexToBytes(KPL_PRODUCER_FRAME_UTF8));

    List<Record> result = KinesisDeaggregator.deaggregate(Collections.singletonList(aggregate));

    assertEquals(2, result.size());
    assertEquals("pk-\u00fc\u00f1\u00ee", result.get(0).partitionKey()); // accented Latin partition key
    assertEquals("{\"city\":\"z\u00fcrich\"}", result.get(0).data().asUtf8String()); // accented Latin payload
    assertEquals("pk-plain", result.get(1).partitionKey());
    assertEquals("{\"city\":\"tokyo \u6771\u4eac\"}", result.get(1).data().asUtf8String()); // CJK payload
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
    // The magic prefix alone does not make an aggregate: an ordinary user record could start with
    // those four bytes, so a frame whose trailing digest does not verify passes through, as in KCL.
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
  void failsOnCorruptPayloadWithValidDigest() throws Exception {
    // Field 1, wire type 2, declared length 127 but only two bytes follow: valid digest, bad protobuf.
    Record corrupted = kinesisRecord(frame(new byte[] {0x0A, (byte) 0x7F, 0x01, 0x02}));

    HoodieReadFromSourceException e = assertThrows(HoodieReadFromSourceException.class,
        () -> KinesisDeaggregator.deaggregate(Collections.singletonList(corrupted)));
    assertTrue(e.getMessage().contains(PARENT_SEQ));
  }

  @Test
  void failsOnPartitionKeyIndexOutOfRange() throws Exception {
    Record aggregate = kinesisRecord(frame(encodeAggregatedRecord(
        Collections.singletonList("pk-a"), Collections.emptyList(),
        Collections.singletonList(encodeSubRecord(5, null, utf8("{\"id\":1}"), null)))));

    assertThrows(HoodieReadFromSourceException.class,
        () -> KinesisDeaggregator.deaggregate(Collections.singletonList(aggregate)));
  }

  @Test
  void failsOnExplicitHashKeyIndexOutOfRange() throws Exception {
    Record aggregate = kinesisRecord(frame(encodeAggregatedRecord(
        Collections.singletonList("pk-a"), Collections.emptyList(),
        Collections.singletonList(encodeSubRecord(0, 3L, utf8("{\"id\":1}"), null)))));

    assertThrows(HoodieReadFromSourceException.class,
        () -> KinesisDeaggregator.deaggregate(Collections.singletonList(aggregate)));
  }

  @Test
  void failsOnAggregateWithZeroSubRecords() throws Exception {
    // Digest verifies and the payload parses, but there is no records field: returning an empty
    // list would make the frame vanish silently instead of failing like the other corruption paths.
    Record aggregate = kinesisRecord(frame(encodeAggregatedRecord(
        Arrays.asList("pk-a", "pk-b"), Collections.emptyList(), Collections.emptyList())));

    assertThrows(HoodieReadFromSourceException.class,
        () -> KinesisDeaggregator.deaggregate(Collections.singletonList(aggregate)));
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
  void partiallyCorruptAggregateFailsWholeRead() throws Exception {
    // Second sub-record is out of range: KCL would emit the first and silently drop the rest,
    // while this decoder fails the read so no part of a corrupt aggregate is silently lost.
    Record aggregate = kinesisRecord(frame(encodeAggregatedRecord(
        Collections.singletonList("pk-a"), Collections.emptyList(),
        Arrays.asList(
            encodeSubRecord(0, null, utf8("{\"id\":1}"), null),
            encodeSubRecord(5, null, utf8("{\"id\":2}"), null),
            encodeSubRecord(0, null, utf8("{\"id\":3}"), null)))));

    assertThrows(HoodieReadFromSourceException.class,
        () -> KinesisDeaggregator.deaggregate(Collections.singletonList(aggregate)));
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
    Record magicOnly = kinesisRecord(KplTestUtils.KPL_MAGIC);
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
