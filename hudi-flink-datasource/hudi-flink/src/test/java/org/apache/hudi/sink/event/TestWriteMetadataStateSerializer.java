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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.sink.event;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieValueTypeInfo;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.metadata.HoodieIndexVersion;
import org.apache.hudi.stats.HoodieColumnRangeMetadata;
import org.apache.hudi.stats.ValueMetadata;
import org.apache.hudi.stats.ValueType;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link WriteMetadataStateSerializer}. */
public class TestWriteMetadataStateSerializer {

  @Test
  void testRoundTripAllStateFields() throws Exception {
    WriteMetadataStateSerializer serializer = WriteMetadataStateSerializer.INSTANCE;
    WriteMetadataEvent expected = event(deltaWriteStat());
    WriteMetadataEvent actual = roundTrip(expected);

    assertEquals(4, serializer.getVersion());
    assertEquals(expected.getTaskID(), actual.getTaskID());
    assertEquals(expected.getCheckpointId(), actual.getCheckpointId());
    assertEquals(expected.getInstantTime(), actual.getInstantTime());
    assertEquals(expected.isLastBatch(), actual.isLastBatch());
    assertEquals(expected.isEndInput(), actual.isEndInput());
    assertEquals(expected.isBootstrap(), actual.isBootstrap());
    assertEquals(expected.isMetadataTable(), actual.isMetadataTable());

    WriteStatus expectedStatus = expected.getWriteStatuses().get(0);
    WriteStatus actualStatus = actual.getWriteStatuses().get(0);
    assertEquals(expectedStatus.isMetadataTable(), actualStatus.isMetadataTable());
    assertEquals(expectedStatus.getTotalRecords(), actualStatus.getTotalRecords());
    assertEquals(expectedStatus.getTotalErrorRecords(), actualStatus.getTotalErrorRecords());
    assertEquals(expectedStatus.getFileId(), actualStatus.getFileId());
    assertEquals(expectedStatus.getPartitionPath(), actualStatus.getPartitionPath());
    assertEquals(expectedStatus.hasErrors(), actualStatus.hasErrors());
    assertThrowable(expectedStatus.getGlobalError(), actualStatus.getGlobalError());
    assertEquals(expectedStatus.getErrors().keySet(), actualStatus.getErrors().keySet());
    for (Map.Entry<HoodieKey, Throwable> entry : expectedStatus.getErrors().entrySet()) {
      assertThrowable(entry.getValue(), actualStatus.getErrors().get(entry.getKey()));
    }

    assertTrue(actualStatus.getFailedRecords().isEmpty());
    assertTrue(actualStatus.getIndexStats().getWrittenRecordDelegates().isEmpty());
    assertTrue(actualStatus.getIndexStats().getSecondaryIndexStats().isEmpty());
    assertWriteStat(expectedStatus.getStat(), actualStatus.getStat());
  }

  @Test
  void testPayloadUsesCompleteEventAvroDto() throws Exception {
    WriteMetadataEvent expected = event(deltaWriteStat());
    org.apache.hudi.sink.avro.model.WriteMetadataEvent state = readState(expected);

    assertEquals(expected.getTaskID(), state.getTaskId());
    assertEquals(expected.getCheckpointId(), state.getCheckpointId());
    assertEquals(expected.getInstantTime(), state.getInstantTime());
    assertEquals(expected.isLastBatch(), state.getLastBatch());
    assertEquals(expected.isEndInput(), state.getEndInput());
    assertEquals(expected.isBootstrap(), state.getBootstrap());
    assertEquals(expected.isMetadataTable(), state.getMetadataTable());
    assertEquals(1, state.getWriteStatuses().size());
    org.apache.hudi.sink.avro.model.HoodieDeltaWriteStat deltaStat = assertInstanceOf(
        org.apache.hudi.sink.avro.model.HoodieDeltaWriteStat.class,
        state.getWriteStatuses().get(0).getStat());
    assertEquals("file-1", deltaStat.getFileId());
    assertEquals(Arrays.asList("file-1.log.1", "file-1.log.2"),
        deltaStat.getLogFiles());
    assertEquals(2, state.getWriteStatuses().get(0).getErrors().size());

    org.apache.hudi.sink.avro.model.WriteMetadataEvent baseState =
        readState(event(baseWriteStat(new HoodieWriteStat())));
    assertInstanceOf(org.apache.hudi.sink.avro.model.HoodieWriteStat.class,
        baseState.getWriteStatuses().get(0).getStat());
  }

  @Test
  void testBaseWriteStatRoundTrip() throws Exception {
    HoodieWriteStat expected = baseWriteStat(new HoodieWriteStat());
    HoodieWriteStat actual = roundTrip(event(expected)).getWriteStatuses().get(0).getStat();

    assertFalse(actual instanceof HoodieDeltaWriteStat);
    assertWriteStat(expected, actual);
  }

  @Test
  void testEmptyLogFilesRestoreDeltaWriteStat() throws Exception {
    HoodieDeltaWriteStat expected = deltaWriteStat();
    expected.setLogFiles(Collections.emptyList());

    HoodieWriteStat actual = roundTrip(event(expected)).getWriteStatuses().get(0).getStat();

    assertInstanceOf(HoodieDeltaWriteStat.class, actual);
    assertTrue(((HoodieDeltaWriteStat) actual).getLogFiles().isEmpty());
  }

  @Test
  void testRestoreV1AvroState() throws Exception {
    WriteMetadataStateSerializer serializer = WriteMetadataStateSerializer.INSTANCE;
    HoodieDeltaWriteStat expectedStat = deltaWriteStat();
    expectedStat.setLogFiles(Collections.emptyList());
    expectedStat.setRecordsStats(v1ColumnStats());
    WriteMetadataEvent expected = event(expectedStat);

    GenericRecord currentRecord = HoodieAvroUtils.bytesToAvro(
        serializer.serialize(expected), org.apache.hudi.sink.avro.model.WriteMetadataEvent.getClassSchema());
    GenericRecord v1Record = HoodieAvroUtils.rewriteRecordWithNewSchema(
        currentRecord, WriteMetadataStateSerializer.getWriterSchema(1));
    WriteMetadataEvent actual = serializer.deserialize(1, HoodieAvroUtils.avroToBytes(v1Record));

    assertEquals(expected.getTaskID(), actual.getTaskID());
    assertEquals(-1L, actual.getCheckpointId());
    assertEquals(expected.getInstantTime(), actual.getInstantTime());
    assertEquals(expected.isLastBatch(), actual.isLastBatch());
    assertEquals(expected.isEndInput(), actual.isEndInput());
    assertEquals(expected.isBootstrap(), actual.isBootstrap());
    assertFalse(actual.isMetadataTable());

    WriteStatus expectedStatus = expected.getWriteStatuses().get(0);
    WriteStatus actualStatus = actual.getWriteStatuses().get(0);
    assertFalse(actualStatus.isMetadataTable());
    assertEquals(expectedStatus.getTotalRecords(), actualStatus.getTotalRecords());
    assertEquals(expectedStatus.getTotalErrorRecords(), actualStatus.getTotalErrorRecords());
    assertEquals(expectedStatus.getFileId(), actualStatus.getFileId());
    assertEquals(expectedStatus.getPartitionPath(), actualStatus.getPartitionPath());
    assertThrowable(expectedStatus.getGlobalError(), actualStatus.getGlobalError());
    assertEquals(expectedStatus.getErrors().keySet(), actualStatus.getErrors().keySet());
    for (Map.Entry<HoodieKey, Throwable> entry : expectedStatus.getErrors().entrySet()) {
      assertThrowable(entry.getValue(), actualStatus.getErrors().get(entry.getKey()));
    }

    // These fields were added after the v1 writer schema and therefore restore to model defaults.
    expectedStat.setPrevBaseFile(null);
    expectedStat.setTotalLogReadTimeMs(0L);
    expectedStat.setNumUpdates(0L);
    assertWriteStat(expectedStat, actualStatus.getStat());
  }

  @Test
  void testRejectsUnsupportedAvroVersion() {
    assertThrows(Exception.class, () -> WriteMetadataStateSerializer.INSTANCE.deserialize(3, new byte[0]));
  }

  private static WriteMetadataEvent roundTrip(WriteMetadataEvent event) throws Exception {
    WriteMetadataStateSerializer serializer = WriteMetadataStateSerializer.INSTANCE;
    return SimpleVersionedSerialization.readVersionAndDeSerialize(
        serializer, SimpleVersionedSerialization.writeVersionAndSerialize(serializer, event));
  }

  private static org.apache.hudi.sink.avro.model.WriteMetadataEvent readState(WriteMetadataEvent event)
      throws Exception {
    byte[] bytes = WriteMetadataStateSerializer.INSTANCE.serialize(event);
    return new SpecificDatumReader<>(org.apache.hudi.sink.avro.model.WriteMetadataEvent.class)
        .read(null, DecoderFactory.get().binaryDecoder(bytes, null));
  }

  private static WriteMetadataEvent event(HoodieWriteStat stat) {
    WriteStatus status = new WriteStatus(true, 0.25d, true);
    status.setFileId("file-1");
    status.setPartitionPath("partition-a");
    status.setTotalRecords(101);
    status.setTotalErrorRecords(2);
    status.getErrors().put(
        new HoodieKey("record-1", "partition-a"), failure("record failure", 101));
    status.getErrors().put(
        new HoodieKey("record-2", "partition-a"), failure("another record failure", 102));
    status.setGlobalError(failure("global failure", 103));
    status.setStat(stat);

    return WriteMetadataEvent.builder()
        .taskID(7)
        .checkpointId(99L)
        .instantTime("001")
        .writeStatus(Collections.singletonList(status))
        .lastBatch(true)
        .endInput(true)
        .bootstrap(true)
        .metadataTable(true)
        .build();
  }

  private static HoodieDeltaWriteStat deltaWriteStat() {
    HoodieDeltaWriteStat stat = baseWriteStat(new HoodieDeltaWriteStat());
    stat.setLogVersion(3);
    stat.setLogOffset(4096L);
    stat.setBaseFile("file-1.parquet");
    stat.setLogFiles(Arrays.asList("file-1.log.1", "file-1.log.2"));
    return stat;
  }

  private static <T extends HoodieWriteStat> T baseWriteStat(T stat) {
    stat.setFileId("file-1");
    stat.setPath("partition-a/file-1.log");
    stat.setCdcStats(Collections.singletonMap("file-1.cdc", 2048L));
    stat.setPrevCommit("000");
    stat.setNumWrites(101L);
    stat.setNumDeletes(2L);
    stat.setNumUpdateWrites(31L);
    stat.setTotalWriteBytes(8192L);
    stat.setTotalWriteErrors(2L);
    stat.setPartitionPath("partition-a");
    stat.setTotalLogRecords(89L);
    stat.setTotalUpdatedRecordsCompacted(23L);
    stat.setNumInserts(70L);
    stat.setTotalLogBlocks(11L);
    stat.setTotalCorruptLogBlock(1L);
    stat.setTotalRollbackBlocks(2L);
    stat.setFileSizeInBytes(16384L);
    stat.setPrevBaseFile("file-0.parquet");
    stat.setMinEventTime(1000L);
    stat.setMaxEventTime(9000L);
    HoodieWriteStat.RuntimeStats runtimeStats = new HoodieWriteStat.RuntimeStats();
    runtimeStats.setTotalScanTime(10L);
    runtimeStats.setTotalCreateTime(20L);
    runtimeStats.setTotalUpsertTime(30L);
    stat.setRuntimeStats(runtimeStats);
    stat.setTotalLogFilesCompacted(4L);
    stat.setTotalLogReadTimeMs(500L);
    stat.setTotalLogSizeCompacted(4096L);
    stat.setTempPath("partition-a/.temp/file-1");
    stat.setNumUpdates(29L);
    stat.setRecordsStats(columnStats());
    return stat;
  }

  private static Map<String, HoodieColumnRangeMetadata<Comparable>> columnStats() {
    Map<String, HoodieColumnRangeMetadata<Comparable>> stats = new LinkedHashMap<>();
    ValueMetadata v1Metadata = ValueMetadata.getEmptyValueMetadata(HoodieIndexVersion.V1);
    Map<String, Comparable> values = new LinkedHashMap<>();
    values.put("string", "value");
    values.put("integer", 17);
    values.put("long", 19L);
    values.put("float", 1.25F);
    values.put("double", 9.5D);
    values.put("boolean", true);
    values.put("big_decimal_v1", new BigDecimal("123.45"));
    values.put("big_integer", new BigInteger("12345678901234567890"));
    values.put("sql_date", Date.valueOf("2026-07-10"));
    Timestamp timestamp = Timestamp.valueOf("2026-07-10 12:34:56.123456789");
    values.put("timestamp", timestamp);
    values.put("byte_buffer", ByteBuffer.wrap(new byte[] {1, 2, 3, 4}));
    values.put("uuid", UUID.fromString("123e4567-e89b-12d3-a456-426614174000"));
    values.put("local_date", LocalDate.of(2026, 7, 10));
    values.put("local_time", LocalTime.of(12, 34, 56, 123456789));
    values.put("instant", Instant.parse("2026-07-10T12:34:56.123456789Z"));
    values.put("local_date_time", LocalDateTime.of(2026, 7, 10, 12, 34, 56, 123456789));
    values.forEach((name, value) -> stats.put(
        name, columnRange(name, value, value, v1Metadata)));

    ValueMetadata decimalMetadata = ValueMetadata.getValueMetadata(HoodieValueTypeInfo.newBuilder()
        .setTypeOrdinal(ValueType.DECIMAL.ordinal())
        .setAdditionalInfo("10,2")
        .build());
    stats.put("decimal", columnRange(
        "decimal", new BigDecimal("1.25"), new BigDecimal("999.99"), decimalMetadata));
    stats.put("null", columnRange("null", null, null, ValueMetadata.NULL_METADATA));
    return stats;
  }

  private static Map<String, HoodieColumnRangeMetadata<Comparable>> v1ColumnStats() {
    Map<String, HoodieColumnRangeMetadata<Comparable>> stats = new LinkedHashMap<>();
    ValueMetadata metadata = ValueMetadata.getEmptyValueMetadata(HoodieIndexVersion.V1);
    stats.put("string", columnRange(
        "string", "alpha", "omega", metadata));
    stats.put("decimal", columnRange(
        "decimal", new BigDecimal("1.25"), new BigDecimal("999.99"),
        metadata));
    return stats;
  }

  private static HoodieColumnRangeMetadata<Comparable> columnRange(
      String columnName,
      Comparable minValue,
      Comparable maxValue,
      ValueMetadata valueMetadata) {
    return HoodieColumnRangeMetadata.<Comparable>create(
        "partition-a/file-1.log", columnName, minValue, maxValue,
        3L, 101L, 2048L, 4096L, valueMetadata);
  }

  private static void assertWriteStat(HoodieWriteStat expected, HoodieWriteStat actual) {
    assertEquals(expected.getFileId(), actual.getFileId());
    assertEquals(expected.getPath(), actual.getPath());
    assertEquals(expected.getCdcStats(), actual.getCdcStats());
    assertEquals(expected.getPrevCommit(), actual.getPrevCommit());
    assertEquals(expected.getNumWrites(), actual.getNumWrites());
    assertEquals(expected.getNumDeletes(), actual.getNumDeletes());
    assertEquals(expected.getNumUpdateWrites(), actual.getNumUpdateWrites());
    assertEquals(expected.getTotalWriteBytes(), actual.getTotalWriteBytes());
    assertEquals(expected.getTotalWriteErrors(), actual.getTotalWriteErrors());
    assertEquals(expected.getPartitionPath(), actual.getPartitionPath());
    assertEquals(expected.getTotalLogRecords(), actual.getTotalLogRecords());
    assertEquals(expected.getTotalUpdatedRecordsCompacted(), actual.getTotalUpdatedRecordsCompacted());
    assertEquals(expected.getNumInserts(), actual.getNumInserts());
    assertEquals(expected.getTotalLogBlocks(), actual.getTotalLogBlocks());
    assertEquals(expected.getTotalCorruptLogBlock(), actual.getTotalCorruptLogBlock());
    assertEquals(expected.getTotalRollbackBlocks(), actual.getTotalRollbackBlocks());
    assertEquals(expected.getFileSizeInBytes(), actual.getFileSizeInBytes());
    assertEquals(expected.getPrevBaseFile(), actual.getPrevBaseFile());
    assertEquals(expected.getMinEventTime(), actual.getMinEventTime());
    assertEquals(expected.getMaxEventTime(), actual.getMaxEventTime());
    assertEquals(expected.getRuntimeStats().getTotalScanTime(), actual.getRuntimeStats().getTotalScanTime());
    assertEquals(expected.getRuntimeStats().getTotalCreateTime(), actual.getRuntimeStats().getTotalCreateTime());
    assertEquals(expected.getRuntimeStats().getTotalUpsertTime(), actual.getRuntimeStats().getTotalUpsertTime());
    assertEquals(expected.getTotalLogFilesCompacted(), actual.getTotalLogFilesCompacted());
    assertEquals(expected.getTotalLogReadTimeMs(), actual.getTotalLogReadTimeMs());
    assertEquals(expected.getTotalLogSizeCompacted(), actual.getTotalLogSizeCompacted());
    assertEquals(expected.getTempPath(), actual.getTempPath());
    assertEquals(expected.getNumUpdates(), actual.getNumUpdates());
    if (expected instanceof HoodieDeltaWriteStat) {
      HoodieDeltaWriteStat expectedDelta = (HoodieDeltaWriteStat) expected;
      HoodieDeltaWriteStat actualDelta = assertInstanceOf(HoodieDeltaWriteStat.class, actual);
      assertEquals(expectedDelta.getLogVersion(), actualDelta.getLogVersion());
      assertEquals(expectedDelta.getLogOffset(), actualDelta.getLogOffset());
      assertEquals(expectedDelta.getBaseFile(), actualDelta.getBaseFile());
      assertEquals(expectedDelta.getLogFiles(), actualDelta.getLogFiles());
    }
    assertColumnStats(expected, actual);
  }

  private static void assertColumnStats(HoodieWriteStat expected, HoodieWriteStat actual) {
    assertEquals(expected.getColumnStats().isPresent(), actual.getColumnStats().isPresent());
    Map<String, HoodieColumnRangeMetadata<Comparable>> expectedStats = expected.getColumnStats().get();
    Map<String, HoodieColumnRangeMetadata<Comparable>> actualStats = actual.getColumnStats().get();
    assertEquals(expectedStats.keySet(), actualStats.keySet());
    expectedStats.forEach((name, expectedStat) -> {
      HoodieColumnRangeMetadata<Comparable> actualStat = actualStats.get(name);
      assertEquals(expectedStat.getFilePath(), actualStat.getFilePath());
      assertEquals(expectedStat.getColumnName(), actualStat.getColumnName());
      assertEquals(expectedStat.getMinValue(), actualStat.getMinValue());
      assertEquals(expectedStat.getMaxValue(), actualStat.getMaxValue());
      assertEquals(expectedStat.getNullCount(), actualStat.getNullCount());
      assertEquals(expectedStat.getValueCount(), actualStat.getValueCount());
      assertEquals(expectedStat.getTotalSize(), actualStat.getTotalSize());
      assertEquals(expectedStat.getTotalUncompressedSize(), actualStat.getTotalUncompressedSize());
      assertEquals(expectedStat.getValueMetadata().getValueType(), actualStat.getValueMetadata().getValueType());
      HoodieValueTypeInfo expectedTypeInfo = expectedStat.getValueMetadata().getValueTypeInfo();
      HoodieValueTypeInfo actualTypeInfo = actualStat.getValueMetadata().getValueTypeInfo();
      if (expectedTypeInfo != null) {
        assertEquals(expectedTypeInfo.getTypeOrdinal(), actualTypeInfo.getTypeOrdinal());
        assertEquals(expectedTypeInfo.getAdditionalInfo(), actualTypeInfo.getAdditionalInfo());
      }
    });
  }

  private static IllegalStateException failure(String message, int lineNumber) {
    IllegalStateException failure = new IllegalStateException(message);
    failure.setStackTrace(new StackTraceElement[] {
        new StackTraceElement("org.apache.hudi.TestWriter", "write", "TestWriter.java", lineNumber)
    });
    return failure;
  }

  private static void assertThrowable(Throwable expected, Throwable actual) {
    assertInstanceOf(RuntimeException.class, actual);
    assertEquals(expected.getClass().getName() + ": " + expected.getMessage(), actual.getMessage());
    assertArrayEquals(expected.getStackTrace(), actual.getStackTrace());
  }
}
