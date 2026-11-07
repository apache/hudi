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

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieWriteStat;

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
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link WriteMetadataStateSerializer}. */
public class TestWriteMetadataStateSerializer {

  @Test
  void testRoundTripAllCoordinatorFields() throws Exception {
    WriteMetadataEvent expected = event(deltaWriteStat());
    WriteMetadataEvent actual = roundTrip(expected);

    assertEquals(1, WriteMetadataStateSerializer.INSTANCE.getVersion());
    assertEquals(expected.getTaskID(), actual.getTaskID());
    assertEquals(expected.getInstantTime(), actual.getInstantTime());
    assertEquals(expected.isLastBatch(), actual.isLastBatch());
    assertEquals(expected.isEndInput(), actual.isEndInput());
    assertEquals(expected.isBootstrap(), actual.isBootstrap());
    assertEquals(1, actual.getWriteStatuses().size());

    WriteStatus expectedStatus = expected.getWriteStatuses().get(0);
    WriteStatus actualStatus = actual.getWriteStatuses().get(0);
    assertEquals(expectedStatus.getTotalRecords(), actualStatus.getTotalRecords());
    assertEquals(expectedStatus.getTotalErrorRecords(), actualStatus.getTotalErrorRecords());
    assertEquals(expectedStatus.getFileId(), actualStatus.getFileId());
    assertEquals(expectedStatus.getPartitionPath(), actualStatus.getPartitionPath());
    assertEquals(expectedStatus.hasErrors(), actualStatus.hasErrors());
    assertThrowable(expectedStatus.getGlobalError(), actualStatus.getGlobalError());
    assertEquals(expectedStatus.getErrors().keySet(), actualStatus.getErrors().keySet());
    expectedStatus.getErrors().forEach((key, error) ->
        assertThrowable(error, actualStatus.getErrors().get(key)));

    assertFalse(actualStatus.isTrackingSuccessfulWrites());
    assertTrue(actualStatus.getWrittenRecordDelegates().isEmpty());
    assertTrue(actualStatus.getFailedRecords().isEmpty());
    assertWriteStat(expectedStatus.getStat(), actualStatus.getStat());
  }

  @Test
  void testBaseWriteStatRoundTrip() throws Exception {
    HoodieWriteStat expected = baseWriteStat(new HoodieWriteStat());
    HoodieWriteStat actual = roundTrip(event(expected)).getWriteStatuses().get(0).getStat();

    assertEquals(HoodieWriteStat.class, actual.getClass());
    assertWriteStat(expected, actual);
  }

  @Test
  void testEmptyLogFilesRestoreDeltaWriteStat() throws Exception {
    HoodieDeltaWriteStat expected = deltaWriteStat();
    expected.setLogFiles(Collections.emptyList());

    HoodieWriteStat actual = roundTrip(event(expected)).getWriteStatuses().get(0).getStat();

    assertEquals(HoodieDeltaWriteStat.class, actual.getClass());
    assertTrue(((HoodieDeltaWriteStat) actual).getLogFiles().isEmpty());
  }

  @Test
  void testPayloadUsesWriteStatSubtypeDtos() throws Exception {
    org.apache.hudi.sink.avro.model.WriteMetadataEvent baseState =
        readState(event(baseWriteStat(new HoodieWriteStat())));
    org.apache.hudi.sink.avro.model.WriteMetadataEvent deltaState = readState(event(deltaWriteStat()));

    assertEquals(org.apache.hudi.sink.avro.model.HoodieWriteStat.class,
        baseState.getWriteStatuses().get(0).getStat().getClass());
    assertEquals(org.apache.hudi.sink.avro.model.HoodieDeltaWriteStat.class,
        deltaState.getWriteStatuses().get(0).getStat().getClass());
  }

  @Test
  void testRejectsUnsupportedVersion() {
    assertThrows(Exception.class, () -> WriteMetadataStateSerializer.INSTANCE.deserialize(2, new byte[0]));
  }

  private static WriteMetadataEvent roundTrip(WriteMetadataEvent event) throws Exception {
    WriteMetadataStateSerializer serializer = WriteMetadataStateSerializer.INSTANCE;
    return SimpleVersionedSerialization.readVersionAndDeSerialize(
        serializer, SimpleVersionedSerialization.writeVersionAndSerialize(serializer, event));
  }

  private static org.apache.hudi.sink.avro.model.WriteMetadataEvent readState(WriteMetadataEvent event)
      throws Exception {
    byte[] bytes = WriteMetadataStateSerializer.INSTANCE.serialize(event);
    return new SpecificDatumReader<org.apache.hudi.sink.avro.model.WriteMetadataEvent>(
        org.apache.hudi.sink.avro.model.WriteMetadataEvent.getClassSchema()).read(
        null, DecoderFactory.get().binaryDecoder(bytes, null));
  }

  private static WriteMetadataEvent event(HoodieWriteStat stat) {
    WriteStatus status = new WriteStatus(true, 0.25D);
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
        .instantTime("001")
        .writeStatus(Collections.singletonList(status))
        .lastBatch(true)
        .endInput(true)
        .bootstrap(true)
        .build();
  }

  private static HoodieDeltaWriteStat deltaWriteStat() {
    HoodieDeltaWriteStat stat = baseWriteStat(new HoodieDeltaWriteStat());
    stat.setLogVersion(3);
    stat.setLogOffset(4096L);
    stat.setBaseFile("file-1.parquet");
    stat.setLogFiles(Arrays.asList("file-1.log.1", "file-1.log.2"));
    stat.setRecordsStats(columnStats());
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
    stat.setNumInserts(70L);
    stat.setTotalWriteBytes(8192L);
    stat.setTotalWriteErrors(2L);
    stat.setTempPath("partition-a/.temp/file-1");
    stat.setPartitionPath("partition-a");
    stat.setTotalLogRecords(89L);
    stat.setTotalLogFilesCompacted(4L);
    stat.setTotalLogSizeCompacted(4096L);
    stat.setTotalUpdatedRecordsCompacted(23L);
    stat.setTotalLogBlocks(11L);
    stat.setTotalCorruptLogBlock(1L);
    stat.setTotalRollbackBlocks(2L);
    stat.setFileSizeInBytes(16384L);
    stat.setMinEventTime(1000L);
    stat.setMaxEventTime(9000L);
    HoodieWriteStat.RuntimeStats runtimeStats = new HoodieWriteStat.RuntimeStats();
    runtimeStats.setTotalScanTime(10L);
    runtimeStats.setTotalCreateTime(20L);
    runtimeStats.setTotalUpsertTime(30L);
    stat.setRuntimeStats(runtimeStats);
    return stat;
  }

  private static Map<String, HoodieColumnRangeMetadata<Comparable>> columnStats() {
    Map<String, Comparable> values = new LinkedHashMap<>();
    values.put("string", "value");
    values.put("integer", 17);
    values.put("long", 19L);
    values.put("float", 1.25F);
    values.put("double", 9.5D);
    values.put("boolean", true);
    values.put("big_decimal", new BigDecimal("123.45"));
    values.put("big_integer", new BigInteger("12345678901234567890"));
    values.put("sql_date", Date.valueOf("2026-07-10"));
    values.put("timestamp", Timestamp.from(Instant.parse("2026-07-10T12:34:56.123456789Z")));
    values.put("byte_buffer", ByteBuffer.wrap(new byte[] {1, 2, 3, 4}));

    Map<String, HoodieColumnRangeMetadata<Comparable>> stats = new LinkedHashMap<>();
    values.forEach((name, value) -> stats.put(name, columnRange(name, value)));
    stats.put("null", columnRange("null", null));
    return stats;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static HoodieColumnRangeMetadata<Comparable> columnRange(String name, Comparable value) {
    return (HoodieColumnRangeMetadata) HoodieColumnRangeMetadata.create(
        "partition-a/file-1.log", name, value, value, 3L, 101L, 2048L, 4096L);
  }

  private static void assertWriteStat(HoodieWriteStat expected, HoodieWriteStat actual) {
    assertEquals(expected.getClass(), actual.getClass());
    assertEquals(expected.getFileId(), actual.getFileId());
    assertEquals(expected.getPath(), actual.getPath());
    assertEquals(expected.getCdcStats(), actual.getCdcStats());
    assertEquals(expected.getPrevCommit(), actual.getPrevCommit());
    assertEquals(expected.getNumWrites(), actual.getNumWrites());
    assertEquals(expected.getNumDeletes(), actual.getNumDeletes());
    assertEquals(expected.getNumUpdateWrites(), actual.getNumUpdateWrites());
    assertEquals(expected.getNumInserts(), actual.getNumInserts());
    assertEquals(expected.getTotalWriteBytes(), actual.getTotalWriteBytes());
    assertEquals(expected.getTotalWriteErrors(), actual.getTotalWriteErrors());
    assertEquals(expected.getTempPath(), actual.getTempPath());
    assertEquals(expected.getPartitionPath(), actual.getPartitionPath());
    assertEquals(expected.getTotalLogRecords(), actual.getTotalLogRecords());
    assertEquals(expected.getTotalLogFilesCompacted(), actual.getTotalLogFilesCompacted());
    assertEquals(expected.getTotalLogSizeCompacted(), actual.getTotalLogSizeCompacted());
    assertEquals(expected.getTotalUpdatedRecordsCompacted(), actual.getTotalUpdatedRecordsCompacted());
    assertEquals(expected.getTotalLogBlocks(), actual.getTotalLogBlocks());
    assertEquals(expected.getTotalCorruptLogBlock(), actual.getTotalCorruptLogBlock());
    assertEquals(expected.getTotalRollbackBlocks(), actual.getTotalRollbackBlocks());
    assertEquals(expected.getFileSizeInBytes(), actual.getFileSizeInBytes());
    assertEquals(expected.getMinEventTime(), actual.getMinEventTime());
    assertEquals(expected.getMaxEventTime(), actual.getMaxEventTime());
    assertEquals(expected.getRuntimeStats().getTotalScanTime(), actual.getRuntimeStats().getTotalScanTime());
    assertEquals(expected.getRuntimeStats().getTotalCreateTime(), actual.getRuntimeStats().getTotalCreateTime());
    assertEquals(expected.getRuntimeStats().getTotalUpsertTime(), actual.getRuntimeStats().getTotalUpsertTime());
    if (expected instanceof HoodieDeltaWriteStat) {
      HoodieDeltaWriteStat expectedDelta = (HoodieDeltaWriteStat) expected;
      HoodieDeltaWriteStat actualDelta = (HoodieDeltaWriteStat) actual;
      assertEquals(expectedDelta.getLogVersion(), actualDelta.getLogVersion());
      assertEquals(expectedDelta.getLogOffset(), actualDelta.getLogOffset());
      assertEquals(expectedDelta.getBaseFile(), actualDelta.getBaseFile());
      assertEquals(expectedDelta.getLogFiles(), actualDelta.getLogFiles());
      assertColumnStats(expectedDelta, actualDelta);
    }
  }

  private static void assertColumnStats(HoodieDeltaWriteStat expected, HoodieDeltaWriteStat actual) {
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
    assertEquals(RuntimeException.class, actual.getClass());
    assertEquals(expected.getClass().getName() + ": " + expected.getMessage(), actual.getMessage());
    assertArrayEquals(expected.getStackTrace(), actual.getStackTrace());
  }
}
