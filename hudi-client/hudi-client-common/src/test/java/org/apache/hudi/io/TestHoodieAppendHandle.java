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

package org.apache.hudi.io;

import org.apache.hudi.common.config.HoodieMemoryConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.EngineProperty;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SizeEstimator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.TestBaseHoodieTable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link HoodieAppendHandle}'s block-flush sizing logic.
 *
 * <p>The block-flush trigger sizes the post-{@code prepareRecord} record (the object actually
 * retained in {@code recordList}), not the incoming pre-{@code prepareRecord} record. On Spark
 * engines the incoming record is a compact {@code UnsafeRow} while the buffered record is a
 * full {@code HoodieAvroIndexedRecord} — sizing the wrong one caused production OOMs on
 * metadata-table writes by letting the buffer grow many times past {@code maxBlockSize} worth
 * of heap before the gate fired.
 */
@ExtendWith(MockitoExtension.class)
public class TestHoodieAppendHandle extends HoodieCommonTestHarness {

  private static final String TEST_INSTANT_TIME = "20231201120000";
  private static final String TEST_PARTITION_PATH = DEFAULT_FIRST_PARTITION_PATH;
  private static final String TEST_FILE_ID = "file-001";

  private HoodieWriteConfig writeConfig;
  private TaskContextSupplier taskContextSupplier;
  private HoodieTable hoodieTable;

  @BeforeEach
  void setUp() throws IOException {
    initPath();
    initMetaClient(false);
    initTestDataGenerator();

    writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withSchema(TRIP_EXAMPLE_SCHEMA)
        .withMarkersType("DIRECT")
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .build();
    hoodieTable = new TestBaseHoodieTable(writeConfig, getEngineContext(), metaClient);
    taskContextSupplier = new LocalTaskContextSupplier();
  }

  @AfterEach
  void tearDown() throws Exception {
    cleanMetaClient();
    cleanupTestDataGenerator();
  }

  /**
   * The estimator must size the post-{@code prepareRecord} buffered record, not the incoming
   * pre-{@code prepareRecord} record. On Spark engines, the incoming record is a compact
   * {@code UnsafeRow} while the buffered record is a full {@code HoodieAvroIndexedRecord} —
   * sizing the wrong one was the root cause of production OOMs on metadata-table writes.
   */
  @Test
  void testSizeEstimatorReceivesBufferedRecord() {
    RecordingSizeEstimator spy = new RecordingSizeEstimator(1000L);
    TestableAppendHandle handle = newTestableHandle(spy);

    HoodieRecord buffered = mock(HoodieRecord.class);
    handle.simulateBufferedRecordForTest(buffered);

    assertEquals(1, spy.sizedObjects.size(), "estimator should be invoked once on the first buffered record");
    assertSame(buffered, spy.sizedObjects.get(0),
        "estimator must size the post-prepareRecord buffered record, not the incoming record");
  }

  /**
   * The initial estimate is seeded lazily on the first buffered record — the old code seeded it
   * eagerly from the incoming pre-{@code prepareRecord} record in {@code init()}, which
   * dramatically under-counted heap on Spark engines.
   */
  @Test
  void testInitialEstimateSeededFromFirstBufferedRecord() {
    TestableAppendHandle handle = newTestableHandle(new RecordingSizeEstimator(2048L));

    assertEquals(0L, handle.getAverageRecordSize(), "no records yet — estimate is unseeded");

    handle.simulateBufferedRecordForTest(mock(HoodieRecord.class));

    assertEquals(2048L, handle.getAverageRecordSize(),
        "first buffered record seeds averageRecordSize directly (no EWMA on first sample)");
  }

  /**
   * A delete-only window must not perturb {@code averageRecordSize} and must not trigger a
   * flush — {@code recordList} did not grow, so there is no in-heap buffer to bound.
   */
  @Test
  void testDeleteOnlyDoesNotPerturbEstimateOrFlush() {
    RecordingSizeEstimator spy = new RecordingSizeEstimator(1234L);
    TestableAppendHandle handle = newTestableHandle(spy);

    for (int i = 0; i < 250; i++) {
      handle.simulateBufferedRecordForTest(null);
    }

    assertEquals(0, spy.sizedObjects.size(), "estimator never invoked on delete-only windows");
    assertEquals(0L, handle.getAverageRecordSize(), "estimate remains unseeded with no buffered records");
    assertEquals(0, handle.appendInvocations.get(), "flush gate never fires when averageRecordSize is 0");
  }

  /**
   * Once seeded, the EWMA blends new samples (20%) with the running estimate (80%). The sampler
   * fires every {@code NUMBER_OF_RECORDS_TO_ESTIMATE_RECORD_SIZE} records.
   */
  @Test
  void testEwmaBlendsSamplesAfterFirstSeed() {
    TestableAppendHandle handle = newTestableHandle(new SteppedSizeEstimator(1000L, 5000L));

    handle.simulateBufferedRecordForTest(mock(HoodieRecord.class));
    assertEquals(1000L, handle.getAverageRecordSize(), "first record seeds the estimate at 1000");

    // 99 more records — the sampler will not fire again until numberOfRecords % 100 == 0.
    // Record #100 returns the second sample (5000); EWMA blends to 0.8*1000 + 0.2*5000 = 1800.
    for (int i = 0; i < 99; i++) {
      handle.simulateBufferedRecordForTest(mock(HoodieRecord.class));
    }
    assertEquals(1800L, handle.getAverageRecordSize(), "EWMA after the second sample: 0.8*1000 + 0.2*5000");
  }

  /**
   * When the per-record estimate reaches {@code maxBlockSize / N}, the gate trips after N
   * records — i.e. {@code numberOfRecords >= maxBlockSize / averageRecordSize}.
   */
  @Test
  void testFlushTriggersWhenBufferedRecordsExceedMaxBlockSize() {
    long perRecord = writeConfig.getLogFileDataBlockMaxSize() / 10;
    TestableAppendHandle handle = newTestableHandle(new RecordingSizeEstimator(perRecord));

    for (int i = 0; i < 9; i++) {
      handle.simulateBufferedRecordForTest(mock(HoodieRecord.class));
    }
    assertEquals(0, handle.appendInvocations.get(), "9 records of perRecord-sized buffer: gate has not fired yet");

    handle.simulateBufferedRecordForTest(mock(HoodieRecord.class));
    assertEquals(1, handle.appendInvocations.get(), "10th record fills the block — flush fires");
    assertEquals(0L, handle.getNumberOfRecordsForTest(), "numberOfRecords resets after flush");
    assertTrue(handle.getEstimatedBytesWrittenForTest() > 0, "estimatedNumberOfBytesWritten advances after flush");
  }

  /** Guards against the test harness silently no-op'ing the flush hook. */
  @Test
  void testHarnessAppendOverrideIsActuallyInvoked() {
    TestableAppendHandle handle =
        newTestableHandle(new RecordingSizeEstimator(writeConfig.getLogFileDataBlockMaxSize()));
    // Single record whose size == maxBlockSize: gate fires immediately on record #1
    // (numberOfRecords == 1 >= maxBlockSize / maxBlockSize == 1).
    handle.simulateBufferedRecordForTest(mock(HoodieRecord.class));
    assertEquals(1, handle.appendInvocations.get(),
        "harness must route flushes through the override, not the real writer");
    assertNotEquals(0L, handle.getEstimatedBytesWrittenForTest());
    assertFalse(handle.getAverageRecordSize() == 0L);
  }

  private TestableAppendHandle newTestableHandle(SizeEstimator<HoodieRecord> estimator) {
    TestableAppendHandle handle = new TestableAppendHandle(writeConfig, hoodieTable, taskContextSupplier);
    handle.setSizeEstimator(estimator);
    return handle;
  }

  /** Records (and counts) every object handed to {@link #sizeEstimate}. */
  private static final class RecordingSizeEstimator implements SizeEstimator<HoodieRecord> {
    final List<HoodieRecord> sizedObjects = new ArrayList<>();
    private final long fixedSize;

    RecordingSizeEstimator(long fixedSize) {
      this.fixedSize = fixedSize;
    }

    @Override
    public long sizeEstimate(HoodieRecord r) {
      sizedObjects.add(r);
      return fixedSize;
    }
  }

  /** Returns {@code first} on the first call, then {@code rest} on every subsequent call. */
  private static final class SteppedSizeEstimator implements SizeEstimator<HoodieRecord> {
    private final long first;
    private final long rest;
    private boolean firstCallDone = false;

    SteppedSizeEstimator(long first, long rest) {
      this.first = first;
      this.rest = rest;
    }

    @Override
    public long sizeEstimate(HoodieRecord r) {
      if (!firstCallDone) {
        firstCallDone = true;
        return first;
      }
      return rest;
    }
  }

  /**
   * Test handle that overrides the disk-flush side-effect — we are only validating the sizing
   * and gate logic, not the writer plumbing.
   */
  private static final class TestableAppendHandle extends HoodieAppendHandle<Object, Object, Object, Object> {
    final AtomicInteger appendInvocations = new AtomicInteger(0);

    TestableAppendHandle(HoodieWriteConfig writeConfig, HoodieTable hoodieTable,
                         TaskContextSupplier taskContextSupplier) {
      super(writeConfig, TEST_INSTANT_TIME, hoodieTable, TEST_PARTITION_PATH, TEST_FILE_ID, taskContextSupplier);
    }

    @Override
    protected void appendDataAndDeleteBlocks(Map<HeaderMetadataType, String> header, boolean appendDeleteBlocks) {
      appendInvocations.incrementAndGet();
    }
  }

  /**
   * When the engine does not expose memory/cores (e.g., Flink, or Spark without {@code SparkEnv}),
   * the dynamic ceiling is absent and the flush gate continues to use the configured
   * {@code maxBlockSize}. {@link LocalTaskContextSupplier} returns {@code Option.empty()} for all
   * engine properties, so this is the fallback path.
   */
  @Test
  void testEffectiveBlockSizeFallsBackToMaxBlockSizeWhenNoEngineProps() {
    TestableAppendHandle handle = newTestableHandle(new RecordingSizeEstimator(1000L));
    assertEquals(handle.getMaxBlockSizeForTest(), handle.getEffectiveBlockSizeForTest(),
        "no engine properties — effective cap collapses to maxBlockSize");
  }

  /**
   * On a small executor, the dynamic per-task ceiling is smaller than the configured
   * {@code maxBlockSize} and must win. With 200MB executor / 0.6 memory.fraction / 1 core / 0.6
   * append-fraction the ceiling is 48MB, well below the default 256MB block size.
   */
  @Test
  void testEffectiveBlockSizeIsDynamicCeilingWhenSmallerThanMaxBlockSize() {
    long executorBytes = 200L * 1024 * 1024;
    TaskContextSupplier supplier = new StubTaskContextSupplier(executorBytes, 0.6, 1);
    TestableAppendHandle handle = new TestableAppendHandle(writeConfig, hoodieTable, supplier);

    long expectedDynamic = (long) Math.floor(executorBytes * (1 - 0.6) / 1 * 0.6); // 48MB
    assertEquals(expectedDynamic, handle.getEffectiveBlockSizeForTest(),
        "small executor — dynamic ceiling wins over configured maxBlockSize");
    assertTrue(handle.getEffectiveBlockSizeForTest() < handle.getMaxBlockSizeForTest(),
        "effective cap is below maxBlockSize on a tight executor");
  }

  /**
   * The dynamic ceiling is floored at
   * {@link HoodieMemoryConfig#MIN_MEMORY_FOR_LOG_APPEND_BUFFER_IN_BYTES} (16MB). With a tiny 50MB
   * executor the raw computation yields ~12MB, but the floor pulls it back to 16MB.
   */
  @Test
  void testEffectiveBlockSizeRespects16MBFloor() {
    long executorBytes = 50L * 1024 * 1024;
    TaskContextSupplier supplier = new StubTaskContextSupplier(executorBytes, 0.6, 1);
    TestableAppendHandle handle = new TestableAppendHandle(writeConfig, hoodieTable, supplier);

    assertEquals(HoodieMemoryConfig.MIN_MEMORY_FOR_LOG_APPEND_BUFFER_IN_BYTES,
        handle.getEffectiveBlockSizeForTest(),
        "raw computation falls below 16MB floor — floor wins");
  }

  /**
   * When the dynamic ceiling is larger than the configured {@code maxBlockSize}, the latter wins.
   * 16GB executor / 0.6 / 4 cores / 0.6 append-fraction → 960MB ceiling, much larger than the
   * default 256MB block size.
   */
  @Test
  void testEffectiveBlockSizeKeepsMaxBlockSizeWhenDynamicIsLarger() {
    long executorBytes = 16L * 1024 * 1024 * 1024;
    TaskContextSupplier supplier = new StubTaskContextSupplier(executorBytes, 0.6, 4);
    TestableAppendHandle handle = new TestableAppendHandle(writeConfig, hoodieTable, supplier);

    assertEquals(handle.getMaxBlockSizeForTest(), handle.getEffectiveBlockSizeForTest(),
        "large executor — configured maxBlockSize is the binding ceiling");
  }

  /**
   * The flush gate must use {@code effectiveBlockSize}, not {@code maxBlockSize}. With a 48MB
   * dynamic ceiling (200MB executor / 0.6 / 1 core / 0.6 append-fraction) and 4MB per-record,
   * the gate fires after 12 records (48MB / 4MB), well before the 256MB / 4MB = 64 records the
   * old code would have allowed.
   */
  @Test
  void testFlushGateFiresAtDynamicCeilingNotMaxBlockSize() {
    long executorBytes = 200L * 1024 * 1024;
    TaskContextSupplier supplier = new StubTaskContextSupplier(executorBytes, 0.6, 1);
    TestableAppendHandle handle = new TestableAppendHandle(writeConfig, hoodieTable, supplier);
    long perRecord = 4L * 1024 * 1024;
    handle.setSizeEstimator(new RecordingSizeEstimator(perRecord));

    long expectedRecordsAtFlush = handle.getEffectiveBlockSizeForTest() / perRecord;
    long recordsAtFlushWithMaxBlockSize = handle.getMaxBlockSizeForTest() / perRecord;
    assertTrue(expectedRecordsAtFlush < recordsAtFlushWithMaxBlockSize,
        "test setup: dynamic ceiling must trip the gate earlier than maxBlockSize would");

    for (int i = 0; i < expectedRecordsAtFlush - 1; i++) {
      handle.simulateBufferedRecordForTest(mock(HoodieRecord.class));
    }
    assertEquals(0, handle.appendInvocations.get(),
        "below the dynamic ceiling: gate has not fired yet");

    handle.simulateBufferedRecordForTest(mock(HoodieRecord.class));
    assertEquals(1, handle.appendInvocations.get(),
        "buffered records hit the dynamic ceiling — flush fires earlier than maxBlockSize would have allowed");
  }

  /**
   * {@link TaskContextSupplier} stub returning the three engine properties the dynamic-ceiling
   * formula consumes — mirrors what {@code SparkTaskContextSupplier} exposes.
   */
  private static final class StubTaskContextSupplier extends TaskContextSupplier {
    private final long executorMemoryBytes;
    private final double memoryFraction;
    private final int executorCores;

    StubTaskContextSupplier(long executorMemoryBytes, double memoryFraction, int executorCores) {
      this.executorMemoryBytes = executorMemoryBytes;
      this.memoryFraction = memoryFraction;
      this.executorCores = executorCores;
    }

    @Override
    public Option<String> getProperty(EngineProperty prop) {
      switch (prop) {
        case TOTAL_MEMORY_AVAILABLE:
          return Option.of(String.valueOf(executorMemoryBytes));
        case MEMORY_FRACTION_IN_USE:
          return Option.of(String.valueOf(memoryFraction));
        case TOTAL_CORES_PER_EXECUTOR:
          return Option.of(String.valueOf(executorCores));
        default:
          return Option.empty();
      }
    }

    @Override
    public Supplier<Integer> getPartitionIdSupplier() {
      return () -> 0;
    }

    @Override
    public Supplier<Integer> getStageIdSupplier() {
      return () -> 0;
    }

    @Override
    public Supplier<Long> getAttemptIdSupplier() {
      return () -> 0L;
    }

    @Override
    public Supplier<Integer> getTaskAttemptNumberSupplier() {
      return () -> -1;
    }

    @Override
    public Supplier<Integer> getStageAttemptNumberSupplier() {
      return () -> -1;
    }
  }
}
