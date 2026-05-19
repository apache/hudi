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

package org.apache.hudi.sink.append;

import org.apache.hudi.common.util.ObjectSizeCalculator;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.sink.StreamWriteOperatorCoordinator;
import org.apache.hudi.sink.buffer.TotalSizeTracer;
import org.apache.hudi.sink.bulk.sort.SortOperatorGen;
import org.apache.hudi.utils.RuntimeContextUtils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.sort.SortCodeGenerator;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.generated.GeneratedNormalizedKeyComputer;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.NormalizedKeyComputer;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Sink function to write data with continuous sorting for improved compression.
 *
 * <p>Unlike {@link AppendWriteFunctionWithBIMBufferSort} which uses batch sorting,
 * this function maintains sorted order continuously using a TreeMap, providing:
 * <ul>
 *   <li>Non-blocking inserts (O(log n) vs O(1) + periodic O(n log n))</li>
 *   <li>Incremental draining without re-sorting</li>
 *   <li>Predictable latency (no sort spikes)</li>
 * </ul>
 *
 * <p>Strategy:
 * <ol>
 *   <li>Records are inserted in sorted order (TreeMap)</li>
 *   <li>When buffer reaches max capacity, oldest record(s) are drained synchronously</li>
 *   <li>Drain size is configurable to balance latency vs. throughput vs compression ratio</li>
 * </ol>
 *
 * @param <T> Type of the input record
 * @see StreamWriteOperatorCoordinator
 */
public class AppendWriteFunctionWithContinuousSort<T> extends AppendWriteFunction<T> {

  private static final Logger LOG = LoggerFactory.getLogger(AppendWriteFunctionWithContinuousSort.class);

  private final long maxCapacity;
  private final int drainSize;

  private transient TreeMap<SortKey, RowData> sortedRecords;
  private transient long insertionSequence;
  private transient TotalSizeTracer sizeTracer;

  // Sort key computation
  private transient NormalizedKeyComputer normalizedKeyComputer;
  private transient RecordComparator recordComparator;
  private transient MemorySegment reusableKeySegment;
  private transient int normalizedKeySize;
  private transient boolean objectReuseEnabled;
  private transient RowDataSerializer rowDataSerializer;

  // Metrics
  private transient long totalDrainOperations;
  private transient long totalDrainedRecords;
  private transient long totalInserted;
  private transient long estimatedRecordSize;

  public AppendWriteFunctionWithContinuousSort(Configuration config, RowType rowType) {
    super(config, rowType);

    // Configuration
    this.maxCapacity = config.get(FlinkOptions.WRITE_BUFFER_SIZE);
    this.drainSize = config.get(FlinkOptions.WRITE_BUFFER_SORT_CONTINUOUS_DRAIN_SIZE);

    LOG.info("AppendWriteFunctionWithContinuousSort created: maxCapacity={}, drainSize={}",
        maxCapacity, drainSize);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    // Validate configuration before calling super.open() which requires Flink runtime context
    if (maxCapacity <= 0) {
      throw new IllegalArgumentException(
          String.format("Buffer capacity must be positive, got: %d", maxCapacity));
    }

    if (drainSize <= 0) {
      throw new IllegalArgumentException(
          String.format("Drain size must be positive, got: %d", drainSize));
    }

    // Resolve sort keys, falling back to record key if not specified
    List<String> sortKeyList = AppendWriteFunctions.resolveSortKeys(config);

    super.open(parameters);

    LOG.info("Initializing continuous sort with keys: {}", sortKeyList);

    // Create sort code generator for normalized key computation and record comparison
    SortOperatorGen sortOperatorGen = new SortOperatorGen(rowType, sortKeyList.toArray(new String[0]));
    SortCodeGenerator codeGenerator = sortOperatorGen.createSortCodeGenerator();
    GeneratedNormalizedKeyComputer generatedKeyComputer = codeGenerator.generateNormalizedKeyComputer("ContinuousSortKeyComputer");
    GeneratedRecordComparator generatedComparator = codeGenerator.generateRecordComparator("ContinuousSortComparator");

    // Instantiate code-generated components
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    this.normalizedKeyComputer = generatedKeyComputer.newInstance(classLoader);
    this.recordComparator = generatedComparator.newInstance(classLoader);
    this.normalizedKeySize = normalizedKeyComputer.getNumKeyBytes();

    // Initialize TreeMap with comparator that uses normalized keys for fast comparison
    // and falls back to RecordComparator for full comparison when normalized keys are equal
    this.sortedRecords = new TreeMap<>((k1, k2) -> {
      int cmp = normalizedKeyComputer.compareKey(k1.keySegment, 0, k2.keySegment, 0);
      if (cmp != 0) {
        return cmp;
      }
      // Normalized keys are equal - use full record comparison for correct ordering
      cmp = recordComparator.compare(k1.record, k2.record);
      if (cmp != 0) {
        return cmp;
      }
      // Records are equal by sort keys - use insertion order for stability
      return Long.compare(k1.insertionOrder, k2.insertionOrder);
    });
    this.insertionSequence = 0L;

    // Allocate reusable on-heap buffer for computing keys
    byte[] reusableKeyBuffer = new byte[normalizedKeySize];
    this.reusableKeySegment = MemorySegmentFactory.wrap(reusableKeyBuffer);

    // Detect object reuse mode and create serializer for copying if needed
    this.objectReuseEnabled = RuntimeContextUtils.isObjectReuseEnabled(getRuntimeContext());
    if (this.objectReuseEnabled) {
      this.rowDataSerializer = new RowDataSerializer(rowType);
    }

    // Initialize metrics
    this.totalDrainOperations = 0;
    this.totalDrainedRecords = 0;
    this.totalInserted = 0;

    // Initialize memory size tracer for bounding buffer memory footprint
    this.sizeTracer = new TotalSizeTracer(config);

    LOG.info("AppendWriteFunctionWithContinuousSort initialized successfully");
  }

  @Override
  public void processElement(T value, Context ctx, Collector<RowData> out) throws Exception {
    RowData data = (RowData) value;

    // Check if buffer has reached max capacity (record count) or memory limit
    if (sortedRecords.size() >= maxCapacity || sizeTracer.bufferSize > sizeTracer.maxBufferSize) {
      drainRecords(drainSize);

      // Verify there's space after draining
      if (sortedRecords.size() >= maxCapacity) {
        throw new HoodieException(
            String.format("Buffer cannot accept record after draining. "
                + "Buffer size: %d, maxCapacity: %d, drainSize: %d",
                sortedRecords.size(), maxCapacity, drainSize));
      }
    }

    // Copy RowData when object reuse is enabled to prevent mutation after insertion
    if (objectReuseEnabled) {
      data = rowDataSerializer.copy(data);
    }

    // Write to buffer (maintains sorted order)
    // Compute normalized key into reusable segment
    normalizedKeyComputer.putKey(data, reusableKeySegment, 0);

    // Create sort key (copies the normalized key from reusable segment)
    SortKey key = new SortKey(reusableKeySegment, normalizedKeySize, data, insertionSequence++);

    // Store the RowData and track memory usage
    sortedRecords.put(key, data);
    if (estimatedRecordSize == 0) {
      estimatedRecordSize = ObjectSizeCalculator.getObjectSize(data);
    }
    sizeTracer.trace(estimatedRecordSize);

    totalInserted++;
  }

  /**
   * Drain oldest records from buffer and write to storage.
   */
  private void drainRecords(int count) throws IOException {
    if (sortedRecords.isEmpty()) {
      return;
    }

    // Initialize writer if needed
    if (this.writerHelper == null) {
      initWriterHelper();
    }

    // Drain records from TreeMap using pollFirstEntry() to avoid iterator allocation
    int actualCount = Math.min(count, sortedRecords.size());
    int drained = 0;

    while (drained < actualCount && !sortedRecords.isEmpty()) {
      Map.Entry<SortKey, RowData> entry = sortedRecords.pollFirstEntry();
      writerHelper.write(entry.getValue());
      drained++;
    }

    totalDrainOperations++;
    totalDrainedRecords += drained;
    sizeTracer.countDown(drained * estimatedRecordSize);
  }

  @Override
  public void snapshotState() {
    try {
      // Drain all remaining records and reset for next checkpoint interval
      if (!sortedRecords.isEmpty()) {
        LOG.info("Snapshot: draining {} remaining records", sortedRecords.size());
        drainRecords(sortedRecords.size());
        sortedRecords.clear();
        insertionSequence = 0L;
        sizeTracer.reset();
      }

      LOG.info("Snapshot complete: total drained={}, operations={}",
          totalDrainedRecords, totalDrainOperations);

    } catch (IOException e) {
      throw new HoodieIOException("Failed to drain buffer during snapshot", e);
    }
    super.snapshotState();
  }

  @Override
  public void endInput() {
    try {
      // Drain all remaining records and clear buffer
      if (!sortedRecords.isEmpty()) {
        LOG.info("EndInput: draining {} remaining records", sortedRecords.size());
        drainRecords(sortedRecords.size());
        sortedRecords.clear();
        insertionSequence = 0L;
        sizeTracer.reset();
      }

    } catch (IOException e) {
      throw new HoodieIOException("Failed to drain buffer during endInput", e);
    }
    super.endInput();
  }

  @Override
  public void close() throws Exception {
    try {
      LOG.info("AppendWriteFunctionWithContinuousSort closed: totalInserted={}, totalDrained={}, operations={}",
          totalInserted, totalDrainedRecords, totalDrainOperations);

    } finally {
      super.close();
    }
  }

  /**
   * Sort key with normalized key stored as a pre-wrapped MemorySegment to avoid
   * repeated allocation during TreeMap comparisons.
   * Holds a reference to the original record for full comparison fallback.
   * Comparison is done via TreeMap comparator.
   */
  private static class SortKey {
    final MemorySegment keySegment;
    final RowData record;
    final long insertionOrder;

    SortKey(MemorySegment sourceSegment, int keySize, RowData record, long insertionOrder) {
      this.record = record;
      this.insertionOrder = insertionOrder;

      // Copy normalized key and wrap as MemorySegment once to avoid per-comparison allocation
      byte[] keyBytes = new byte[keySize];
      sourceSegment.get(0, keyBytes, 0, keySize);
      this.keySegment = MemorySegmentFactory.wrap(keyBytes);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof SortKey)) {
        return false;
      }
      return this.insertionOrder == ((SortKey) obj).insertionOrder;
    }

    @Override
    public int hashCode() {
      return Long.hashCode(insertionOrder);
    }
  }
}
