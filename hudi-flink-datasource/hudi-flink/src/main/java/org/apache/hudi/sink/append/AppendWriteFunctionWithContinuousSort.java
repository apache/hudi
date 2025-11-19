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

import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.sink.StreamWriteOperatorCoordinator;
import org.apache.hudi.sink.bulk.sort.SortOperatorGen;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.sort.SortCodeGenerator;
import org.apache.flink.table.runtime.generated.GeneratedNormalizedKeyComputer;
import org.apache.flink.table.runtime.generated.NormalizedKeyComputer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Sink function to write data with continuous sorting for improved compression.
 *
 * <p>Unlike {@link AppendWriteFunctionWithBufferSort} which uses batch sorting,
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

  // Sort key computation
  private transient NormalizedKeyComputer normalizedKeyComputer;
  private transient byte[] reusableKeyBuffer;
  private transient MemorySegment reusableKeySegment;
  private transient int normalizedKeySize;

  // Metrics
  private transient long totalDrainOperations;
  private transient long totalDrainedRecords;
  private transient long totalInserted;

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
    super.open(parameters);

    // Validate configuration
    if (maxCapacity <= 0) {
      throw new IllegalArgumentException(
          String.format("Buffer capacity must be positive, got: %d", maxCapacity));
    }

    if (drainSize <= 0) {
      throw new IllegalArgumentException(
          String.format("Drain size must be positive, got: %d", drainSize));
    }

    // Parse and validate sort keys
    String sortKeys = config.get(FlinkOptions.WRITE_BUFFER_SORT_KEYS);
    if (sortKeys == null || sortKeys.trim().isEmpty()) {
      throw new IllegalArgumentException("Sort keys cannot be null or empty for continuous sort");
    }

    List<String> sortKeyList = Arrays.stream(sortKeys.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toList());

    if (sortKeyList.isEmpty()) {
      throw new IllegalArgumentException(
          String.format("Sort keys list is empty after parsing: '%s'", sortKeys));
    }

    LOG.info("Initializing continuous sort with keys: {}", sortKeyList);

    // Create sort code generator for normalized key computation
    SortOperatorGen sortOperatorGen = new SortOperatorGen(rowType, sortKeyList.toArray(new String[0]));
    SortCodeGenerator codeGenerator = sortOperatorGen.createSortCodeGenerator();
    GeneratedNormalizedKeyComputer generatedKeyComputer = codeGenerator.generateNormalizedKeyComputer("ContinuousSortKeyComputer");

    // Instantiate code-generated normalizedKeyComputer
    this.normalizedKeyComputer = generatedKeyComputer.newInstance(Thread.currentThread().getContextClassLoader());
    this.normalizedKeySize = normalizedKeyComputer.getNumKeyBytes();

    // Initialize TreeMap with comparator that has access to normalizedKeyComputer
    // Note: We wrap byte arrays in MemorySegments for comparison
    this.sortedRecords = new TreeMap<>((k1, k2) -> {
      // Wrap byte arrays in MemorySegments for comparison
      MemorySegment seg1 = MemorySegmentFactory.wrap(k1.keyBytes);
      MemorySegment seg2 = MemorySegmentFactory.wrap(k2.keyBytes);

      int cmp = normalizedKeyComputer.compareKey(seg1, 0, seg2, 0);
      if (cmp != 0) {
        return cmp;
      }
      return Long.compare(k1.insertionOrder, k2.insertionOrder);
    });
    this.insertionSequence = 0L;

    // Allocate reusable on-heap buffer for computing keys
    // Using heap memory to avoid off-heap memory leak
    this.reusableKeyBuffer = new byte[normalizedKeySize];
    this.reusableKeySegment = MemorySegmentFactory.wrap(reusableKeyBuffer);

    // Initialize metrics
    this.totalDrainOperations = 0;
    this.totalDrainedRecords = 0;
    this.totalInserted = 0;

    LOG.info("AppendWriteFunctionWithContinuousSort initialized successfully");
  }

  @Override
  public void processElement(T value, Context ctx, Collector<RowData> out) throws Exception {
    RowData data = (RowData) value;

    // Check if buffer has reached max capacity
    if (sortedRecords.size() >= maxCapacity) {
      drainRecords(drainSize);

      // Verify there's space after draining
      if (sortedRecords.size() >= maxCapacity) {
        throw new HoodieException(
            String.format("Buffer cannot accept record after draining. "
                + "Buffer size: %d, maxCapacity: %d, drainSize: %d",
                sortedRecords.size(), maxCapacity, drainSize));
      }
    }

    // Write to buffer (maintains sorted order)
    // Compute normalized key into reusable segment
    normalizedKeyComputer.putKey(data, reusableKeySegment, 0);

    // Create sort key (copies the normalized key from reusable segment)
    SortKey key = new SortKey(reusableKeySegment, normalizedKeySize, insertionSequence++);

    // Store the original RowData
    sortedRecords.put(key, data);

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

    // Drain records from TreeMap
    int actualCount = Math.min(count, sortedRecords.size());
    int drained = 0;

    Iterator<Map.Entry<SortKey, RowData>> iterator = sortedRecords.entrySet().iterator();
    while (iterator.hasNext() && drained < actualCount) {
      Map.Entry<SortKey, RowData> entry = iterator.next();
      RowData record = entry.getValue();

      // Write record
      writerHelper.write(record);

      // Remove from TreeMap - memory immediately reclaimed
      iterator.remove();
      drained++;
    }

    totalDrainOperations++;
    totalDrainedRecords += drained;
  }

  @Override
  public void snapshotState() {
    try {
      // Drain all remaining records
      if (!sortedRecords.isEmpty()) {
        LOG.info("Snapshot: draining {} remaining records", sortedRecords.size());
        drainRecords(sortedRecords.size());
      }

      // Reset for next checkpoint interval
      sortedRecords.clear();
      insertionSequence = 0L;

      LOG.info("Snapshot complete: total drained={}, operations={}",
          totalDrainedRecords, totalDrainOperations);

    } catch (IOException e) {
      throw new HoodieIOException("Failed to drain buffer during snapshot", e);
    } finally {
      super.snapshotState();
    }
  }

  @Override
  public void endInput() {
    try {
      // Drain all remaining records
      if (!sortedRecords.isEmpty()) {
        LOG.info("EndInput: draining {} remaining records", sortedRecords.size());
        drainRecords(sortedRecords.size());
      }

      // Clear buffer and reset sequence
      sortedRecords.clear();
      insertionSequence = 0L;

    } catch (IOException e) {
      throw new HoodieIOException("Failed to drain buffer during endInput", e);
    } finally {
      super.endInput();
    }
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
   * Sort key with normalized key stored in byte array (on-heap).
   * Comparison is done via TreeMap comparator.
   */
  private static class SortKey {
    final byte[] keyBytes;
    final long insertionOrder;

    SortKey(MemorySegment sourceSegment, int keySize, long insertionOrder) {
      this.insertionOrder = insertionOrder;

      // Copy normalized key from MemorySegment to on-heap byte array
      this.keyBytes = new byte[keySize];
      sourceSegment.get(0, keyBytes, 0, keySize);
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
