/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.table.log;

import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.HoodieFileGroupRecordBuffer;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.expression.Expression;
import org.apache.hudi.expression.Predicate;
import org.apache.hudi.expression.Predicates;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.hudi.common.fs.FSUtils.getRelativePartitionPath;

/**
 * A log record reader that merges the records with the same record key.
 *
 * @param <T> type of engine-specific record representation.
 */
public class HoodieMergedLogRecordReader<T> extends BaseHoodieLogRecordReader<T>
    implements Iterable<BufferedRecord<T>>, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieMergedLogRecordReader.class);
  // A timer for calculating elapsed time in millis
  public final HoodieTimer timer = HoodieTimer.create();
  // count of merged records in log
  private long numMergedRecordsInLog;
  // Stores the total time taken to perform reading and merging of log blocks
  private long totalTimeTakenToReadAndMergeBlocks;

  @SuppressWarnings("unchecked")
  private HoodieMergedLogRecordReader(HoodieReaderContext<T> readerContext, HoodieTableMetaClient metaClient, HoodieStorage storage, List<String> logFilePaths, boolean reverseReader,
                                      int bufferSize, Option<InstantRange> instantRange, boolean withOperationField, boolean forceFullScan,
                                      Option<String> partitionName, Option<String> keyFieldOverride, boolean enableOptimizedLogBlocksScan,
                                      HoodieFileGroupRecordBuffer<T> recordBuffer, boolean allowInflightInstants) {
    super(readerContext, metaClient, storage, logFilePaths, reverseReader, bufferSize, instantRange, withOperationField,
        forceFullScan, partitionName, keyFieldOverride, enableOptimizedLogBlocksScan, recordBuffer, allowInflightInstants);

    if (forceFullScan) {
      performScan();
    }
  }

  /**
   * Scans delta-log files processing blocks
   */
  public final void scan() {
    scan(false);
  }

  public final void scan(boolean skipProcessingBlocks) {
    if (forceFullScan) {
      // NOTE: When full-scan is enforced, scanning is invoked upfront (during initialization)
      return;
    }

    scanInternal(Option.empty(), skipProcessingBlocks);
  }

  private void performScan() {
    // Do the scan and merge
    timer.startTimer();

    Option<KeySpec> keySpecOpt = createKeySpec(readerContext.getKeyFilterOpt());
    scanInternal(keySpecOpt, false);

    this.totalTimeTakenToReadAndMergeBlocks = timer.endTimer();
    this.numMergedRecordsInLog = recordBuffer.size();

    LOG.info("Number of log files scanned => {}", logFilePaths.size());
    LOG.info("Number of entries in Map => {}", recordBuffer.size());
  }

  static Option<KeySpec> createKeySpec(Option<Predicate> filter) {
    if (filter.isEmpty()) {
      return Option.empty();
    }
    if (filter.get().getOperator() == Expression.Operator.IN) {
      List<Expression> rightChildren = ((Predicates.In) filter.get()).getRightChildren();
      List<String> keyOrPrefixes = rightChildren.stream()
          .map(e -> (String) e.eval(null)).collect(Collectors.toList());
      return Option.of(new FullKeySpec(keyOrPrefixes));
    } else if (filter.get().getOperator() == Expression.Operator.STARTS_WITH) {
      List<Expression> rightChildren = ((Predicates.StringStartsWithAny) filter.get()).getRightChildren();
      List<String> keyOrPrefixes = rightChildren.stream()
          .map(e -> (String) e.eval(null)).collect(Collectors.toList());
      return Option.of(new PrefixKeySpec(keyOrPrefixes));
    } else {
      return Option.empty();
    }
  }

  @Override
  public Iterator<BufferedRecord<T>> iterator() {
    return recordBuffer.getLogRecordIterator();
  }

  public Map<Serializable, BufferedRecord<T>> getRecords() {
    return recordBuffer.getLogRecords();
  }

  public long getNumMergedRecordsInLog() {
    return numMergedRecordsInLog;
  }

  /**
   * Returns the builder for {@code HoodieMergedLogRecordReader}.
   */
  public static <T> Builder<T> newBuilder() {
    return new Builder<>();
  }

  public long getTotalTimeTakenToReadAndMergeBlocks() {
    return totalTimeTakenToReadAndMergeBlocks;
  }

  @Override
  public void close() {
    // No op.
  }

  /**
   * Builder used to build {@code HoodieUnMergedLogRecordScanner}.
   */
  public static class Builder<T> extends BaseHoodieLogRecordReader.Builder<T> {
    private HoodieReaderContext<T> readerContext;
    private HoodieStorage storage;
    private List<String> logFilePaths;
    private boolean reverseReader;
    private int bufferSize;
    // specific configurations
    private Long maxMemorySizeInBytes;
    // incremental filtering
    private Option<InstantRange> instantRange = Option.empty();
    private String partitionName;
    // operation field default false
    private boolean withOperationField = false;
    private String keyFieldOverride;
    // By default, we're doing a full-scan
    private boolean forceFullScan = true;
    private boolean enableOptimizedLogBlocksScan = false;

    private HoodieFileGroupRecordBuffer<T> recordBuffer;
    private boolean allowInflightInstants = false;
    private HoodieTableMetaClient metaClient;

    @Override
    public Builder<T> withHoodieReaderContext(HoodieReaderContext<T> readerContext) {
      this.readerContext = readerContext;
      return this;
    }

    @Override
    public Builder<T> withStorage(HoodieStorage storage) {
      this.storage = storage;
      return this;
    }

    @Override
    public Builder<T> withLogFiles(List<HoodieLogFile> hoodieLogFiles) {
      this.logFilePaths = hoodieLogFiles.stream()
          .filter(l -> !l.isCDC())
          .map(l -> l.getPath().toString())
          .collect(Collectors.toList());
      return this;
    }

    @Override
    public Builder<T> withReverseReader(boolean reverseReader) {
      this.reverseReader = reverseReader;
      return this;
    }

    @Override
    public Builder<T> withBufferSize(int bufferSize) {
      this.bufferSize = bufferSize;
      return this;
    }

    @Override
    public Builder<T> withInstantRange(Option<InstantRange> instantRange) {
      this.instantRange = instantRange;
      return this;
    }

    public Builder<T> withOperationField(boolean withOperationField) {
      this.withOperationField = withOperationField;
      return this;
    }

    @Override
    public Builder<T> withPartition(String partitionName) {
      this.partitionName = partitionName;
      return this;
    }

    @Override
    public Builder<T> withOptimizedLogBlocksScan(boolean enableOptimizedLogBlocksScan) {
      this.enableOptimizedLogBlocksScan = enableOptimizedLogBlocksScan;
      return this;
    }

    public Builder<T> withKeyFieldOverride(String keyFieldOverride) {
      this.keyFieldOverride = Objects.requireNonNull(keyFieldOverride);
      return this;
    }

    public Builder<T> withForceFullScan(boolean forceFullScan) {
      this.forceFullScan = forceFullScan;
      return this;
    }

    public Builder<T> withRecordBuffer(HoodieFileGroupRecordBuffer<T> recordBuffer) {
      this.recordBuffer = recordBuffer;
      return this;
    }

    public Builder<T> withAllowInflightInstants(boolean allowInflightInstants) {
      this.allowInflightInstants = allowInflightInstants;
      return this;
    }

    public Builder<T> withMetaClient(HoodieTableMetaClient metaClient) {
      this.metaClient = metaClient;
      return this;
    }

    @Override
    public HoodieMergedLogRecordReader<T> build() {
      ValidationUtils.checkArgument(recordBuffer != null, "Record Buffer is null in Merged Log Record Reader");
      ValidationUtils.checkArgument(readerContext != null, "Reader Context is null in Merged Log Record Reader");
      if (this.partitionName == null && CollectionUtils.nonEmpty(this.logFilePaths)) {
        this.partitionName = getRelativePartitionPath(
            new StoragePath(readerContext.getTablePath()), new StoragePath(this.logFilePaths.get(0)).getParent());
      }

      return new HoodieMergedLogRecordReader<>(
          readerContext, metaClient, storage, logFilePaths,
          reverseReader, bufferSize, instantRange,
          withOperationField, forceFullScan,
          Option.ofNullable(partitionName),
          Option.ofNullable(keyFieldOverride),
          enableOptimizedLogBlocksScan, recordBuffer,
          allowInflightInstants);
    }
  }
}
