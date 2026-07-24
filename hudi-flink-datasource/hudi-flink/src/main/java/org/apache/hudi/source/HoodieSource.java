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

package org.apache.hudi.source;

import org.apache.hudi.common.function.SerializableSupplier;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.source.enumerator.HoodieContinuousSplitEnumerator;
import org.apache.hudi.source.enumerator.HoodieEnumeratorStateSerializer;
import org.apache.hudi.source.enumerator.HoodieSplitEnumeratorState;
import org.apache.hudi.source.enumerator.HoodieStaticSplitEnumerator;
import org.apache.hudi.source.reader.HoodieRecordEmitter;
import org.apache.hudi.source.reader.HoodieSourceReader;
import org.apache.hudi.source.reader.function.SplitReaderFunction;
import org.apache.hudi.source.split.DefaultHoodieSplitDiscover;
import org.apache.hudi.source.split.DefaultHoodieSplitProvider;
import org.apache.hudi.source.split.HoodieContinuousSplitDiscover;
import org.apache.hudi.source.split.HoodieSourceSplit;
import org.apache.hudi.source.split.HoodieSourceSplitSerializer;
import org.apache.hudi.source.split.HoodieSourceSplitState;
import org.apache.hudi.source.split.HoodieSplitProvider;
import org.apache.hudi.source.split.SerializableComparator;
import org.apache.hudi.source.split.assign.HoodieSplitAssigner;
import org.apache.hudi.source.split.assign.HoodieSplitAssigners;
import org.apache.hudi.util.FileIndexReader;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Hudi Flink Source V2 implementation for Flink streaming and batch reads.
 *
 * <p>This source supports both bounded (batch) and unbounded (streaming) modes
 * based on the configuration. It uses Flink's new Source API @see FLIP-27 to
 * provide efficient reading of Hudi tables.
 *
 * @param <T> the record type to emit
 */
@Slf4j
public class HoodieSource<T> extends FileIndexReader implements Source<T, HoodieSourceSplit, HoodieSplitEnumeratorState> {
  /** Recorded in place of a {@code read.*-commit} bound that is not configured. */
  private static final String UNSET = "";

  private final HoodieScanContext scanContext;
  private final SerializableSupplier<SplitReaderFunction<T>> readerFunctionSupplier;
  private final SerializableComparator<HoodieSourceSplit> splitComparator;
  private final HoodieTableMetaClient metaClient;
  private final HoodieRecordEmitter<T> recordEmitter;
  private final String tableName;

  public HoodieSource(
      HoodieScanContext scanContext,
      SerializableSupplier<SplitReaderFunction<T>> readerFunctionSupplier,
      SerializableComparator<HoodieSourceSplit> splitComparator,
      HoodieTableMetaClient metaClient,
      HoodieRecordEmitter<T> recordEmitter) {
    ValidationUtils.checkArgument(scanContext != null, "scanContext can't be null.");
    ValidationUtils.checkArgument(readerFunctionSupplier != null, "readerFunctionSupplier can't be null.");
    ValidationUtils.checkArgument(splitComparator != null, "splitComparator can't be null.");
    ValidationUtils.checkArgument(metaClient != null, "metaClient can't be null.");
    ValidationUtils.checkArgument(recordEmitter != null, "recordEmitter can't be null.");

    this.scanContext = scanContext;
    this.readerFunctionSupplier = readerFunctionSupplier;
    this.splitComparator = splitComparator;
    this.metaClient = metaClient;
    this.recordEmitter = recordEmitter;
    this.tableName = metaClient.getTableConfig().getTableName();
  }

  @Override
  public Boundedness getBoundedness() {
    return scanContext.isStreaming() ? Boundedness.CONTINUOUS_UNBOUNDED : Boundedness.BOUNDED;
  }

  @Override
  public SplitEnumerator<HoodieSourceSplit, HoodieSplitEnumeratorState> createEnumerator(SplitEnumeratorContext<HoodieSourceSplit> enumContext) throws Exception {
    return createEnumerator(enumContext, null);
  }

  @Override
  public SplitEnumerator<HoodieSourceSplit, HoodieSplitEnumeratorState> restoreEnumerator(SplitEnumeratorContext<HoodieSourceSplit> enumContext, HoodieSplitEnumeratorState enumeratorState)
      throws Exception {
    return createEnumerator(enumContext, enumeratorState);
  }

  @Override
  public SimpleVersionedSerializer<HoodieSourceSplit> getSplitSerializer() {
    return new HoodieSourceSplitSerializer();
  }

  @Override
  public SimpleVersionedSerializer<HoodieSplitEnumeratorState> getEnumeratorCheckpointSerializer() {
    return new HoodieEnumeratorStateSerializer();
  }

  @Override
  public SourceReader<T, HoodieSourceSplit> createReader(SourceReaderContext readerContext) throws Exception {
    return new HoodieSourceReader<T>(
        tableName, recordEmitter, scanContext, readerContext, readerFunctionSupplier, splitComparator);
  }

  private SplitEnumerator<HoodieSourceSplit, HoodieSplitEnumeratorState> createEnumerator(
      SplitEnumeratorContext<HoodieSourceSplit> enumContext,
      @Nullable HoodieSplitEnumeratorState enumeratorState) {
    boolean streaming = scanContext.isStreaming();

    // read.start-commit / read.end-commit as configured for this run. They are recorded in the
    // checkpoint of a bounded read and re-checked on the next restore. UNSET stands in for an option
    // that is not configured, so that "recorded but unset" stays distinguishable from "not recorded
    // at all" (a streaming read, or a checkpoint written before this field existed).
    Configuration conf = scanContext.getConf();
    Option<String> readStartCommit = Option.of(conf.getOptional(FlinkOptions.READ_START_COMMIT).orElse(UNSET));
    Option<String> readEndCommit = Option.of(conf.getOptional(FlinkOptions.READ_END_COMMIT).orElse(UNSET));
    // Deferred commit-range failure, raised from the enumerator's start() rather than here. See
    // checkBoundedCommitRangeUnchanged for why the restore path cannot fail the job itself.
    Option<String> rangeFailure = Option.empty();

    HoodieSplitProvider splitProvider;
    HoodieSplitAssigner splitAssigner = HoodieSplitAssigners.createHoodieSplitAssigner(
            conf, enumContext.currentParallelism());

    if (enumeratorState == null) {
      splitProvider = new DefaultHoodieSplitProvider(splitAssigner);
    } else {
      if (!streaming) {
        rangeFailure = checkBoundedCommitRangeUnchanged(
            tableName, enumeratorState, readStartCommit, readEndCommit);
      }
      log.info(
          "Hoodie source restored {} splits from state for table {}",
          enumeratorState.getPendingSplitStates().size(), tableName);
      List<HoodieSourceSplit> pendingSplits =
          enumeratorState.getPendingSplitStates().stream().map(HoodieSourceSplitState::getSplit).collect(Collectors.toList());
      splitProvider = new DefaultHoodieSplitProvider(splitAssigner);
      splitProvider.onDiscoveredSplits(pendingSplits);
    }

    if (streaming) {
      HoodieContinuousSplitDiscover discover = new DefaultHoodieSplitDiscover(
          scanContext);

      return new HoodieContinuousSplitEnumerator(
              tableName, enumContext, splitProvider, discover, scanContext,
              enumeratorState == null ? Option.empty() : Option.of(enumeratorState));
    } else {
      if (enumeratorState == null) {
        List<HoodieSourceSplit> splits = createBatchHoodieSplits();
        splitProvider.onDiscoveredSplits(splits);
      }
      return new HoodieStaticSplitEnumerator(
          tableName, enumContext, splitProvider, readStartCommit, readEndCommit, rangeFailure);
    }
  }

  /**
   * Returns the failure message for a bounded read being restored from a checkpoint taken under a
   * different {@code read.start-commit} / {@code read.end-commit} range, or {@link Option#empty()}
   * when the range is unchanged or cannot be verified.
   *
   * <p>A bounded read enumerates its splits once, at job start, and a restore reuses that persisted
   * split set without re-enumerating. Resuming after the range was edited would therefore read the
   * checkpoint's old range and silently ignore the configured one.
   *
   * <p>The message is returned rather than thrown, and is raised by {@link
   * HoodieStaticSplitEnumerator#start()} instead. Throwing from here does not fail the job on the
   * case that matters — the initial restore from a savepoint or retained checkpoint. As
   * {@code OperatorCoordinatorHolder#resetToCheckpoint} documents, that first call happens during
   * ExecutionGraph construction, before {@code lazyInitialize} has supplied the scheduler executor.
   * {@code RecreateOnResetOperatorCoordinator$DeferrableCoordinator#resetAndStart} does catch the
   * throw and call {@code cleanAndFailJob}, but the {@code failJob} underneath it begins with
   * {@code checkInitialized()}, which at that point throws {@code IllegalStateException} from inside
   * the unobserved {@code closingFuture.whenComplete(...)} callback. The job then comes up RUNNING
   * with a coordinator that never started: no splits, no throughput, and checkpoints that never
   * complete. By {@code start()} the context is initialized —
   * {@code OperatorCoordinatorHolder#start()} asserts it — so the failure reaches
   * {@code context.failJob} and terminates the job.
   *
   * <p>A state carrying no range at all cannot be verified and is allowed through with a warning, so
   * that checkpoints written by serializer VERSION 1 stay restorable. Note that a VERSION 2 state
   * written by the streaming enumerator also carries no range, so a streaming-to-bounded
   * reconfiguration takes the same unverified path.
   */
  @VisibleForTesting
  static Option<String> checkBoundedCommitRangeUnchanged(
      String tableName,
      HoodieSplitEnumeratorState state,
      Option<String> configuredStart,
      Option<String> configuredEnd) {
    Option<String> checkpointedStart = state.getReadStartCommit();
    Option<String> checkpointedEnd = state.getReadEndCommit();
    if (!checkpointedStart.isPresent() && !checkpointedEnd.isPresent()) {
      log.warn(
          "Restoring bounded read for table {} from a checkpoint that records no commit range "
              + "(written by serializer VERSION 1, or by a streaming read). Cannot verify it was "
              + "taken with the configured range [{}, {}]; resuming anyway.",
          tableName, configuredStart.orElse(UNSET), configuredEnd.orElse(UNSET));
      return Option.empty();
    }
    if (boundsMatch(checkpointedStart, configuredStart) && boundsMatch(checkpointedEnd, configuredEnd)) {
      return Option.empty();
    }
    return Option.of(String.format(
        "Refusing to resume bounded read for table %s: read.start-commit/read.end-commit changed "
            + "since the checkpoint was taken.%n  checkpoint range: [%s, %s]%n"
            + "  configured range: [%s, %s]%n"
            + "A bounded read enumerates its splits once at job start and reuses them on restore, so "
            + "resuming would read the checkpoint's range and ignore the configured one. To read the "
            + "new range, start the job fresh instead of resuming: submit without a savepoint or "
            + "retained checkpoint, or use a new checkpoint directory.",
        tableName,
        checkpointedStart.orElse(UNSET), checkpointedEnd.orElse(UNSET),
        configuredStart.orElse(UNSET), configuredEnd.orElse(UNSET)));
  }

  /**
   * Whether two recorded bounds select the same data. Compared after normalization so that a purely
   * cosmetic edit does not fail the job: the {@code earliest} sentinel is matched case-insensitively,
   * the way {@link org.apache.hudi.configuration.OptionsResolver} matches it everywhere else, and
   * surrounding whitespace is ignored.
   */
  private static boolean boundsMatch(Option<String> checkpointed, Option<String> configured) {
    return normalizeBound(checkpointed).equals(normalizeBound(configured));
  }

  private static Option<String> normalizeBound(Option<String> bound) {
    return bound.map(value -> {
      String trimmed = value.trim();
      return trimmed.equalsIgnoreCase(FlinkOptions.START_COMMIT_EARLIEST)
          ? FlinkOptions.START_COMMIT_EARLIEST
          : trimmed;
    });
  }

  @VisibleForTesting
  List<HoodieSourceSplit> createBatchHoodieSplits() {
    final Configuration flinkConf = this.scanContext.getConf();
    final String queryType = flinkConf.get(FlinkOptions.QUERY_TYPE);
    switch (queryType) {
      case FlinkOptions.QUERY_TYPE_SNAPSHOT:
        final HoodieTableType tableType = HoodieTableType.valueOf(flinkConf.get(FlinkOptions.TABLE_TYPE));
        switch (tableType) {
          case MERGE_ON_READ:
            List<HoodieSourceSplit> splits = buildHoodieSplits(metaClient, flinkConf);
            if (splits.isEmpty()) {
              // When there is no input splits, just return an empty source.
              log.info("No input splits generate for MERGE_ON_READ input format. Returning empty collection");
            }
            return splits;
          case COPY_ON_WRITE:
            return baseFileOnlyHoodieSourceSplits(metaClient, scanContext.getPath(), flinkConf.get(FlinkOptions.MERGE_TYPE));
          default:
            throw new HoodieException("Unexpected table type: " + flinkConf.get(FlinkOptions.TABLE_TYPE));
        }
      case FlinkOptions.QUERY_TYPE_READ_OPTIMIZED:
        return baseFileOnlyHoodieSourceSplits(metaClient, scanContext.getPath(), flinkConf.get(FlinkOptions.MERGE_TYPE));
      case FlinkOptions.QUERY_TYPE_INCREMENTAL:
        IncrementalInputSplits incrementalInputSplits = IncrementalInputSplits.builder()
            .conf(scanContext.getConf())
            .path(new Path(scanContext.getPath().toUri()))
            .rowType(scanContext.getRowType())
            .maxCompactionMemoryInBytes(scanContext.getMaxCompactionMemoryInBytes())
            .skipCompaction(scanContext.isSkipCompaction())
            .skipClustering(scanContext.isSkipClustering())
            .partitionPruner(scanContext.getPartitionPruner())
            .skipInsertOverwrite(scanContext.isSkipInsertOverwrite()).build();
        return new ArrayList<>(incrementalInputSplits.batchHoodieSourceSplits(metaClient, scanContext.isCdcEnabled()).getSplits());
      default:
        throw new HoodieException("Unsupported query type: " + queryType);
    }
  }

  @Override
  protected FileIndex buildFileIndex() {
    return FileIndex.builder()
        .path(scanContext.getPath())
        .conf(this.scanContext.getConf())
        .rowType(scanContext.getRowType())
        .metaClient(metaClient)
        .columnStatsProbe(scanContext.getColumnStatsProbe())
        .partitionPruner(scanContext.getPartitionPruner())
        .partitionBucketIdFunc(scanContext.getPartitionBucketIdFunc())
        .build();
  }
}
