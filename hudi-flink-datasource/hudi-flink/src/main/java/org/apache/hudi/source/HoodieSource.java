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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.source.assign.HoodieSplitAssigner;
import org.apache.hudi.source.assign.HoodieSplitAssigners;
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

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.hudi.util.FileIndexReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class HoodieSource<T> extends FileIndexReader implements Source<T, HoodieSourceSplit, HoodieSplitEnumeratorState> {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieSource.class);

  private final HoodieScanContext scanContext;
  private final SplitReaderFunction<T> readerFunction;
  private final SerializableComparator<HoodieSourceSplit> splitComparator;
  private final HoodieTableMetaClient metaClient;
  private final HoodieRecordEmitter<T> recordEmitter;
  private final String tableName;

  public HoodieSource(
      HoodieScanContext scanContext,
      SplitReaderFunction<T> readerFunction,
      SerializableComparator<HoodieSourceSplit> splitComparator,
      HoodieTableMetaClient metaClient,
      HoodieRecordEmitter<T> recordEmitter) {
    ValidationUtils.checkArgument(scanContext != null, "scanContext can't be null.");
    ValidationUtils.checkArgument(readerFunction != null, "readerFunction can't be null.");
    ValidationUtils.checkArgument(splitComparator != null, "splitComparator can't be null.");
    ValidationUtils.checkArgument(metaClient != null, "metaClient can't be null.");
    ValidationUtils.checkArgument(recordEmitter != null, "recordEmitter can't be null.");

    this.scanContext = scanContext;
    this.readerFunction = readerFunction;
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
    return new HoodieSourceReader<T>(tableName, recordEmitter, scanContext.getConf(), readerContext, readerFunction, splitComparator);
  }

  private SplitEnumerator<HoodieSourceSplit, HoodieSplitEnumeratorState> createEnumerator(
      SplitEnumeratorContext<HoodieSourceSplit> enumContext,
      @Nullable HoodieSplitEnumeratorState enumeratorState) {
    HoodieSplitProvider splitProvider;
    HoodieSplitAssigner splitAssigner = HoodieSplitAssigners.createHoodieSplitAssigner(
            scanContext.getConf(), enumContext.currentParallelism());

    if (enumeratorState == null) {
      splitProvider = new DefaultHoodieSplitProvider(splitAssigner);
    } else {
      LOG.info(
          "Hoodie source restored {} splits from state for table {}",
          enumeratorState.getPendingSplitStates().size(), tableName);
      List<HoodieSourceSplit> pendingSplits =
          enumeratorState.getPendingSplitStates().stream().map(HoodieSourceSplitState::getSplit).collect(Collectors.toList());
      splitProvider = new DefaultHoodieSplitProvider(splitAssigner);
      splitProvider.onDiscoveredSplits(pendingSplits);
    }

    if (scanContext.isStreaming()) {
      HoodieContinuousSplitDiscover discover = new DefaultHoodieSplitDiscover(
          scanContext, metaClient);

      return new HoodieContinuousSplitEnumerator(
              tableName, enumContext, splitProvider, discover, scanContext,
              enumeratorState == null ? Option.empty() : Option.of(enumeratorState));
    } else {
      if (enumeratorState == null) {
        List<HoodieSourceSplit> splits = createBatchHoodieSplits();
        splitProvider.onDiscoveredSplits(splits);
      }
      return new HoodieStaticSplitEnumerator(tableName, enumContext, splitProvider);
    }
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
              LOG.info("No input splits generate for MERGE_ON_READ input format. Returning empty collection");
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
        .partitionPruner(scanContext.getPartitionPruner())
        .build();
  }
}
