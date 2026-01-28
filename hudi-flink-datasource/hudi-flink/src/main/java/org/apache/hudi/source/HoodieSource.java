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

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.source.enumerator.HoodieContinuousSplitEnumerator;
import org.apache.hudi.source.enumerator.HoodieEnumeratorStateSerializer;
import org.apache.hudi.source.enumerator.HoodieSplitEnumeratorState;
import org.apache.hudi.source.enumerator.HoodieStaticSplitEnumerator;
import org.apache.hudi.source.reader.HoodieRecordEmitter;
import org.apache.hudi.source.reader.HoodieSourceReader;
import org.apache.hudi.source.reader.function.SplitReaderFunction;
import org.apache.hudi.source.split.DefaultHoodieSplitDiscover;
import org.apache.hudi.source.split.DefaultHoodieSplitProvider;
import org.apache.hudi.source.split.HoodieContinuousSplitBatch;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Hoodie Source implementation.
 * @param <T> record Type
 */
public class HoodieSource<T> implements Source<T, HoodieSourceSplit, HoodieSplitEnumeratorState> {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieSource.class);

  private final ScanContext scanContext;
  private final SplitReaderFunction<T> readerFunction;
  private final SerializableComparator<HoodieSourceSplit> splitComparator;
  private final HoodieTableMetaClient metaClient;
  private final HoodieRecordEmitter<T> recordEmitter;

  HoodieSource(
      ScanContext scanContext,
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
    return new HoodieSourceReader<T>(recordEmitter, scanContext.getConf(), readerContext, readerFunction, splitComparator);
  }

  private SplitEnumerator<HoodieSourceSplit, HoodieSplitEnumeratorState> createEnumerator(
      SplitEnumeratorContext<HoodieSourceSplit> enumContext,
      @Nullable HoodieSplitEnumeratorState enumeratorState) {
    HoodieSplitProvider splitProvider;
    if (enumeratorState == null) {
      splitProvider = new DefaultHoodieSplitProvider();
    } else {
      LOG.info(
          "Hoodie source restored {} splits from state for table {}",
          enumeratorState.getPendingSplitStates().size(),
          metaClient.getTableConfig().getTableName());
      List<HoodieSourceSplit> pendingSplits =
          enumeratorState.getPendingSplitStates().stream().map(HoodieSourceSplitState::split).collect(Collectors.toList());
      splitProvider = new DefaultHoodieSplitProvider();
      splitProvider.onDiscoveredSplits(pendingSplits);
    }

    if (scanContext.isStreaming()) {
      HoodieContinuousSplitDiscover discover = new DefaultHoodieSplitDiscover(
          scanContext, metaClient);

      return new HoodieContinuousSplitEnumerator(enumContext, splitProvider, discover, scanContext, enumeratorState == null ? Option.empty() : Option.of(enumeratorState));
    } else {
      if (enumeratorState == null) {
        // Only do scan planning if nothing is restored from checkpoint state
        IncrementalInputSplits incrementalInputSplits = IncrementalInputSplits.builder()
            .conf(scanContext.getConf())
            .path(scanContext.getPath())
            .rowType(scanContext.getRowType())
            .maxCompactionMemoryInBytes(scanContext.getMaxCompactionMemoryInBytes())
            .skipCompaction(scanContext.skipCompaction())
            .skipClustering(scanContext.skipClustering())
            .skipInsertOverwrite(scanContext.skipInsertOverwrite()).build();

        HoodieContinuousSplitBatch batch = incrementalInputSplits.inputHoodieSourceSplits(metaClient, null, scanContext.cdcEnabled());
        splitProvider.onDiscoveredSplits(batch.getSplits());
      }

      return new HoodieStaticSplitEnumerator(enumContext, splitProvider);
    }
  }
}
