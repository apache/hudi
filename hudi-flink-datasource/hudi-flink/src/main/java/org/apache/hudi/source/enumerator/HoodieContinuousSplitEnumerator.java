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

package org.apache.hudi.source.enumerator;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.metrics.FlinkStreamReadLagMetrics;
import org.apache.hudi.source.HoodieScanContext;
import org.apache.hudi.source.split.HoodieContinuousSplitBatch;
import org.apache.hudi.source.split.HoodieContinuousSplitDiscover;
import org.apache.hudi.source.split.HoodieSourceSplit;
import org.apache.hudi.source.split.HoodieSourceSplitLagInfo;
import org.apache.hudi.source.split.HoodieSourceSplitState;
import org.apache.hudi.source.split.HoodieSplitProvider;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Continuous Hoodie enumerator that discovers Hoodie splits from new Hoodie commits of upstream Hoodie table.
 */
@Slf4j
public class HoodieContinuousSplitEnumerator extends AbstractHoodieSplitEnumerator {
  private final SplitEnumeratorContext<HoodieSourceSplit> enumeratorContext;
  private final HoodieSplitProvider splitProvider;
  private final HoodieContinuousSplitDiscover splitDiscover;
  private final HoodieScanContext scanContext;

  /**
   * Instant for the last enumerated commit. Next incremental enumeration should be based off
   * this as the starting position.
   */
  private final AtomicReference<HoodieEnumeratorPosition> position;

  // Read lag metrics, null when the context has no metric group
  private final FlinkStreamReadLagMetrics lagMetrics;

  // Splits restored from the state, whose lag info is resolved once the enumerator starts, released then
  private List<HoodieSourceSplit> restoredSplits;

  public HoodieContinuousSplitEnumerator(
      String tableName,
      SplitEnumeratorContext<HoodieSourceSplit> enumeratorContext,
      HoodieSplitProvider splitProvider,
      HoodieContinuousSplitDiscover splitDiscover,
      HoodieScanContext scanContext,
      Option<HoodieSplitEnumeratorState> enumStateOpt) {
    super(tableName, enumeratorContext, splitProvider);
    this.enumeratorContext = enumeratorContext;
    this.splitProvider = splitProvider;
    this.splitDiscover = splitDiscover;
    this.scanContext = scanContext;
    this.position = new AtomicReference<>();

    if (enumStateOpt.isPresent()) {
      this.position.set(HoodieEnumeratorPosition.of(enumStateOpt.get().getLastEnumeratedInstant(), enumStateOpt.get().getLastEnumeratedInstantOffset()));
    } else {
      // We need to set the instantOffset as null for the first read. For first read, the start instant is inclusive,
      // while for continuous incremental read, the start instant is exclusive.
      this.position.set(HoodieEnumeratorPosition.of(Option.empty(), Option.empty()));
    }

    if (enumeratorContext.metricGroup() != null) {
      this.lagMetrics = new FlinkStreamReadLagMetrics(enumeratorContext.metricGroup(), tableName, splitDiscover.getTimelineZone());
      this.lagMetrics.registerMetrics();
    } else {
      this.lagMetrics = null;
    }
    this.restoredSplits = lagMetrics != null && enumStateOpt.isPresent()
        ? enumStateOpt.get().getPendingSplitStates().stream().map(HoodieSourceSplitState::getSplit).collect(Collectors.toList())
        : Collections.emptyList();
    if (!restoredSplits.isEmpty()) {
      // track the restored splits right away, so that the ones finished before their lag info is resolved are not left over
      lagMetrics.addSplits(HoodieSourceSplitLagInfo.ofSplits(restoredSplits));
    }
  }

  @Override
  public void start() {
    super.start();
    enumeratorContext.callAsync(
        this::discoverSplits,
        this::processDiscoveredSplits,
        0L,
        scanContext.getScanInterval().toMillis());
    List<HoodieSourceSplit> splits = restoredSplits;
    restoredSplits = Collections.emptyList();
    if (!splits.isEmpty()) {
      resolveLagInfo(splits, "restored");
    }
  }

  @Override
  boolean shouldWaitForMoreSplits() {
    return true;
  }

  @Override
  public HoodieSplitEnumeratorState snapshotState(long checkpointId) throws Exception {
    return new HoodieSplitEnumeratorState(splitProvider.state(), position.get().getIssuedInstant(), position.get().getIssuedOffset());
  }

  @Override
  void onSplitsFinished(Collection<String> finishedSplitIds) {
    if (lagMetrics != null) {
      lagMetrics.finishSplits(finishedSplitIds);
    }
  }

  @Override
  void onSplitsAddedBack(List<HoodieSourceSplit> splits) {
    if (lagMetrics != null) {
      lagMetrics.addSplits(HoodieSourceSplitLagInfo.ofSplits(splits));
    }
  }

  /**
   * Returns the discovered splits, with the lag info known from their ranges and the completion times
   * of the commits left to discover, which are null when the metrics are disabled or failed to be
   * resolved. The lag info read from the commit metadata is resolved once the splits are published.
   */
  private DiscoveryResult discoverSplits() {
    String issuedOffset = position.get().getIssuedOffset().orElse(null);
    int pendingSplitNumber = splitProvider.pendingSplitCount();
    if (pendingSplitNumber > scanContext.getMaxPendingSplits()) {
      log.info(
          "Pause split discovery as the assigner already has too many pending splits: {}",
          pendingSplitNumber);
      return new DiscoveryResult(HoodieContinuousSplitBatch.EMPTY, Collections.emptyMap(), getUndiscoveredCommits(issuedOffset, true));
    }
    HoodieContinuousSplitBatch batch = splitDiscover.discoverSplits(issuedOffset);
    String nextOffset = StringUtils.isNullOrEmpty(batch.getOffset()) ? issuedOffset : batch.getOffset();
    return new DiscoveryResult(batch, getRangeLagInfo(batch.getSplits()), getUndiscoveredCommits(nextOffset, false));
  }

  @Nullable
  private Map<String, HoodieSourceSplitLagInfo> getRangeLagInfo(Collection<HoodieSourceSplit> splits) {
    if (lagMetrics == null || splits.isEmpty()) {
      return null;
    }
    try {
      return splitDiscover.getRangeLagInfo(splits);
    } catch (Exception e) {
      log.warn("Failed to get the lag info of {} discovered splits from their ranges", splits.size(), e);
      return null;
    }
  }

  /**
   * Resolves the lag info of the tracked splits from the commit metadata in the worker thread, so that
   * it does not delay the splits.
   */
  private void resolveLagInfo(Collection<HoodieSourceSplit> splits, String kind) {
    enumeratorContext.callAsync(
        () -> splitDiscover.getLagInfo(splits),
        (lagInfos, throwable) -> {
          if (throwable != null) {
            log.warn("Failed to resolve the lag info of {} {} splits", splits.size(), kind, throwable);
          } else {
            lagMetrics.updateSplits(lagInfos);
          }
        });
  }

  @Nullable
  private List<String> getUndiscoveredCommits(String offset, boolean reloadTimeline) {
    if (lagMetrics == null) {
      return null;
    }
    try {
      return splitDiscover.getUndiscoveredCommits(offset, reloadTimeline);
    } catch (Exception e) {
      log.warn("Failed to get the undiscovered commits after {}", offset, e);
      return null;
    }
  }

  private void processDiscoveredSplits(DiscoveryResult discovery, Throwable throwable) {
    if (throwable != null) {
      throw new RuntimeException("Failed to discover new splits", throwable);
    }

    HoodieContinuousSplitBatch result = discovery.batch;
    if (lagMetrics != null) {
      lagMetrics.addSplits(discovery.rangeLagInfos != null ? discovery.rangeLagInfos : HoodieSourceSplitLagInfo.ofSplits(result.getSplits()));
      if (discovery.undiscoveredCommits != null) {
        lagMetrics.setUndiscoveredCommits(discovery.undiscoveredCommits);
      }
    }
    if (!result.getSplits().isEmpty()) {
      splitProvider.onDiscoveredSplits(result.getSplits());
      if (enumeratorMetrics != null) {
        position.get().getIssuedInstant().ifPresent(enumeratorMetrics::setIssuedInstant);
      }
      log.debug(
          "Added {} splits discovered between ({}, {}] to the assigner",
          result.getSplits().size(),
          position.get().getIssuedOffset(),
          result.getOffset());
    } else {
      log.debug(
          "No new splits discovered between ({}, {}]",
          position.get().getIssuedOffset(),
          result.getOffset());
    }
    // Only advance position when there is a valid new offset from the analyzed commits.
    // An empty/null offset means either no new commits were found or split discovery was paused
    // due to too many pending splits. Preserving the current position ensures the next scan
    // resumes from the correct point, which is required for READ_COMMITS_LIMIT to work correctly.
    if (!StringUtils.isNullOrEmpty(result.getOffset())) {
      position.set(HoodieEnumeratorPosition.of(result.getEndInstant(), result.getOffset()));
      log.info("Update enumerator position to {}", position.get());
    }
    if (lagMetrics != null && !result.getSplits().isEmpty()) {
      resolveLagInfo(result.getSplits(), "discovered");
    }
  }

  /**
   * Result of a discovery pass.
   */
  @AllArgsConstructor
  private static class DiscoveryResult {
    private final HoodieContinuousSplitBatch batch;
    @Nullable
    private final Map<String, HoodieSourceSplitLagInfo> rangeLagInfos;
    @Nullable
    private final List<String> undiscoveredCommits;
  }
}
