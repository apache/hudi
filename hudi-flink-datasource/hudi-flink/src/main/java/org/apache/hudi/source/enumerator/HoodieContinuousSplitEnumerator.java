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

import org.apache.hudi.source.ScanContext;
import org.apache.hudi.source.split.HoodieContinuousSplitBatch;
import org.apache.hudi.source.split.HoodieContinuousSplitDiscover;
import org.apache.hudi.source.split.HoodieSourceSplit;
import org.apache.hudi.source.split.HoodieSplitProvider;

import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Continuous Hoodie enumerator that discovers Hoodie splits from new Hudi commits of upstream Hoodie table.
 */
public class HoodieContinuousSplitEnumerator extends AbstractHoodieSplitEnumerator {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieContinuousSplitEnumerator.class);

  private final SplitEnumeratorContext<HoodieSourceSplit> enumeratorContext;
  private final HoodieSplitProvider splitProvider;
  private final HoodieContinuousSplitDiscover splitDiscover;
  private final ScanContext scanContext;

  /**
   * Instant for the last enumerated commit. Next incremental enumeration should be based off
   * this as the starting position.
   */
  private final AtomicReference<HoodieEnumeratorPosition> position;

  public HoodieContinuousSplitEnumerator(
      SplitEnumeratorContext<HoodieSourceSplit> enumeratorContext,
      HoodieSplitProvider splitProvider,
      HoodieContinuousSplitDiscover splitDiscover,
      ScanContext scanContext,
      @Nullable HoodieSplitEnumeratorState enumState) {
    super(enumeratorContext, splitProvider);
    this.enumeratorContext = enumeratorContext;
    this.splitProvider = splitProvider;
    this.splitDiscover = splitDiscover;
    this.scanContext = scanContext;
    this.position = new AtomicReference<>();

    if (enumState != null) {
      this.position.set(HoodieEnumeratorPosition.of(enumState.getLastEnumeratedInstant(), enumState.getLastEnumeratedInstantCompletionTime()));
    } else {
      this.position.set(HoodieEnumeratorPosition.of(scanContext.getStartInstant(), scanContext.getStartInstant()));
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
  }

  @Override
  boolean shouldWaitForMoreSplits() {
    return true;
  }

  @Override
  public HoodieSplitEnumeratorState snapshotState(long checkpointId) throws Exception {
    return new HoodieSplitEnumeratorState(splitProvider.state(), position.get().lastInstant(), position.get().lastInstantCompletionTime());
  }

  private HoodieContinuousSplitBatch discoverSplits() {
    int pendingSplitNumber = splitProvider.pendingSplitCount();
    if (pendingSplitNumber > scanContext.getMaxPendingSplits()) {
      LOG.info(
          "Pause split discovery as the assigner already has too many pending splits: {}",
          pendingSplitNumber);
      return HoodieContinuousSplitBatch.EMPTY;
    }
    return splitDiscover.discoverSplits(position.get().lastInstantCompletionTime());
  }

  private void processDiscoveredSplits(HoodieContinuousSplitBatch result, Throwable throwable) {
    if (throwable == null) {
      if (!Objects.equals(result.getFromInstant(), position.get().lastInstantCompletionTime())) {
        LOG.info(
            "Skip {} discovered splits because the scan starting position doesn't match "
                + "the current enumerator position: enumerator position = {}, scan starting position = {}",
            result.getSplits().size(),
            position.get().lastInstantCompletionTime(),
            result.getFromInstant());
      } else {
        if (!result.getSplits().isEmpty()) {
          splitProvider.onDiscoveredSplits(result.getSplits());
          LOG.info(
              "Added {} splits discovered between ({}, {}] to the assigner",
              result.getSplits().size(),
              result.getFromInstant(),
              result.getToInstant());
        } else {
          LOG.info(
              "No new splits discovered between ({}, {}]",
              result.getFromInstant(),
              result.getToInstant());
        }
        position.set(HoodieEnumeratorPosition.of(position.get().lastInstantCompletionTime(), result.getToInstant()));
        LOG.info("Update enumerator position to {}", position.get());
      }
    } else {
      throw new RuntimeException("Failed to discover new splits", throwable);
    }
  }
}
