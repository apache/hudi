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
import org.apache.hudi.source.ScanContext;
import org.apache.hudi.source.split.HoodieContinuousSplitBatch;
import org.apache.hudi.source.split.HoodieContinuousSplitDiscover;
import org.apache.hudi.source.split.HoodieSourceSplit;
import org.apache.hudi.source.split.HoodieSplitProvider;

import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Continuous Hoodie enumerator that discovers Hoodie splits from new Hoodie commits of upstream Hoodie table.
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
      Option<HoodieSplitEnumeratorState> enumStateOpt) {
    super(enumeratorContext, splitProvider);
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
    return new HoodieSplitEnumeratorState(splitProvider.state(), position.get().issuedInstant(), position.get().issuedOffset());
  }

  private HoodieContinuousSplitBatch discoverSplits() {
    int pendingSplitNumber = splitProvider.pendingSplitCount();
    if (pendingSplitNumber > scanContext.getMaxPendingSplits()) {
      LOG.info(
          "Pause split discovery as the assigner already has too many pending splits: {}",
          pendingSplitNumber);
      return HoodieContinuousSplitBatch.EMPTY;
    }
    return splitDiscover.discoverSplits(position.get().issuedOffset().isPresent() ? position.get().issuedOffset().get() : null);
  }

  private void processDiscoveredSplits(HoodieContinuousSplitBatch result, Throwable throwable) {
    if (throwable != null) {
      throw new RuntimeException("Failed to discover new splits", throwable);
    }

    if (!result.getSplits().isEmpty()) {
      splitProvider.onDiscoveredSplits(result.getSplits());
      LOG.debug(
          "Added {} splits discovered between ({}, {}] to the assigner",
          result.getSplits().size(),
          position.get().issuedOffset(),
          result.getOffset());
    } else {
      LOG.debug(
          "No new splits discovered between ({}, {}]",
          position.get().issuedOffset(),
          result.getOffset());
    }
    position.set(HoodieEnumeratorPosition.of(result.getEndInstant(), result.getOffset()));
    LOG.info("Update enumerator position to {}", position.get());
  }

}
