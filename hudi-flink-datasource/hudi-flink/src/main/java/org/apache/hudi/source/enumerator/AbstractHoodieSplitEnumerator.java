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
import org.apache.hudi.source.split.HoodieSourceSplit;
import org.apache.hudi.source.split.HoodieSplitProvider;
import org.apache.hudi.source.split.SplitRequestEvent;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SupportsHandleExecutionAttemptSourceEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Abstract class for Hoodie SourceSplit enumerator.
 */
abstract class AbstractHoodieSplitEnumerator
    implements SplitEnumerator<HoodieSourceSplit, HoodieEnumeratorState>,
    SupportsHandleExecutionAttemptSourceEvent {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractHoodieSplitEnumerator.class);

  // Split provider that provide Hoodie split for split request
  protected final HoodieSplitProvider splitProvider;
  // Context of split enumerator
  private final SplitEnumeratorContext<HoodieSourceSplit> enumeratorContext;
  // Registered reader task id to request host home map
  private final Map<Integer, String> readersAwaitingSplit;
  // Completable future for signal of new split readiness
  private final AtomicReference<CompletableFuture<Void>> availableFuture;

  AbstractHoodieSplitEnumerator(
      SplitEnumeratorContext<HoodieSourceSplit> enumeratorContext,
      HoodieSplitProvider splitProvider) {
    this.enumeratorContext = enumeratorContext;
    // Use LinkedHashMap to make sure split request are handled in request order
    this.readersAwaitingSplit = new LinkedHashMap<>();
    this.availableFuture = new AtomicReference<>();
    this.splitProvider = splitProvider;
    this.enumeratorContext
        .metricGroup()
        // This number may not capture the entire backlog due to split discovery throttling to avoid
        // excessive memory footprint. Some pending splits may not have been discovered yet.
        .setUnassignedSplitsGauge(() -> Long.valueOf(splitProvider.pendingSplitCount()));
  }

  @Override
  public void start() {
    splitProvider.start();
  }

  @Override
  public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
    readersAwaitingSplit.put(subtaskId, requesterHostname);
    assignSplits();
  }

  @Override
  public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
    if (sourceEvent instanceof SplitRequestEvent) {
      SplitRequestEvent splitRequestEvent = (SplitRequestEvent) sourceEvent;
      LOG.info("Received request split event from subtask {}", subtaskId);
      splitProvider.onCompletedSplits(splitRequestEvent.finishedSplitIds());
      readersAwaitingSplit.put(subtaskId, splitRequestEvent.requesterHostname());
      assignSplits();
    } else {
      throw new IllegalArgumentException(
          String.format(
              Locale.ROOT,
              "Received unknown event from subtask %d: %s",
              subtaskId,
              sourceEvent.getClass().getCanonicalName()));
    }
  }

  @Override
  public void addSplitsBack(List<HoodieSourceSplit> splits, int subtaskId) {
    LOG.info("Add {} splits back to the split provider for failed subtask {}", splits.size(), subtaskId);
    splitProvider.onUnassignedSplits(splits);
    assignSplits();
  }

  @Override
  public void addReader(int subtaskId) {
    LOG.info("Add reader for subtask {}", subtaskId);
  }

  @Override
  public HoodieEnumeratorState snapshotState(long checkpointId) throws Exception {
    return new HoodieEnumeratorState(splitProvider.state());
  }

  @Override
  public void close() throws IOException {
    splitProvider.close();
  }

  @Override
  public void handleSourceEvent(int subTaskId, int attemptNumber, SourceEvent sourceEvent) {
    handleSourceEvent(subTaskId, sourceEvent);
  }

  private void assignSplits() {
    LOG.info("Assigning splits for {} awaiting readers", readersAwaitingSplit.size());
    Iterator<Map.Entry<Integer, String>> awaitingReader =
        readersAwaitingSplit.entrySet().iterator();
    while (awaitingReader.hasNext()) {
      Map.Entry<Integer, String> nextAwaiting = awaitingReader.next();
      // if the reader that requested another split has failed in the meantime, remove
      // it from the list of waiting readers
      if (!enumeratorContext.registeredReaders().containsKey(nextAwaiting.getKey())) {
        awaitingReader.remove();
        continue;
      }

      int awaitingSubtask = nextAwaiting.getKey();
      String hostname = nextAwaiting.getValue();
      Option<HoodieSourceSplit> splitOption = splitProvider.getNext(hostname);
      if (splitOption.isPresent()) {
        LOG.info("Assign split to subtask {}: {}", awaitingSubtask, splitOption.get());
        enumeratorContext.assignSplit(splitOption.get(), awaitingSubtask);
        awaitingReader.remove();
      } else if (splitOption.isEmpty()) {
        if (shouldWaitForMoreSplits()) {
          // for continuous split provider callback
          registerCallbackFromSplitProvider();
          break;
        } else {
          // for static split provider
          LOG.info("No more splits available for subtask {}", awaitingSubtask);
          enumeratorContext.signalNoMoreSplits(awaitingSubtask);
          awaitingReader.remove();
        }
      }
    }
  }

  abstract boolean shouldWaitForMoreSplits();

  private synchronized void registerCallbackFromSplitProvider() {
    if (availableFuture.get() != null) {
      return;
    }

    CompletableFuture<Void> future =
        splitProvider
            .isAvailable()
            .thenAccept(
                ignore ->
                    enumeratorContext.runInCoordinatorThread(
                        () -> {
                          LOG.debug("Executing callback of new splits from provider");
                          availableFuture.set(null);
                          assignSplits();
                        }));
    availableFuture.set(future);
    LOG.debug("Registered callback for future available splits");
  }
}
