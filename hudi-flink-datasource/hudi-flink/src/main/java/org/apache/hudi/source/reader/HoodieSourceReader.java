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

package org.apache.hudi.source.reader;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.hudi.source.split.HoodieSourceSplit;
import org.apache.hudi.source.split.SerializableComparator;
import org.apache.hudi.source.split.SplitRequestEvent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

/**
 * The reader implementation of Hoodie Source.
 * @param <T> record type
 */
public class HoodieSourceReader<T> extends
        SingleThreadMultiplexSourceReaderBase<HoodieRecordWithPosition<T>, T, HoodieSourceSplit, HoodieSourceSplit> {

  public HoodieSourceReader(
          RecordEmitter<HoodieRecordWithPosition<T>, T, HoodieSourceSplit> recordEmitter,
          Configuration config,
          SourceReaderContext context,
          SplitReaderFunction<T> readerFunction,
          SerializableComparator<HoodieSourceSplit> splitComparator) {
    super(() -> new HoodieSourceSplitReader<>(context, readerFunction, splitComparator), recordEmitter, config, context);
  }

  @Override
  public void start() {
    // We request a split only if we did not get splits during the checkpoint restore.
    // Otherwise, reader restarts will keep requesting more and more splits.
    if (getNumberOfCurrentlyAssignedSplits() == 0) {
      requestSplit(new ArrayList<>());
    }
  }

  @Override
  protected void onSplitFinished(Map<String, HoodieSourceSplit> finishedSplitIds) {
    requestSplit(new ArrayList<>(finishedSplitIds.keySet()));
  }

  @Override
  protected HoodieSourceSplit initializedState(HoodieSourceSplit hoodieSourceSplit) {
    return hoodieSourceSplit;
  }

  @Override
  protected HoodieSourceSplit toSplitType(String splitId, HoodieSourceSplit hoodieSourceSplit) {
    return hoodieSourceSplit;
  }

  private void requestSplit(Collection<String> finishedSplitIds) {
    context.sendSourceEventToCoordinator(new SplitRequestEvent(finishedSplitIds));
  }
}
