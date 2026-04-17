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

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.source.split.HoodieSourceSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.text.ParseException;

/**
 * Default Hoodie record emitter.
 *
 * <p>In addition to forwarding each record downstream, this emitter advances the Flink
 * watermark to the split's {@code latestCommit} epoch when the last record of the split
 * is processed.  Downstream operators can rely on this watermark to trigger time-based
 * windows or gauge read progress in bounded Hudi pipelines.
 *
 * @param <T> record type
 */
public class HoodieRecordEmitter<T> implements RecordEmitter<HoodieRecordWithPosition<T>, T, HoodieSourceSplit>, Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieRecordEmitter.class);
  private final boolean commitTimeWatermarkEnabled;

  public HoodieRecordEmitter(boolean commitTimeWatermarkEnabled) {
    this.commitTimeWatermarkEnabled = commitTimeWatermarkEnabled;
  }

  @Override
  public void emitRecord(HoodieRecordWithPosition<T> record, SourceOutput<T> output, HoodieSourceSplit split) throws Exception {
    T data = record.record();
    boolean isSentinel = data == null && record.isLastInSplit();
    // Collect the record unless it is the null-data sentinel emitted at end-of-split.
    if (!isSentinel) {
      output.collect(data);
    }
    split.updatePosition(record.fileOffset(), record.recordOffset());
    if (record.isLastInSplit() && commitTimeWatermarkEnabled) {
      emitSplitWatermark(output, split);
    }
  }

  /**
   * Parses the split's {@code latestCommit} Hudi instant string into epoch milliseconds and
   * emits a Flink {@link Watermark}, signalling that all records belonging to this split have
   * been produced.
   */
  private void emitSplitWatermark(SourceOutput<T> output, HoodieSourceSplit split) {
    String latestCommit = split.getLatestCommit();
    if (latestCommit != null) {
      try {
        long watermarkMs = HoodieInstantTimeGenerator.parseDateFromInstantTime(latestCommit).getTime();
        output.emitWatermark(new Watermark(watermarkMs));
        LOG.debug("Emitted split-end watermark {} ms for split {} (latestCommit={})",
                watermarkMs, split.splitId(), latestCommit);
      } catch (ParseException e) {
        LOG.warn("Could not parse latestCommit '{}' as a watermark timestamp for split {}; "
                + "no watermark emitted.", latestCommit, split.splitId(), e);
      }
    }
  }
}
