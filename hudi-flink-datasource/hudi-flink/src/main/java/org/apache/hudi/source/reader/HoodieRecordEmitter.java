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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.source.split.HoodieSourceSplit;

/**
 * Default Hoodie record emitter.
 *
 * <p>This emitter handles watermark emission based on split information.
 *
 * @param <T> The type of records to emit
 */
public class HoodieRecordEmitter<T> implements RecordEmitter<HoodieRecordWithPosition<T>, T, HoodieSourceSplit> {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieRecordEmitter.class);
  private HoodieSourceSplit lastSplit = null;
  private long watermark = Long.MIN_VALUE;

  @Override
  public void emitRecord(HoodieRecordWithPosition<T> record, SourceOutput<T> output, HoodieSourceSplit split) throws Exception {
    if (lastSplit == null || !split.splitId().equals(lastSplit.splitId())) {
      long newWatermark = extractWatermark(split);
      if (newWatermark < watermark) {
        LOG.warn(
            "Received a new split with lower watermark. Previous watermark = {}, current watermark = {}, previous split = {}, current split = {}",
            watermark,
            newWatermark,
            lastSplit,
            split);
      } else {
        // emit the watermark of previous finished split
        if (watermark != Long.MIN_VALUE) {
          output.emitWatermark(new Watermark(watermark));
          LOG.debug("Watermark = {} emitted based on split = {}", watermark, split);
        }
        watermark = newWatermark;
      }

      lastSplit = split;
    }

    output.collect(record.record());
    split.updatePosition(record.fileOffset(), record.recordOffset());
  }

  private long extractWatermark(HoodieSourceSplit split) {
    long maxInstantTime = Long.MIN_VALUE;

    if (split.getBasePath().isPresent()) {
      String basePath = split.getBasePath().get();
      try {
        long baseCommitTime = Long.parseLong(FSUtils.getCommitTime(basePath));
        maxInstantTime = Math.max(baseCommitTime, maxInstantTime);
      } catch (NumberFormatException e) {
        LOG.warn("Failed to parse commit time from basePath: {}", basePath, e);
      }
    }

    if (split.getLogPaths().isPresent()) {
      for (String logPath : split.getLogPaths().get()) {
        try {
          long logCommitTime = Long.parseLong(FSUtils.getCommitTime(logPath));
          maxInstantTime = Math.max(logCommitTime, maxInstantTime);
        } catch (NumberFormatException e) {
          LOG.warn("Failed to parse commit time from logPath: {}", logPath, e);
        }
      }
    }

    return maxInstantTime;
  }
}
