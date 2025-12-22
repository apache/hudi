/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.table.action.commit;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.storage.StoragePath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.DELTA_COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;

/**
 * Util class to assist with fetching average record size.
 */
public class AverageRecordSizeUtils {
  private static final Logger LOG = LoggerFactory.getLogger(AverageRecordSizeUtils.class);

  /**
   * Obtains the average record size based on records written during previous commits. Used for estimating how many
   * records pack into one file.
   */
  static long averageBytesPerRecord(HoodieTimeline commitTimeline, HoodieWriteConfig hoodieWriteConfig) {
    long avgSize = hoodieWriteConfig.getCopyOnWriteRecordSizeEstimate();
    long fileSizeThreshold = (long) (hoodieWriteConfig.getRecordSizeEstimationThreshold() * hoodieWriteConfig.getParquetSmallFileLimit());
    if (!commitTimeline.empty()) {
      // Go over the reverse ordered commits to get a more recent estimate of average record size.
      Iterator<HoodieInstant> instants = commitTimeline.getReverseOrderedInstants().iterator();
      while (instants.hasNext()) {
        HoodieInstant instant = instants.next();
        try {
          HoodieCommitMetadata commitMetadata = HoodieCommitMetadata
              .fromBytes(commitTimeline.getInstantDetails(instant).get(), HoodieCommitMetadata.class);
          if (instant.getAction().equals(COMMIT_ACTION) || instant.getAction().equals(REPLACE_COMMIT_ACTION)) {
            long totalBytesWritten = commitMetadata.fetchTotalBytesWritten();
            long totalRecordsWritten = commitMetadata.fetchTotalRecordsWritten();
            if (totalBytesWritten > fileSizeThreshold && totalRecordsWritten > 0) {
              avgSize = (long) Math.ceil((1.0 * totalBytesWritten) / totalRecordsWritten);
              break;
            }
          } else if (instant.getAction().equals(DELTA_COMMIT_ACTION)) {
            // lets consider only base files in case of delta commits
            AtomicLong totalBytesWritten = new AtomicLong(0L);
            AtomicLong totalRecordsWritten = new AtomicLong(0L);
            commitMetadata.getWriteStats().stream()
                .filter(hoodieWriteStat -> FSUtils.isBaseFile(new StoragePath(hoodieWriteStat.getPath())))
                .forEach(hoodieWriteStat -> {
                  totalBytesWritten.addAndGet(hoodieWriteStat.getTotalWriteBytes());
                  totalRecordsWritten.addAndGet(hoodieWriteStat.getNumWrites());
                });
            if (totalBytesWritten.get() > fileSizeThreshold && totalRecordsWritten.get() > 0) {
              avgSize = (long) Math.ceil((1.0 * totalBytesWritten.get()) / totalRecordsWritten.get());
              break;
            }
          }
        } catch (Throwable t) {
          // make this fail safe.
          LOG.error("Error trying to compute average bytes/record ", t);
        }
      }
    }
    return avgSize;
  }
}
