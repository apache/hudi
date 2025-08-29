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

package org.apache.hudi.util;

import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.model.HoodieWriteStat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Helper clazz to merge hoodie write stats that belong to one file path.
 *
 * <p>CAUTION: The merge can be buggy, we need to maintain the new variables for the write stat.
 */
public class WriteStatMerger {
  public static HoodieWriteStat merge(HoodieWriteStat stat1, HoodieWriteStat stat2) {
    if (stat1 instanceof HoodieDeltaWriteStat) {
      return mergeDeltaWriteStat((HoodieDeltaWriteStat) stat1, (HoodieDeltaWriteStat) stat2);
    }
    return mergeWriteStat(new HoodieWriteStat(), stat1, stat2);
  }

  private static HoodieDeltaWriteStat mergeDeltaWriteStat(
      HoodieDeltaWriteStat stat1,
      HoodieDeltaWriteStat stat2) {
    HoodieDeltaWriteStat merged = new HoodieDeltaWriteStat();
    mergeWriteStat(merged, stat1, stat2);
    merged.setLogVersion(stat2.getLogVersion());
    merged.setLogOffset(maxLong(stat1.getLogOffset(), stat2.getLogOffset()));
    merged.setBaseFile(stat2.getBaseFile());
    // log files
    List<String> mergedLogFiles = new ArrayList<>(stat1.getLogFiles());
    for (String logFile : stat2.getLogFiles()) {
      if (!mergedLogFiles.contains(logFile)) {
        mergedLogFiles.add(logFile);
      }
    }
    merged.setLogFiles(mergedLogFiles);
    // column stats
    if (stat1.getColumnStats().isPresent()) {
      merged.putRecordsStats(stat1.getColumnStats().get());
    }
    if (stat2.getColumnStats().isPresent()) {
      merged.putRecordsStats(stat2.getColumnStats().get());
    }
    return merged;
  }

  private static HoodieWriteStat mergeWriteStat(HoodieWriteStat merged, HoodieWriteStat stat1, HoodieWriteStat stat2) {
    merged.setFileId(stat2.getFileId());
    merged.setPath(stat2.getPath());
    // merge cdc stats
    merged.setCdcStats(getMergedCdcStats(stat1.getCdcStats(), stat2.getCdcStats()));
    // prev commit
    merged.setPrevCommit(stat2.getPrevCommit());
    // prev base file
    merged.setPrevBaseFile(stat1.getPrevBaseFile());

    merged.setNumWrites(stat2.getNumWrites() + stat1.getNumWrites());
    merged.setNumDeletes(stat2.getNumDeletes() + stat1.getNumDeletes());
    merged.setNumUpdateWrites(stat2.getNumUpdateWrites() + stat1.getNumUpdateWrites());
    merged.setNumInserts(stat2.getNumInserts() + stat1.getNumInserts());
    merged.setTotalWriteBytes(stat2.getTotalWriteBytes() + stat1.getTotalWriteBytes());
    merged.setTotalWriteErrors(stat2.getTotalWriteErrors() + stat1.getTotalWriteErrors());

    // -------------------------------------------------------------------------
    //  Nullable
    // -------------------------------------------------------------------------

    // tmp path
    merged.setTempPath(stat2.getTempPath());
    // partition path
    merged.setPartitionPath(stat2.getPartitionPath());
    // runtime stats
    merged.setRuntimeStats(getMergedRuntimeStats(stat1.getRuntimeStats(), stat2.getRuntimeStats()));

    // log statistics
    merged.setTotalLogRecords(stat2.getTotalLogRecords() + stat1.getTotalLogRecords());
    merged.setTotalLogFilesCompacted(stat2.getTotalLogFilesCompacted() + stat1.getTotalLogFilesCompacted());
    merged.setTotalLogSizeCompacted(stat2.getTotalLogSizeCompacted() + stat1.getTotalLogSizeCompacted());
    merged.setTotalUpdatedRecordsCompacted(stat2.getTotalUpdatedRecordsCompacted() + stat1.getTotalUpdatedRecordsCompacted());
    merged.setTotalLogBlocks(stat2.getTotalLogBlocks() + stat1.getTotalLogBlocks());
    merged.setTotalCorruptLogBlock(stat2.getTotalCorruptLogBlock() + stat1.getTotalCorruptLogBlock());
    merged.setTotalRollbackBlocks(stat2.getTotalRollbackBlocks() + stat1.getTotalRollbackBlocks());
    merged.setFileSizeInBytes(stat2.getFileSizeInBytes() + stat1.getFileSizeInBytes());
    // event time
    merged.setMinEventTime(minLong(stat1.getMinEventTime(), stat2.getMinEventTime()));
    merged.setMaxEventTime(maxLong(stat1.getMaxEventTime(), stat2.getMaxEventTime()));
    return merged;
  }

  private static HoodieWriteStat.RuntimeStats getMergedRuntimeStats(
      HoodieWriteStat.RuntimeStats runtimeStats1,
      HoodieWriteStat.RuntimeStats runtimeStats2) {
    final HoodieWriteStat.RuntimeStats runtimeStats;
    if (runtimeStats1 != null && runtimeStats2 != null) {
      runtimeStats = new HoodieWriteStat.RuntimeStats();
      runtimeStats.setTotalScanTime(runtimeStats1.getTotalScanTime() + runtimeStats2.getTotalScanTime());
      runtimeStats.setTotalUpsertTime(runtimeStats1.getTotalUpsertTime() + runtimeStats2.getTotalUpsertTime());
      runtimeStats.setTotalCreateTime(runtimeStats1.getTotalCreateTime() + runtimeStats2.getTotalCreateTime());
    } else if (runtimeStats1 == null) {
      runtimeStats = runtimeStats2;
    } else {
      runtimeStats = runtimeStats1;
    }
    return runtimeStats;
  }

  private static Map<String, Long> getMergedCdcStats(Map<String, Long> cdcStats1, Map<String, Long> cdcStats2) {
    final Map<String, Long> cdcStats;
    if (cdcStats1 != null && cdcStats2 != null) {
      cdcStats = new HashMap<>();
      cdcStats.putAll(cdcStats1);
      cdcStats.putAll(cdcStats2);
    } else if (cdcStats1 == null) {
      cdcStats = cdcStats2;
    } else {
      cdcStats = cdcStats1;
    }
    return cdcStats;
  }

  private static Long minLong(Long v1, Long v2) {
    if (v1 == null) {
      return v2;
    }
    if (v2 == null) {
      return v1;
    }
    return v1.compareTo(v2) < 0 ? v1 : v2;
  }

  private static Long maxLong(Long v1, Long v2) {
    if (v1 == null) {
      return v2;
    }
    if (v2 == null) {
      return v1;
    }
    return v1.compareTo(v2) > 0 ? v1 : v2;
  }
}
