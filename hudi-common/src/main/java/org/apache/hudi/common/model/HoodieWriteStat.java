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

package org.apache.hudi.common.model;

import org.apache.hudi.common.table.read.HoodieReadStats;
import org.apache.hudi.common.util.JsonUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.stats.HoodieColumnRangeMetadata;
import org.apache.hudi.storage.StoragePath;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Getter;
import lombok.Setter;
import org.apache.avro.reflect.AvroIgnore;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Statistics about a single Hoodie write operation.
 */
public class HoodieWriteStat extends HoodieReadStats {

  public static final String NULL_COMMIT = "null";

  /**
   * Id of the file being written.
   */
  @Getter
  @Setter
  private String fileId;

  /**
   * Relative path to the file from the base path.
   */
  @Getter
  @Setter
  private String path;

  /**
   * Relative CDC file path that store the CDC data and its size.
   */
  @Setter
  private Map<String, Long> cdcStats;

  /**
   * The previous version of the file. (null if this is the first version. i.e insert)
   */
  @Getter
  @Setter
  private String prevCommit;

  /**
   * Total number of records written for this file. - for updates, its the entire number of records in the file - for
   * inserts, it's the actual number of records inserted.
   */
  @Getter
  @Setter
  private long numWrites;

  /**
   * Total number of records actually changed. (0 for inserts)
   */
  @Getter
  @Setter
  private long numUpdateWrites;

  /**
   * Total number of bytes written.
   */
  @Setter
  @Getter
  private long totalWriteBytes;

  /**
   * Total number of records, that were n't able to be written due to errors.
   */
  @Setter
  @Getter
  private long totalWriteErrors;

  /**
   * Relative path to the temporary file from the base path.
   */
  @Getter
  @Setter
  @Nullable
  private String tempPath;

  /**
   * Following properties are associated only with the result of a Compaction Operation
   */

  /**
   * Partition Path associated with this writeStat.
   */
  @Setter
  @Getter
  @Nullable
  private String partitionPath;

  /**
   * File Size as of close.
   */
  @Setter
  @Getter
  private long fileSizeInBytes;

  /**
   * The earliest of incoming records' event times (Epoch ms) for calculating latency.
   */
  @Getter
  @Nullable
  private Long minEventTime;

  /**
   * The latest of incoming records' event times (Epoch ms) for calculating freshness.
   */
  @Nullable
  private Long maxEventTime;

  @Setter
  @Getter
  @Nullable
  private String prevBaseFile;

  @Nullable
  @Setter
  private RuntimeStats runtimeStats;

  @JsonIgnore
  @AvroIgnore
  private Option<Map<String, HoodieColumnRangeMetadata<Comparable>>> recordsStats = Option.empty();

  public HoodieWriteStat() {
    // called by jackson json lib
  }

  public void setNumDeletes(long numDeletes) {
    this.numDeletes = numDeletes;
  }

  public void setNumInserts(long numInserts) {
    this.numInserts = numInserts;
  }

  @Nullable
  public Map<String, Long> getCdcStats() {
    return cdcStats;
  }

  public void setMinEventTime(Long minEventTime) {
    if (this.minEventTime == null) {
      this.minEventTime = minEventTime;
    } else {
      this.minEventTime = Math.min(minEventTime, this.minEventTime);
    }
  }

  @Nullable
  public Long getMaxEventTime() {
    return maxEventTime;
  }

  public void setMaxEventTime(Long maxEventTime) {
    if (this.maxEventTime == null) {
      this.maxEventTime = maxEventTime;
    } else {
      this.maxEventTime = Math.max(maxEventTime, this.maxEventTime);
    }
  }

  @Nullable
  public RuntimeStats getRuntimeStats() {
    return runtimeStats;
  }

  /**
   * Set path and tempPath relative to the given basePath.
   */
  public void setPath(StoragePath basePath, StoragePath path) {
    this.path = path.toString().replace(basePath + "/", "");
  }

  public void putRecordsStats(Map<String, HoodieColumnRangeMetadata<Comparable>> stats) {
    if (!recordsStats.isPresent()) {
      recordsStats = Option.of(stats);
    } else {
      // in case there are multiple log blocks for one write process.
      recordsStats = Option.of(mergeRecordsStats(recordsStats.get(), stats));
    }
  }

  // keep for serialization efficiency
  // please do not remove it even if it is not used anywhere.
  public void setRecordsStats(Map<String, HoodieColumnRangeMetadata<Comparable>> stats) {
    recordsStats = Option.of(stats);
  }

  public void removeRecordStats() {
    this.recordsStats = Option.empty();
  }

  public Option<Map<String, HoodieColumnRangeMetadata<Comparable>>> getColumnStats() {
    return recordsStats;
  }

  @Override
  public String toString() {
    return "HoodieWriteStat{fileId='" + fileId + '\'' + ", path='" + path + '\'' + ", prevCommit='" + prevCommit
        + '\'' + ", prevBaseFile=" + prevBaseFile + '\'' + ", numWrites=" + numWrites + ", numDeletes=" + numDeletes
        + ", numUpdateWrites=" + numUpdateWrites + ", totalWriteBytes=" + totalWriteBytes + ", totalWriteErrors="
        + totalWriteErrors + ", tempPath='" + tempPath + '\'' + ", cdcStats='" + JsonUtils.toString(cdcStats)
        + '\'' + ", partitionPath='" + partitionPath + '\'' + ", totalLogRecords=" + totalLogRecords
        + ", totalLogFilesCompacted=" + totalLogFilesCompacted + ", totalLogSizeCompacted=" + totalLogSizeCompacted
        + ", totalUpdatedRecordsCompacted=" + totalUpdatedRecordsCompacted + ", totalLogBlocks=" + totalLogBlocks
        + ", totalCorruptLogBlock=" + totalCorruptLogBlock + ", totalRollbackBlocks=" + totalRollbackBlocks + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    HoodieWriteStat that = (HoodieWriteStat) o;
    if (!path.equals(that.path)) {
      return false;
    }
    return prevCommit.equals(that.prevCommit);

  }

  @Override
  public int hashCode() {
    int result = path.hashCode();
    result = 31 * result + prevCommit.hashCode();
    return result;
  }

  /**
   * The runtime stats for writing operation.
   */
  @Setter
  @Getter
  public static class RuntimeStats implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * Total time taken to read and merge logblocks in a log file.
     */
    private long totalScanTime;

    /**
     * Total time taken by a Hoodie Merge for an existing file.
     */
    private long totalUpsertTime;

    /**
     * Total time taken by a Hoodie Insert to a file.
     */
    private long totalCreateTime;

  }

  private static Map<String, HoodieColumnRangeMetadata<Comparable>> mergeRecordsStats(
      Map<String, HoodieColumnRangeMetadata<Comparable>> stats1,
      Map<String, HoodieColumnRangeMetadata<Comparable>> stats2) {
    Map<String, HoodieColumnRangeMetadata<Comparable>> mergedStats = new HashMap<>(stats1);
    for (Map.Entry<String, HoodieColumnRangeMetadata<Comparable>> entry : stats2.entrySet()) {
      final String colName = entry.getKey();
      mergedStats.merge(colName, entry.getValue(),
          (oldValue, newValue) -> HoodieColumnRangeMetadata.merge(oldValue, newValue));
    }
    return mergedStats;
  }
}
