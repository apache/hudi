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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.stats.HoodieColumnRangeMetadata;
import org.apache.hudi.storage.StoragePath;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.apache.avro.reflect.AvroIgnore;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Statistics about a single Hoodie write operation.
 */
@NoArgsConstructor
@Getter
@Setter
@EqualsAndHashCode(onlyExplicitlyIncluded = true, callSuper = false)
@ToString(callSuper = true)
public class HoodieWriteStat extends HoodieReadStats {

  public static final String NULL_COMMIT = "null";

  /**
   * Id of the file being written.
   */
  private String fileId;

  /**
   * Relative path to the file from the base path.
   */
  @EqualsAndHashCode.Include
  private String path;

  /**
   * Relative CDC file path that store the CDC data and its size.
   */
  @Getter(AccessLevel.NONE)
  private Map<String, Long> cdcStats;

  /**
   * The previous version of the file. (null if this is the first version. i.e insert)
   */
  @EqualsAndHashCode.Include
  private String prevCommit;

  /**
   * Total number of records written for this file. - for updates, its the entire number of records in the file - for
   * inserts, it's the actual number of records inserted.
   */
  private long numWrites;

  /**
   * Total number of records actually changed. (0 for inserts)
   */
  private long numUpdateWrites;

  /**
   * Total number of bytes written.
   */
  private long totalWriteBytes;

  /**
   * Total number of records, that were n't able to be written due to errors.
   */
  private long totalWriteErrors;

  /**
   * Relative path to the temporary file from the base path.
   */
  @Nullable
  private String tempPath;

  /**
   * Following properties are associated only with the result of a Compaction Operation
   */

  /**
   * Partition Path associated with this writeStat.
   */
  @Nullable
  private String partitionPath;

  /**
   * File Size as of close.
   */
  private long fileSizeInBytes;

  /**
   * The earliest of incoming records' event times (Epoch ms) for calculating latency.
   */
  @Setter(AccessLevel.NONE)
  @Nullable
  private Long minEventTime;

  /**
   * The latest of incoming records' event times (Epoch ms) for calculating freshness.
   */
  @Setter(AccessLevel.NONE)
  @Nullable
  private Long maxEventTime;

  @Nullable
  private String prevBaseFile;

  @Getter(AccessLevel.NONE)
  @Nullable
  private RuntimeStats runtimeStats;

  @JsonIgnore
  @AvroIgnore
  private Option<Map<String, HoodieColumnRangeMetadata<Comparable>>> recordsStats = Option.empty();

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

  /**
   * The runtime stats for writing operation.
   */
  @Getter
  @Setter
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
