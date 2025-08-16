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

package org.apache.hudi.client;

import org.apache.hudi.ApiMaturityLevel;
import org.apache.hudi.PublicAPIClass;
import org.apache.hudi.PublicAPIMethod;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordDelegate;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.util.DateTimeUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.DateTimeException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

import static org.apache.hudi.common.model.DefaultHoodieRecordPayload.METADATA_EVENT_TIME_KEY;
import static org.apache.hudi.common.util.StringUtils.isNullOrEmpty;

/**
 * Status of a write operation.
 */
@PublicAPIClass(maturity = ApiMaturityLevel.STABLE)
public class WriteStatus implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(WriteStatus.class);
  private static final long serialVersionUID = 1L;
  private static final long RANDOM_SEED = 9038412832L;

  private final HashMap<HoodieKey, Throwable> errors = new HashMap<>();

  private final List<Pair<HoodieRecordDelegate, Throwable>> failedRecords = new ArrayList<>();

  // true if this WriteStatus refers to a write happening in metadata table.
  private final boolean isMetadataTable;
  private Throwable globalError = null;

  private String fileId = null;

  private String partitionPath = null;

  private HoodieWriteStat stat = null;

  private long totalRecords = 0;
  private long totalErrorRecords = 0;

  private final double failureFraction;
  private boolean trackSuccessRecords;
  private final transient Random random;
  private IndexStats indexStats = new IndexStats();

  public WriteStatus(Boolean trackSuccessRecords, Double failureFraction, Boolean isMetadataTable) {
    this.trackSuccessRecords = trackSuccessRecords;
    this.failureFraction = failureFraction;
    this.random = new Random(RANDOM_SEED);
    this.isMetadataTable = isMetadataTable;
  }

  public WriteStatus(Boolean trackSuccessRecords, Double failureFraction) {
    this(trackSuccessRecords, failureFraction, false);
  }

  public WriteStatus() {
    this.failureFraction = 0.0d;
    this.trackSuccessRecords = false;
    this.random = null;
    this.isMetadataTable = false;
  }

  /**
   * Mark write as success, optionally using given parameters for the purpose of calculating some aggregate metrics.
   * This method is not meant to cache passed arguments, since WriteStatus objects are collected in Spark Driver.
   *
   * @param record                 deflated {@code HoodieRecord} containing information that uniquely identifies it.
   * @param optionalRecordMetadata optional metadata related to data contained in {@link HoodieRecord} before deflation.
   */
  public void markSuccess(HoodieRecord record, Option<Map<String, String>> optionalRecordMetadata) {
    if (trackSuccessRecords) {
      indexStats.addHoodieRecordDelegate(HoodieRecordDelegate.fromHoodieRecord(record));
    }
    updateStatsForSuccess(optionalRecordMetadata);
  }

  /**
   * Allows the writer to manually add record delegates to the index stats.
   */
  public void manuallyTrackSuccess() {
    this.trackSuccessRecords = false;
  }

  public void addRecordDelegate(HoodieRecordDelegate recordDelegate) {
    indexStats.addHoodieRecordDelegate(recordDelegate);
  }

  /**
   * Used by native write handles like HoodieRowCreateHandle and HoodieRowDataCreateHandle.
   *
   * @see WriteStatus#markSuccess(HoodieRecord, Option)
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public void markSuccess(HoodieRecordDelegate recordDelegate, Option<Map<String, String>> optionalRecordMetadata) {
    if (trackSuccessRecords) {
      indexStats.addHoodieRecordDelegate(Objects.requireNonNull(recordDelegate));
    }
    updateStatsForSuccess(optionalRecordMetadata);
  }

  private void updateStatsForSuccess(Option<Map<String, String>> optionalRecordMetadata) {
    totalRecords++;

    // get the min and max event time for calculating latency and freshness
    String eventTimeVal = optionalRecordMetadata.orElse(Collections.emptyMap())
        .getOrDefault(METADATA_EVENT_TIME_KEY, null);
    if (isNullOrEmpty(eventTimeVal)) {
      return;
    }
    try {
      int length = eventTimeVal.length();
      long millisEventTime;
      // eventTimeVal in seconds unit
      if (length == 10) {
        millisEventTime = Long.parseLong(eventTimeVal) * 1000;
      } else if (length == 13) {
        // eventTimeVal in millis unit
        millisEventTime = Long.parseLong(eventTimeVal);
      } else {
        throw new IllegalArgumentException("not support event_time format:" + eventTimeVal);
      }
      long eventTime = DateTimeUtils.parseDateTime(Long.toString(millisEventTime)).toEpochMilli();
      stat.setMinEventTime(eventTime);
      stat.setMaxEventTime(eventTime);
    } catch (DateTimeException | IllegalArgumentException e) {
      LOG.debug("Fail to parse event time value: {}", eventTimeVal, e);
    }
  }

  /**
   * Mark write as failed, optionally using given parameters for the purpose of calculating some aggregate metrics. This
   * method is not meant to cache passed arguments, since WriteStatus objects are collected in Spark Driver.
   *
   * @param record                 deflated {@code HoodieRecord} containing information that uniquely identifies it.
   * @param optionalRecordMetadata optional metadata related to data contained in {@link HoodieRecord} before deflation.
   */
  public void markFailure(HoodieRecord record, Throwable t, Option<Map<String, String>> optionalRecordMetadata) {
    if (failedRecords.isEmpty() || (random.nextDouble() <= failureFraction)) {
      // Guaranteed to have at-least one error
      failedRecords.add(Pair.of(HoodieRecordDelegate.fromHoodieRecord(record), t));
      errors.put(record.getKey(), t);
    }
    updateStatsForFailure();
  }

  /**
   * Used by native write handles like HoodieRowCreateHandle and HoodieRowDataCreateHandle.
   *
   * @see WriteStatus#markFailure(HoodieRecord, Throwable, Option)
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public void markFailure(String recordKey, String partitionPath, Throwable t) {
    if (failedRecords.isEmpty() || (random.nextDouble() <= failureFraction)) {
      // Guaranteed to have at-least one error
      HoodieRecordDelegate recordDelegate = HoodieRecordDelegate.create(recordKey, partitionPath);
      failedRecords.add(Pair.of(recordDelegate, t));
      errors.put(recordDelegate.getHoodieKey(), t);
    }
    updateStatsForFailure();
  }

  private void updateStatsForFailure() {
    totalRecords++;
    totalErrorRecords++;
  }

  public WriteStatus removeMetadataStats() {
    this.indexStats = null;
    return this;
  }

  public String getFileId() {
    return fileId;
  }

  public void setFileId(String fileId) {
    this.fileId = fileId;
  }

  public boolean hasErrors() {
    return totalErrorRecords > 0;
  }

  public boolean isErrored(HoodieKey key) {
    return errors.containsKey(key);
  }

  public HashMap<HoodieKey, Throwable> getErrors() {
    return errors;
  }

  public boolean hasGlobalError() {
    return globalError != null;
  }

  public Throwable getGlobalError() {
    return this.globalError;
  }

  public void setGlobalError(Throwable t) {
    this.globalError = t;
  }

  public IndexStats getIndexStats() {
    return indexStats;
  }

  public List<HoodieRecordDelegate> getWrittenRecordDelegates() {
    return indexStats.getWrittenRecordDelegates();
  }

  public List<Pair<HoodieRecordDelegate, Throwable>> getFailedRecords() {
    return failedRecords;
  }

  public HoodieWriteStat getStat() {
    return stat;
  }

  public void setStat(HoodieWriteStat stat) {
    this.stat = stat;
  }

  public String getPartitionPath() {
    return partitionPath;
  }

  public void setPartitionPath(String partitionPath) {
    this.partitionPath = partitionPath;
  }

  public long getTotalRecords() {
    return totalRecords;
  }

  public void setTotalRecords(long totalRecords) {
    this.totalRecords = totalRecords;
  }

  public long getTotalErrorRecords() {
    return totalErrorRecords;
  }

  public void setTotalErrorRecords(long totalErrorRecords) {
    this.totalErrorRecords = totalErrorRecords;
  }

  public boolean isTrackingSuccessfulWrites() {
    return trackSuccessRecords;
  }

  public boolean isMetadataTable() {
    return isMetadataTable;
  }

  @Override
  public String toString() {
    return "WriteStatus {"
        + "isMetadataTable=" + isMetadataTable
        + ", fileId=" + fileId
        + ", writeStat=" + stat
        + ", globalError='" + globalError + '\''
        + ", hasErrors='" + hasErrors() + '\''
        + ", errorCount='" + totalErrorRecords + '\''
        + ", errorPct='" + (100.0 * totalErrorRecords) / totalRecords + '\''
        + '}';
  }
}
