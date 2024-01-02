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

package org.apache.hudi.cli;

/**
 * Fields of print table header.
 */
public class HoodieTableHeaderFields {
  public static final String HEADER_ROW_NO = "No.";
  public static final String HEADER_PARTITION = "Partition";
  public static final String HEADER_INSTANT = "Instant";
  public static final String HEADER_PARTITION_PATH = HEADER_PARTITION + " Path";
  public static final String HEADER_FILE_ID = "FileId";
  public static final String HEADER_BASE_INSTANT = "Base-Instant";
  public static final String HEADER_INSTANT_TIME = "InstantTime";
  public static final String HEADER_CLEAN_TIME = "CleanTime";
  public static final String HEADER_EARLIEST_COMMAND_RETAINED = "EarliestCommandRetained";
  public static final String HEADER_CLEANING_POLICY = "Cleaning policy";
  public static final String HEADER_FILE_SIZE = "File Size";

  public static final String HEADER_TOTAL_FILES_DELETED = "Total Files Deleted";
  public static final String HEADER_TOTAL_FILES_SUCCESSFULLY_DELETED = "Total Files Successfully Deleted";
  public static final String HEADER_TOTAL_FAILED_DELETIONS = "Total Failed Deletions";
  public static final String HEADER_TOTAL_TIME_TAKEN = "Total Time Taken";

  /**
   * Fields of log file.
   */
  public static final String HEADER_RECORDS = "Records";
  public static final String HEADER_RECORD_COUNT = "RecordCount";
  public static final String HEADER_BLOCK_TYPE = "BlockType";
  public static final String HEADER_HEADER_METADATA = "HeaderMetadata";
  public static final String HEADER_FOOTER_METADATA = "FooterMetadata";

  /**
   * Fields of data header.
   */
  public static final String HEADER_DATA_FILE = "Data-File";
  public static final String HEADER_DATA_FILE_SIZE = HEADER_DATA_FILE + " Size";

  /**
   * Fields of delta header.
   */
  public static final String HEADER_DELTA_SIZE = "Delta Size";
  public static final String HEADER_DELTA_FILES = "Delta Files";
  public static final String HEADER_TOTAL_DELTA_SIZE = "Total " + HEADER_DELTA_SIZE;
  public static final String HEADER_TOTAL_DELTA_FILE_SIZE = "Total Delta " + HEADER_FILE_SIZE;
  public static final String HEADER_NUM_DELTA_FILES = "Num " + HEADER_DELTA_FILES;

  /**
   * Fields of compaction scheduled header.
   */
  private static final String COMPACTION_SCHEDULED_SUFFIX = " - compaction scheduled";
  private static final String COMPACTION_UNSCHEDULED_SUFFIX = " - compaction unscheduled";

  public static final String HEADER_DELTA_SIZE_SCHEDULED = HEADER_DELTA_SIZE + COMPACTION_SCHEDULED_SUFFIX;
  public static final String HEADER_DELTA_SIZE_UNSCHEDULED = HEADER_DELTA_SIZE + COMPACTION_UNSCHEDULED_SUFFIX;
  public static final String HEADER_DELTA_BASE_SCHEDULED = "Delta To Base Ratio" + COMPACTION_SCHEDULED_SUFFIX;
  public static final String HEADER_DELTA_BASE_UNSCHEDULED = "Delta To Base Ratio" + COMPACTION_UNSCHEDULED_SUFFIX;
  public static final String HEADER_DELTA_FILES_SCHEDULED = "Delta Files" + COMPACTION_SCHEDULED_SUFFIX;
  public static final String HEADER_DELTA_FILES_UNSCHEDULED = "Delta Files" + COMPACTION_UNSCHEDULED_SUFFIX;

  /**
   * Fields of Repair.
   */
  public static final String HEADER_METADATA_PRESENT = "Metadata Present?";
  public static final String HEADER_ACTION = "Action";
  public static final String HEADER_HOODIE_PROPERTY = "Property";
  public static final String HEADER_OLD_VALUE = "Old Value";
  public static final String HEADER_NEW_VALUE = "New Value";
  public static final String HEADER_TEXT_METAFILE_PRESENT = "Text Metafile present ?";
  public static final String HEADER_BASE_METAFILE_PRESENT = "Base Metafile present ?";

  /**
   * Fields of Savepoints.
   */
  public static final String HEADER_SAVEPOINT_TIME = "SavepointTime";

  /**
   * Fields of Rollback.
   */
  public static final String HEADER_ROLLBACK_INSTANT = "Rolledback " + HEADER_INSTANT;
  public static final String HEADER_TIME_TOKEN_MILLIS = "Time taken in millis";
  public static final String HEADER_TOTAL_PARTITIONS = "Total Partitions";
  public static final String HEADER_DELETED_FILE = "Deleted File";
  public static final String HEADER_SUCCEEDED = "Succeeded";

  /**
   * Fields of Restore.
   */
  public static final String HEADER_RESTORE_INSTANT = "Restored " + HEADER_INSTANT;
  public static final String HEADER_RESTORE_STATE = "Restore State";

  /**
   * Fields of Stats.
   */
  public static final String HEADER_COMMIT_TIME = "CommitTime";
  public static final String HEADER_TOTAL_UPSERTED = "Total Upserted";
  public static final String HEADER_TOTAL_WRITTEN = "Total Written";
  public static final String HEADER_WRITE_AMPLIFICATION_FACTOR = "Write Amplification Factor";
  public static final String HEADER_HISTOGRAM_MIN = "Min";
  public static final String HEADER_HISTOGRAM_10TH = "10th";
  public static final String HEADER_HISTOGRAM_50TH = "50th";
  public static final String HEADER_HISTOGRAM_AVG = "avg";
  public static final String HEADER_HISTOGRAM_95TH = "95th";
  public static final String HEADER_HISTOGRAM_MAX = "Max";
  public static final String HEADER_HISTOGRAM_NUM_FILES = "NumFiles";
  public static final String HEADER_HISTOGRAM_STD_DEV = "StdDev";

  /**
   * Fields of Commit.
   */
  public static final String HEADER_TOTAL_BYTES_WRITTEN = "Total Bytes Written";
  public static final String HEADER_TOTAL_FILES_ADDED = "Total Files Added";
  public static final String HEADER_TOTAL_FILES_UPDATED = "Total Files Updated";
  public static final String HEADER_TOTAL_PARTITIONS_WRITTEN = "Total Partitions Written";
  public static final String HEADER_TOTAL_RECORDS_WRITTEN = "Total Records Written";
  public static final String HEADER_TOTAL_UPDATE_RECORDS_WRITTEN = "Total Update Records Written";
  public static final String HEADER_TOTAL_RECORDS_INSERTED = "Total Records Inserted";
  public static final String HEADER_TOTAL_RECORDS_UPDATED = "Total Records Updated";
  public static final String HEADER_TOTAL_ERRORS = "Total Errors";
  public static final String HEADER_TOTAL_RECORDS_WRITTEN_COMMIT = "Total Records Written for entire commit";
  public static final String HEADER_TOTAL_BYTES_WRITTEN_COMMIT = "Total Bytes Written for entire commit";
  public static final String HEADER_AVG_REC_SIZE_COMMIT = "Avg record size for entire commit";

  /**
   * Fields of commit metadata.
   */
  public static final String HEADER_PREVIOUS_COMMIT = "Previous Commit";
  public static final String HEADER_NUM_WRITES = "Num Writes";
  public static final String HEADER_NUM_INSERTS = "Num Inserts";
  public static final String HEADER_NUM_DELETES = "Num Deletes";
  public static final String HEADER_NUM_UPDATE_WRITES = "Num Update Writes";
  public static final String HEADER_TOTAL_LOG_BLOCKS = "Total Log Blocks";
  public static final String HEADER_TOTAL_CORRUPT_LOG_BLOCKS = "Total Corrupt LogBlocks";
  public static final String HEADER_TOTAL_ROLLBACK_BLOCKS = "Total Rollback Blocks";
  public static final String HEADER_TOTAL_LOG_RECORDS = "Total Log Records";
  public static final String HEADER_TOTAL_UPDATED_RECORDS_COMPACTED = "Total Updated Records Compacted";

  /**
   * Fields of Compaction.
   */
  public static final String HEADER_INSTANT_BLANK_TIME = "Instant Time";
  public static final String HEADER_FILE_PATH = "File Path";
  public static final String HEADER_COMPACTION_INSTANT_TIME = "Compaction " + HEADER_INSTANT_BLANK_TIME;
  public static final String HEADER_STATE = "State";
  public static final String HEADER_TOTAL_FILES_TO_BE_COMPACTED = "Total FileIds to be Compacted";
  public static final String HEADER_EXTRA_METADATA = "Extra Metadata";
  public static final String HEADER_DATA_FILE_PATH = "Data " + HEADER_FILE_PATH;
  public static final String HEADER_TOTAL_DELTA_FILES = "Total " + HEADER_DELTA_FILES;
  public static final String HEADER_METRICS = "getMetrics";
  public static final String HEADER_BASE_INSTANT_TIME = "Base " + HEADER_INSTANT_BLANK_TIME;
  public static final String HEADER_BASE_DATA_FILE = "Base Data File";
  public static final String HEADER_VALID = "Valid";
  public static final String HEADER_ERROR = "Error";
  public static final String HEADER_SOURCE_FILE_PATH = "Source " + HEADER_FILE_PATH;
  public static final String HEADER_DESTINATION_FILE_PATH = "Destination " + HEADER_FILE_PATH;
  public static final String HEADER_RENAME_EXECUTED = "Rename Executed?";
  public static final String HEADER_RENAME_SUCCEEDED = "Rename Succeeded?";

  /**
   * Fields of timeline command output
   */
  public static final String HEADER_REQUESTED_TIME = "Requested\nTime";
  public static final String HEADER_INFLIGHT_TIME = "Inflight\nTime";
  public static final String HEADER_COMPLETED_TIME = "Completed\nTime";
  public static final String HEADER_MT_PREFIX = "MT\n";
  public static final String HEADER_MT_ACTION = HEADER_MT_PREFIX + HEADER_ACTION;
  public static final String HEADER_MT_STATE = HEADER_MT_PREFIX + HEADER_STATE;
  public static final String HEADER_MT_REQUESTED_TIME = HEADER_MT_PREFIX + HEADER_REQUESTED_TIME;
  public static final String HEADER_MT_INFLIGHT_TIME = HEADER_MT_PREFIX + HEADER_INFLIGHT_TIME;
  public static final String HEADER_MT_COMPLETED_TIME = HEADER_MT_PREFIX + HEADER_COMPLETED_TIME;

  public static TableHeader getTableHeader() {
    return new TableHeader()
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_COMMIT_TIME)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_BYTES_WRITTEN)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_FILES_ADDED)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_FILES_UPDATED)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_PARTITIONS_WRITTEN)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_RECORDS_WRITTEN)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_UPDATE_RECORDS_WRITTEN)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_ERRORS);
  }

  public static TableHeader getTableHeaderWithExtraMetadata() {
    return new TableHeader()
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_ACTION)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_INSTANT)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_PARTITION)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_FILE_ID)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_PREVIOUS_COMMIT)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_NUM_WRITES)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_NUM_INSERTS)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_NUM_DELETES)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_NUM_UPDATE_WRITES)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_ERRORS)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_LOG_BLOCKS)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_CORRUPT_LOG_BLOCKS)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_ROLLBACK_BLOCKS)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_LOG_RECORDS)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_UPDATED_RECORDS_COMPACTED)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_BYTES_WRITTEN);
  }
}
