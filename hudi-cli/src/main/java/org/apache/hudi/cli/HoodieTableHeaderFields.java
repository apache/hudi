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
  public static final String HEADER_PARTITION = "Partition";
  public static final String HEADER_PARTITION_PATH = HEADER_PARTITION + " Path";
  public static final String HEADER_FILE_ID = "FileId";
  public static final String HEADER_BASE_INSTANT = "Base-Instant";
  public static final String HEADER_INSTANT_TIME = "InstantTime";
  public static final String HEADER_CLEAN_TIME = "CleanTime";
  public static final String HEADER_EARLIEST_COMMAND_RETAINED = "EarliestCommandRetained";
  public static final String HEADER_CLEANING_POLICY = "Cleaning policy";

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
  public static final String HEADER_TOTAL_DELTA_FILE_SIZE = "Total Delta File Size";
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
  public static final String HEADER_REPAIR_ACTION = "Action";
  public static final String HEADER_HOODIE_PROPERTY = "Property";
  public static final String HEADER_OLD_VALUE = "Old Value";
  public static final String HEADER_NEW_VALUE = "New Value";
}
