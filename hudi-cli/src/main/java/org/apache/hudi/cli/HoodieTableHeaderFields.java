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
  public static final String HEADER_FILE_ID = "FileId";
  public static final String HEADER_BASE_INSTANT = "Base-Instant";

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
}
