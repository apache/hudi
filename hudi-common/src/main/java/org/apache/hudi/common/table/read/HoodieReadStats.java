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

package org.apache.hudi.common.table.read;

import lombok.Getter;
import lombok.Setter;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.Serializable;

/**
 * Statistics about a single Hoodie read operation.
 */
@Getter
@NotThreadSafe
public class HoodieReadStats implements Serializable {
  private static final long serialVersionUID = 1L;

  // Total number of insert records or converted to updates (for small file handling)
  protected long numInserts;
  // Total number of updates
  private long numUpdates = 0L;
  // Total number of records deleted
  protected long numDeletes;
  // Time reading and merging all records from the log files
  @Setter
  protected long totalLogReadTimeMs;
  // Total number of log records that were compacted by a compaction operation
  @Setter
  protected long totalLogRecords;
  // Total number of log files compacted for a file slice with this base fileid
  @Setter
  protected long totalLogFilesCompacted;
  // Total size of all log files for a file slice with this base fileid
  @Setter
  protected long totalLogSizeCompacted;
  // Total number of records updated by a compaction operation
  @Setter
  protected long totalUpdatedRecordsCompacted;
  // Total number of log blocks seen in a compaction operation
  @Setter
  protected long totalLogBlocks;
  // Total number of corrupt blocks seen in a compaction operation
  @Setter
  protected long totalCorruptLogBlock;
  // Total number of rollback blocks seen in a compaction operation
  @Setter
  protected long totalRollbackBlocks;

  public HoodieReadStats() {
  }

  public HoodieReadStats(long numInserts, long numUpdates, long numDeletes) {
    this.numInserts = numInserts;
    this.numUpdates = numUpdates;
    this.numDeletes = numDeletes;
  }

  public void incrementNumInserts() {
    numInserts++;
  }

  public void incrementNumUpdates() {
    numUpdates++;
  }

  public void incrementNumDeletes() {
    numDeletes++;
  }

}
