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

package org.apache.hudi.common.table.log;

import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;

import java.util.Map;

/**
 * Pojo holding information on the result of a {@link org.apache.hudi.common.table.log.HoodieLogFormat.Writer#appendBlock(HoodieLogBlock)}.
 */
public class AppendResult {

  private final HoodieLogFile logFile;
  private Map<String, HoodieColumnRangeMetadata<Comparable>> recordsColumnStats;
  private final long offset;
  private final long size;

  public AppendResult(
      HoodieLogFile logFile,
      Map<String, HoodieColumnRangeMetadata<Comparable>> recordsColumnStats,
      long offset,
      long size) {
    this.recordsColumnStats = recordsColumnStats;
    this.logFile = logFile;
    this.offset = offset;
    this.size = size;
  }

  public HoodieLogFile logFile() {
    return logFile;
  }

  public Map<String, HoodieColumnRangeMetadata<Comparable>> getRecordsStats() {
    return recordsColumnStats;
  }

  public long offset() {
    return offset;
  }

  public long size() {
    return size;
  }
}
