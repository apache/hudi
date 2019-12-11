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

import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;

import java.util.List;

/**
 * A scanner used to scan hoodie unmerged log records.
 */
public class HoodieUnMergedLogRecordScanner extends AbstractHoodieLogRecordScanner {

  private final LogRecordScannerCallback callback;

  public HoodieUnMergedLogRecordScanner(FileSystem fs, String basePath, List<String> logFilePaths, Schema readerSchema,
      String latestInstantTime, boolean readBlocksLazily, boolean reverseReader, int bufferSize,
      LogRecordScannerCallback callback) {
    super(fs, basePath, logFilePaths, readerSchema, latestInstantTime, readBlocksLazily, reverseReader, bufferSize);
    this.callback = callback;
  }

  @Override
  protected void processNextRecord(HoodieRecord<? extends HoodieRecordPayload> hoodieRecord) throws Exception {
    // Just call callback without merging
    callback.apply(hoodieRecord);
  }

  @Override
  protected void processNextDeletedKey(HoodieKey key) {
    throw new IllegalStateException("Not expected to see delete records in this log-scan mode. Check Job Config");
  }

  /**
   * A callback for log record scanner.
   */
  @FunctionalInterface
  public static interface LogRecordScannerCallback {

    public void apply(HoodieRecord<? extends HoodieRecordPayload> record) throws Exception;
  }
}
