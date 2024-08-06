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

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.storage.HoodieStorage;

import org.apache.avro.Schema;

import java.io.Closeable;
import java.util.List;

public abstract class AbstractHoodieLogRecordScanner extends AbstractHoodieLogRecordReader implements Iterable<HoodieRecord>, Closeable {

  // A timer for calculating elapsed time in millis
  private final HoodieTimer timer = HoodieTimer.create();
  // Stores the total time perform scanning of log blocks
  protected long totalTimeTakenToScanRecords;

  protected AbstractHoodieLogRecordScanner(HoodieStorage storage, String basePath, List<String> logFilePaths, Schema readerSchema,
                                           String latestInstantTime, boolean reverseReader, int bufferSize, Option<InstantRange> instantRange, boolean withOperationField,
                                           boolean forceFullScan, Option<String> partitionNameOverride, InternalSchema internalSchema,
                                           Option<String> keyFieldOverride, boolean enableOptimizedLogBlocksScan,
                                           HoodieRecordMerger recordMerger,
                                           Option<HoodieTableMetaClient> hoodieTableMetaClientOption) {
    super(storage, basePath, logFilePaths, readerSchema, latestInstantTime, reverseReader, bufferSize, instantRange, withOperationField, forceFullScan, partitionNameOverride, internalSchema,
        keyFieldOverride, enableOptimizedLogBlocksScan, recordMerger, hoodieTableMetaClientOption);
  }

  public long getTotalTimeTakenToScanRecords() {
    return this.totalTimeTakenToScanRecords;
  }

  protected void scanStart() {
    this.timer.startTimer();
  }
  protected void scanEnd() {
    this.totalTimeTakenToScanRecords = timer.endTimer();
  }
}
