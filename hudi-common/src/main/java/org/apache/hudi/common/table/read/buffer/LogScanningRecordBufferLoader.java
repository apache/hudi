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

package org.apache.hudi.common.table.read.buffer;

import org.apache.hudi.common.config.HoodieMemoryConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordReader;
import org.apache.hudi.common.table.read.HoodieReadStats;
import org.apache.hudi.common.table.read.InputSplit;
import org.apache.hudi.common.table.read.ReaderParameters;
import org.apache.hudi.storage.HoodieStorage;

import java.util.List;

import static org.apache.hudi.common.util.ConfigUtils.getIntWithAltKeys;

abstract class LogScanningRecordBufferLoader {

  protected <T> List<String> scanLogFiles(HoodieReaderContext<T> readerContext, HoodieStorage storage,
                                          InputSplit inputSplit, HoodieTableMetaClient hoodieTableMetaClient,
                                          TypedProperties props, ReaderParameters readerParameters,
                                          HoodieReadStats readStats, FileGroupRecordBuffer<T> recordBuffer) {
    try (HoodieMergedLogRecordReader<T> logRecordReader = HoodieMergedLogRecordReader.<T>newBuilder()
        .withHoodieReaderContext(readerContext)
        .withStorage(storage)
        .withLogFiles(inputSplit.getLogFiles())
        .withReverseReader(false)
        .withBufferSize(getIntWithAltKeys(props, HoodieMemoryConfig.MAX_DFS_STREAM_BUFFER_SIZE))
        .withInstantRange(readerContext.getInstantRange())
        .withPartition(inputSplit.getPartitionPath())
        .withRecordBuffer(recordBuffer)
        .withAllowInflightInstants(readerParameters.allowInflightInstants())
        .withMetaClient(hoodieTableMetaClient)
        .withOptimizedLogBlocksScan(readerParameters.enableOptimizedLogBlockScan())
        .build()) {
      readStats.setTotalLogReadTimeMs(logRecordReader.getTotalTimeTakenToReadAndMergeBlocks());
      readStats.setTotalUpdatedRecordsCompacted(logRecordReader.getNumMergedRecordsInLog());
      readStats.setTotalLogFilesCompacted(logRecordReader.getTotalLogFiles());
      readStats.setTotalLogRecords(logRecordReader.getTotalLogRecords());
      readStats.setTotalLogBlocks(logRecordReader.getTotalLogBlocks());
      readStats.setTotalCorruptLogBlock(logRecordReader.getTotalCorruptBlocks());
      readStats.setTotalRollbackBlocks(logRecordReader.getTotalRollbacks());
      readStats.setTotalCorruptLogFiles(logRecordReader.getTotalCorruptLogFiles());
      return logRecordReader.getValidBlockInstants();
    }
  }
}
