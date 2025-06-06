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

import org.apache.hudi.avro.HoodieAvroReaderContext;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;

import org.apache.avro.generic.IndexedRecord;

import java.util.List;

/**
 * Scans a set of log files to extract metadata about the log blocks. It does not read the actual records.
 */
public class HoodieLogBlockMetadataScanner extends BaseHoodieLogRecordReader<IndexedRecord> {

  public HoodieLogBlockMetadataScanner(HoodieTableMetaClient metaClient, List<String> logFilePaths, int bufferSize, String maxInstantTime, Option<InstantRange> instantRange) {
    super(getReaderContext(metaClient, maxInstantTime), metaClient, metaClient.getStorage(), logFilePaths, false, bufferSize, instantRange, false, false, Option.empty(), Option.empty(), true,
        null, false);
    scanInternal(Option.empty(), true);
  }

  private static HoodieReaderContext<IndexedRecord> getReaderContext(HoodieTableMetaClient metaClient, String maxInstantTime) {
    HoodieReaderContext<IndexedRecord> readerContext = new HoodieAvroReaderContext(metaClient.getStorage().getConf(), metaClient.getTableConfig(), Option.empty(), Option.empty());
    readerContext.setHasLogFiles(true);
    readerContext.setHasBootstrapBaseFile(false);
    readerContext.setLatestCommitTime(maxInstantTime);
    return readerContext;
  }
}
