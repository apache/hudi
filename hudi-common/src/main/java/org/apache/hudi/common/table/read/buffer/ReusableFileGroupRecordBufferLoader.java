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

package org.apache.hudi.common.table.read.buffer;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.PartialUpdateMode;
import org.apache.hudi.common.table.read.BaseFileUpdateCallback;
import org.apache.hudi.common.table.read.HoodieReadStats;
import org.apache.hudi.common.table.read.InputSplit;
import org.apache.hudi.common.table.read.ReaderParameters;
import org.apache.hudi.common.table.read.UpdateProcessor;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.storage.HoodieStorage;

import java.io.Closeable;
import java.util.List;

/**
 * A special case for when the record buffer needs to be reused across multiple reads of the same file group.
 *
 * @param <T> the engine specific record type
 */
public class ReusableFileGroupRecordBufferLoader<T> extends LogScanningRecordBufferLoader implements FileGroupRecordBufferLoader<T>, Closeable {
  private final HoodieReaderContext<T> readerContextWithoutFilters;
  private Pair<KeyBasedFileGroupRecordBuffer<T>, List<String>> cachedResults;

  ReusableFileGroupRecordBufferLoader(HoodieReaderContext<T> readerContextWithoutFilters) {
    this.readerContextWithoutFilters = readerContextWithoutFilters;
  }

  @Override
  public synchronized Pair<HoodieFileGroupRecordBuffer<T>, List<String>> getRecordBuffer(HoodieReaderContext<T> readerContext,
                                                                                         HoodieStorage storage,
                                                                                         InputSplit inputSplit,
                                                                                         List<String> orderingFieldNames,
                                                                                         HoodieTableMetaClient hoodieTableMetaClient,
                                                                                         TypedProperties props,
                                                                                         ReaderParameters readerParameters,
                                                                                         HoodieReadStats readStats,
                                                                                         Option<BaseFileUpdateCallback<T>> fileGroupUpdateCallback) {
    UpdateProcessor<T> updateProcessor = UpdateProcessor.create(readStats, readerContext, readerParameters.emitDeletes(), fileGroupUpdateCallback, props);
    Option<PartialUpdateMode> partialUpdateModeOpt = hoodieTableMetaClient.getTableConfig().getPartialUpdateMode();
    if (cachedResults == null) {
      // Create an initial buffer to process the log files
      KeyBasedFileGroupRecordBuffer<T> initialBuffer = new KeyBasedFileGroupRecordBuffer<>(
          readerContext, hoodieTableMetaClient, readerContext.getMergeMode(), partialUpdateModeOpt, props, orderingFieldNames, updateProcessor);
      List<String> validInstants = scanLogFiles(readerContextWithoutFilters, storage, inputSplit, hoodieTableMetaClient, props, readerParameters, readStats, initialBuffer);
      cachedResults = Pair.of(initialBuffer, validInstants);
    }
    // Create a reusable buffer with the results from the initial scan
    ReusableKeyBasedRecordBuffer<T> reusableBuffer = new ReusableKeyBasedRecordBuffer<>(
        readerContext, hoodieTableMetaClient, readerContext.getMergeMode(), partialUpdateModeOpt, props, orderingFieldNames, updateProcessor, cachedResults.getLeft().getLogRecords());
    return Pair.of(reusableBuffer, cachedResults.getRight());
  }

  @Override
  public void close() {
    if (cachedResults != null && cachedResults.getLeft() != null) {
      cachedResults.getLeft().close();
      cachedResults = null;
    }
  }
}
