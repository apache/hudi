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

import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.PartialUpdateMode;
import org.apache.hudi.common.table.read.BaseFileUpdateCallback;
import org.apache.hudi.common.table.read.HoodieReadStats;
import org.apache.hudi.common.table.read.InputSplit;
import org.apache.hudi.common.table.read.ReaderParameters;
import org.apache.hudi.common.table.read.UpdateProcessor;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.storage.HoodieStorage;

import java.util.List;

/**
 * Default implementation of {@link FileGroupRecordBufferLoader} that initializes a buffer based on the reader parameters.
 *
 * @param <T> the engine specific record type
 */
class DefaultFileGroupRecordBufferLoader<T> extends LogScanningRecordBufferLoader implements FileGroupRecordBufferLoader<T> {
  private static final DefaultFileGroupRecordBufferLoader INSTANCE = new DefaultFileGroupRecordBufferLoader<>();

  static <T> DefaultFileGroupRecordBufferLoader<T> getInstance() {
    return INSTANCE;
  }

  private DefaultFileGroupRecordBufferLoader() {
  }

  @Override
  public Pair<HoodieFileGroupRecordBuffer<T>, List<String>> getRecordBuffer(HoodieReaderContext<T> readerContext,
                                                                            HoodieStorage storage,
                                                                            InputSplit inputSplit,
                                                                            List<String> orderingFieldNames,
                                                                            HoodieTableMetaClient hoodieTableMetaClient,
                                                                            TypedProperties props,
                                                                            ReaderParameters readerParameters,
                                                                            HoodieReadStats readStats,
                                                                            Option<BaseFileUpdateCallback<T>> fileGroupUpdateCallback) {
    boolean isSkipMerge = ConfigUtils.getStringWithAltKeys(props, HoodieReaderConfig.MERGE_TYPE, true).equalsIgnoreCase(HoodieReaderConfig.REALTIME_SKIP_MERGE);
    PartialUpdateMode partialUpdateMode = hoodieTableMetaClient.getTableConfig().getPartialUpdateMode();
    UpdateProcessor<T> updateProcessor = UpdateProcessor.create(readStats, readerContext, readerParameters.emitDeletes(), fileGroupUpdateCallback);
    FileGroupRecordBuffer<T> recordBuffer;
    if (isSkipMerge) {
      recordBuffer = new UnmergedFileGroupRecordBuffer<>(
          readerContext, hoodieTableMetaClient, readerContext.getMergeMode(), partialUpdateMode, props, readStats);
    } else if (readerParameters.sortOutputs()) {
      recordBuffer = new SortedKeyBasedFileGroupRecordBuffer<>(
          readerContext, hoodieTableMetaClient, readerContext.getMergeMode(), partialUpdateMode, props, orderingFieldNames, updateProcessor);
    } else if (readerParameters.useRecordPosition() && inputSplit.getBaseFileOption().isPresent()) {
      recordBuffer = new PositionBasedFileGroupRecordBuffer<>(
          readerContext, hoodieTableMetaClient, readerContext.getMergeMode(), partialUpdateMode, inputSplit.getBaseFileOption().get().getCommitTime(), props,
          orderingFieldNames, updateProcessor);
    } else {
      recordBuffer = new KeyBasedFileGroupRecordBuffer<>(
          readerContext, hoodieTableMetaClient, readerContext.getMergeMode(), partialUpdateMode, props, orderingFieldNames, updateProcessor);
    }
    return Pair.of(recordBuffer, scanLogFiles(readerContext, storage, inputSplit, hoodieTableMetaClient, props,
        readerParameters, readStats, recordBuffer));
  }
}
