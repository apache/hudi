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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.read.BaseFileUpdateCallback;
import org.apache.hudi.common.table.read.HoodieReadStats;
import org.apache.hudi.common.table.read.InputSplit;
import org.apache.hudi.common.table.read.ReaderParameters;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.storage.HoodieStorage;

import java.util.List;

/**
 * This interface defines the contract for initializing a {@link FileGroupRecordBuffer} for a given file group.
 * The implementation is expected to establish the record buffer and populate it with the records from the log files.
 * @param <T> the engine specific record type
 */
public interface FileGroupRecordBufferLoader<T> {

  Pair<HoodieFileGroupRecordBuffer<T>, List<String>> getRecordBuffer(HoodieReaderContext<T> readerContext,
                                                                     HoodieStorage storage,
                                                                     InputSplit inputSplit,
                                                                     List<String> orderingFieldNames,
                                                                     HoodieTableMetaClient hoodieTableMetaClient,
                                                                     TypedProperties props,
                                                                     ReaderParameters readerParameters,
                                                                     HoodieReadStats readStats,
                                                                     Option<BaseFileUpdateCallback<T>> fileGroupUpdateCallback);

  static <T> FileGroupRecordBufferLoader<T> createDefault() {
    return DefaultFileGroupRecordBufferLoader.getInstance();
  }

  static <T> ReusableFileGroupRecordBufferLoader<T> createReusable(HoodieReaderContext<T> readerContextWithoutFilters) {
    return new ReusableFileGroupRecordBufferLoader<>(readerContextWithoutFilters);
  }
}
