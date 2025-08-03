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
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.HoodieReadStats;
import org.apache.hudi.common.table.read.InputSplit;
import org.apache.hudi.common.table.read.ReaderParameters;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.HoodieStorage;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Records based {@link FileGroupRecordBuffer} which takes in a map of records to be merged with a base file of interest.
 * This will be used in write paths for COW merge cases.
 * @param <T> Engine native presentation of the record.
 */
public class RecordsBasedFileGroupRecordBufferLoader<T> extends DefaultFileGroupRecordBufferLoader<T> {

  private final Map<Serializable, BufferedRecord<T>> toBeMergedRecords;

  private RecordsBasedFileGroupRecordBufferLoader(Map<Serializable, BufferedRecord<T>> toBeMergedRecords) {
    super();
    this.toBeMergedRecords = toBeMergedRecords;
  }

  static <T> RecordsBasedFileGroupRecordBufferLoader<T> getInstance(Map<Serializable, BufferedRecord<T>> toBeMergedRecords) {
    return new RecordsBasedFileGroupRecordBufferLoader<>(toBeMergedRecords);
  }

  @Override
  public Option<Pair<HoodieFileGroupRecordBuffer<T>, List<String>>> getRecordBuffer(HoodieReaderContext<T> readerContext,
                                                                            HoodieStorage storage,
                                                                            InputSplit inputSplit,
                                                                            List<String> orderingFieldNames,
                                                                            HoodieTableMetaClient hoodieTableMetaClient,
                                                                            TypedProperties props,
                                                                            ReaderParameters readerParameters,
                                                                            HoodieReadStats readStats,
                                                                            Option<BaseFileUpdateCallback<T>> fileGroupUpdateCallback) {
    Option<FileGroupRecordBuffer<T>> recordBufferOpt = getFileGroupRecordBuffer(readerContext, inputSplit, orderingFieldNames, hoodieTableMetaClient, props,
        readerParameters, readStats, fileGroupUpdateCallback, true);

    return recordBufferOpt.map(recordBuffer -> {
      toBeMergedRecords.forEach((key, value) -> {
        try {
          recordBuffer.processNextDataRecord(value, key);
        } catch (IOException e) {
          throw new HoodieIOException("Failed to process next toBeMergedRecord ", e);
        }
      });
      return Pair.of(recordBufferOpt.get(), Collections.emptyList());
    });
  }
}
