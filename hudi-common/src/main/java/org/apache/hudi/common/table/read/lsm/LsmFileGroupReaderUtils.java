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

package org.apache.hudi.common.table.read.lsm;

import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecordConverter;
import org.apache.hudi.common.table.read.BufferedRecords;
import org.apache.hudi.common.table.read.DeleteContext;
import org.apache.hudi.common.table.read.IteratorMode;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePathInfo;

import java.io.IOException;
import java.util.List;

/**
 * Shared iterator helpers for reading RFC-103 LSM file groups.
 */
final class LsmFileGroupReaderUtils {

  private LsmFileGroupReaderUtils() {
  }

  /**
   * Creates an iterator for an L1/base file.
   *
   * <p>For the base-file-only fast path, records are converted according to the requested
   * {@link IteratorMode} and emitted directly. When log files need to be merged, every engine record
   * is converted to a complete {@link BufferedRecord}, including its record key and ordering value.
   */
  static <T> ClosableIterator<BufferedRecord<T>> createBaseFileIterator(
      HoodieReaderContext<T> readerContext,
      HoodieStorage storage,
      HoodieBaseFile baseFile,
      long start,
      long length,
      List<String> orderingFieldNames,
      boolean isBaseFileOnly) throws IOException {
    ClosableIterator<T> baseFileIterator = createBaseEngineRecordIterator(
        readerContext, storage, baseFile, start, length);
    return toBufferedRecordIterator(
        readerContext, baseFileIterator, readerContext.getSchemaHandler().getRequiredSchema(),
        orderingFieldNames, isBaseFileOnly);
  }

  private static <T> ClosableIterator<T> createBaseEngineRecordIterator(
      HoodieReaderContext<T> readerContext,
      HoodieStorage storage,
      HoodieBaseFile baseFile,
      long start,
      long length) throws IOException {
    if (baseFile.getBootstrapBaseFile().isPresent()) {
      throw new UnsupportedOperationException("LSM file group reader does not support bootstrap base files");
    }

    HoodieSchema tableSchema = readerContext.getSchemaHandler().getTableSchema();
    HoodieSchema readerSchema = readerContext.getSchemaHandler().getRequiredSchema();
    StoragePathInfo pathInfo = baseFile.getPathInfo();
    return pathInfo != null
        ? readerContext.getFileRecordIterator(pathInfo, start, length, tableSchema, readerSchema, storage)
        : readerContext.getFileRecordIterator(baseFile.getStoragePath(), start, length, tableSchema, readerSchema, storage);
  }

  /**
   * Converts engine records into sealed buffered records for direct output or the LSM merge.
   * Base-file-only records use the configured iterator mode, while records participating in a
   * merge always include the record key and ordering value.
   */
  static <T> ClosableIterator<BufferedRecord<T>> toBufferedRecordIterator(
      HoodieReaderContext<T> readerContext,
      ClosableIterator<T> recordIterator,
      HoodieSchema recordSchema,
      List<String> orderingFieldNames,
      boolean isBaseFileOnly) {
    if (readerContext.getInstantRange().isPresent()) {
      recordIterator = readerContext.applyInstantRangeFilter(recordIterator);
    }

    if (isBaseFileOnly) {
      BufferedRecordConverter<T> converter = BufferedRecordConverter.createConverter(
          readerContext.getIteratorMode(), recordSchema, readerContext.getRecordContext(), orderingFieldNames);
      return new CloseableMappingIterator<>(recordIterator,
          record -> converter.convert(readerContext.getRecordContext().seal(recordSchema, record)));
    } else {
      DeleteContext deleteContext = readerContext.getSchemaHandler().getDeleteContext();
      return new CloseableMappingIterator<>(recordIterator, record -> {
        boolean isDelete = readerContext.getRecordContext().isDeleteRecord(
            record, deleteContext.withReaderSchema(recordSchema));
        T sealedRecord = readerContext.getRecordContext().seal(recordSchema, record);
        return BufferedRecords.fromEngineRecord(
            sealedRecord, recordSchema, readerContext.getRecordContext(), orderingFieldNames, isDelete);
      });
    }
  }
}
