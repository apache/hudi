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
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemas;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecordConverter;
import org.apache.hudi.common.table.read.BufferedRecords;
import org.apache.hudi.common.table.read.DeleteContext;
import org.apache.hudi.common.table.read.IteratorMode;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

/**
 * Shared iterator helpers for reading RFC-103 LSM file groups.
 */
final class LsmFileIterators {

  private LsmFileIterators() {
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
    ClosableIterator<T> baseFileIterator = createBaseFileEngineIterator(
        readerContext, storage, baseFile, start, length);
    if (readerContext.getInstantRange().isPresent()) {
      baseFileIterator = readerContext.applyInstantRangeFilter(baseFileIterator);
    }
    return toBufferedRecordIterator(
        readerContext, baseFileIterator, readerContext.getSchemaHandler().getRequiredSchema(),
        orderingFieldNames, isBaseFileOnly);
  }

  private static <T> ClosableIterator<T> createBaseFileEngineIterator(
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
   * Creates a sorted-run iterator for an RFC-103 native log file.
   *
   * <p>Native data logs use the writer schema stored in the footer when schema evolution is
   * enabled, while native delete logs use their key-and-ordering-only schema.
   */
  static <T> ClosableIterator<BufferedRecord<T>> createLogFileIterator(
      HoodieReaderContext<T> readerContext,
      HoodieTableMetaClient metaClient,
      HoodieStorage storage,
      HoodieLogFile logFile,
      List<String> orderingFieldNames) throws IOException {
    StoragePathInfo pathInfo = logFile.getPathInfo();
    StoragePath storagePath = pathInfo != null ? pathInfo.getPath() : logFile.getPath();
    if (FSUtils.isNativeDeleteLogFile(storagePath.getName())) {
      return createNativeDeleteLogIterator(
          readerContext, storage, pathInfo, storagePath, logFile.getFileSize(), orderingFieldNames);
    }
    return createNativeDataLogIterator(
        readerContext, metaClient, storage, pathInfo, storagePath, logFile.getFileSize(), orderingFieldNames);
  }

  /**
   * Reads a native data log with the same schema flow as an inline log block.
   *
   * <p>With schema evolution enabled, records are decoded with the writer schema stored in the
   * native log footer and then transformed once to the evolved schema for that instant. Without
   * schema evolution, the file reader projects directly to the required reader schema.
   */
  private static <T> ClosableIterator<BufferedRecord<T>> createNativeDataLogIterator(
      HoodieReaderContext<T> readerContext,
      HoodieTableMetaClient metaClient,
      HoodieStorage storage,
      StoragePathInfo pathInfo,
      StoragePath storagePath,
      long fileSize,
      List<String> orderingFieldNames) throws IOException {
    HoodieSchema readerSchema = readerContext.getSchemaHandler().getRequiredSchema();
    if (readerContext.getSchemaHandler().getInternalSchema().isEmptySchema()) {
      ClosableIterator<T> recordIterator = createFileRecordIterator(
          readerContext,
          storage,
          pathInfo,
          storagePath,
          fileSize,
          readerContext.getSchemaHandler().getTableSchema(),
          readerSchema);
      return toBufferedRecordIterator(
          readerContext, recordIterator, readerSchema, orderingFieldNames, false);
    }

    // Read the writer schema from the footer instead of using the table schema. For partial updates,
    // the footer stores the partial schema written to this log file, which may differ from the table schema.
    HoodieSchema writerSchema = TableSchemaResolver.readSchemaFromLogFile(metaClient, storagePath);
    Pair<Function<T, T>, HoodieSchema> schemaEvolutionTransformer =
        readerContext.getSchemaHandler().getSchemaEvolutionTransformer(
            writerSchema, FSUtils.getCommitTime(storagePath.getName())).get();
    ClosableIterator<T> recordIterator = createFileRecordIterator(
        readerContext, storage, pathInfo, storagePath, fileSize, writerSchema, writerSchema);
    recordIterator = new CloseableMappingIterator<>(recordIterator, schemaEvolutionTransformer.getLeft());
    return toBufferedRecordIterator(
        readerContext, recordIterator, schemaEvolutionTransformer.getRight(), orderingFieldNames, false);
  }

  /**
   * Creates delete records from an RFC-103 native delete log.
   *
   * <p>The delete log schema intentionally contains only the record key and ordering fields;
   * partition path and full data columns are not read for delete-only logs.
   */
  private static <T> ClosableIterator<BufferedRecord<T>> createNativeDeleteLogIterator(
      HoodieReaderContext<T> readerContext,
      HoodieStorage storage,
      StoragePathInfo pathInfo,
      StoragePath storagePath,
      long fileSize,
      List<String> orderingFieldNames) throws IOException {
    HoodieSchema deleteLogSchema = HoodieSchemas.createDeleteLogSchema(
        readerContext.getSchemaHandler().getTableSchema(), orderingFieldNames);
    ClosableIterator<T> recordIterator = createFileRecordIterator(
        readerContext, storage, pathInfo, storagePath, fileSize, deleteLogSchema, deleteLogSchema);
    return new CloseableMappingIterator<>(recordIterator,
        record -> createNativeDeleteRecord(readerContext, record, deleteLogSchema, orderingFieldNames));
  }

  private static <T> ClosableIterator<T> createFileRecordIterator(
      HoodieReaderContext<T> readerContext,
      HoodieStorage storage,
      StoragePathInfo pathInfo,
      StoragePath storagePath,
      long fileSize,
      HoodieSchema dataSchema,
      HoodieSchema requiredSchema) throws IOException {
    if (pathInfo != null) {
      return readerContext.getFileRecordIterator(
          pathInfo, 0, pathInfo.getLength(), dataSchema, requiredSchema, storage);
    }
    long length = fileSize >= 0 ? fileSize : storage.getPathInfo(storagePath).getLength();
    return readerContext.getFileRecordIterator(
        storagePath, 0, length, dataSchema, requiredSchema, storage);
  }

  @VisibleForTesting
  static <T> BufferedRecord<T> createNativeDeleteRecord(
      HoodieReaderContext<T> readerContext,
      T record,
      HoodieSchema deleteLogSchema,
      List<String> orderingFieldNames) {
    Object recordKey = readerContext.getRecordContext()
        .getValue(record, deleteLogSchema, HoodieRecord.RECORD_KEY_METADATA_FIELD);
    // Preserve the delete log's ordering value so event-time/custom merge modes can compare
    // deletes against data records instead of treating every native delete as commit-time ordered.
    Comparable orderingValue =
        readerContext.getRecordContext().getOrderingValue(record, deleteLogSchema, orderingFieldNames);
    return BufferedRecords.createDelete(recordKey.toString(), orderingValue);
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
