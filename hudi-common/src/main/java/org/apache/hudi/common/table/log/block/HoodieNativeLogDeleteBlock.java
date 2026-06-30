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

package org.apache.hudi.common.table.log.block;

import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemas;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecords;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.core.io.storage.HoodieFileReader;
import org.apache.hudi.core.io.storage.HoodieIOFactory;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.hudi.common.util.ConfigUtils.DEFAULT_HUDI_CONFIG_FOR_READER;

/**
 * Delete block backed by a native delete log file.
 */
public class HoodieNativeLogDeleteBlock extends HoodieDeleteBlock {

  private final HoodieStorage storage;
  private final HoodieLogFile logFile;
  private final HoodieSchema deleteLogSchema;
  private final List<String> orderingFieldNames;
  private final String partitionPath;
  private final Properties props;
  private DeleteRecord[] recordsToDelete;
  private List<BufferedRecord<?>> bufferedRecordsToDelete;

  public HoodieNativeLogDeleteBlock(HoodieStorage storage,
                                    HoodieLogFile logFile,
                                    List<String> orderingFieldNames,
                                    String partitionPath,
                                    Properties props,
                                    Map<HeaderMetadataType, String> header,
                                    Map<FooterMetadataType, String> footer) {
    super(Option.empty(), null, true, getContentLocation(storage, logFile), header, footer);
    this.storage = storage;
    this.logFile = logFile;
    this.deleteLogSchema = HoodieSchemas.createDeleteLogSchema(getSchemaFromHeader(), orderingFieldNames);
    this.orderingFieldNames = orderingFieldNames;
    this.partitionPath = partitionPath;
    this.props = props == null ? new Properties() : props;
  }

  @Override
  public DeleteRecord[] getRecordsToDelete() {
    if (recordsToDelete == null) {
      recordsToDelete = readRecordsToDelete();
    }
    return recordsToDelete;
  }

  @Override
  @SuppressWarnings({"rawtypes", "unchecked"})
  public <T> List<BufferedRecord<T>> getRecordsToDelete(HoodieReaderContext<T> readerContext) {
    if (bufferedRecordsToDelete == null) {
      bufferedRecordsToDelete = (List) readBufferedRecordsToDelete(readerContext);
    }
    return (List) bufferedRecordsToDelete;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private DeleteRecord[] readRecordsToDelete() {
    List<DeleteRecord> deleteRecords = new ArrayList<>();
    String[] orderingFields = orderingFieldNames.toArray(new String[0]);
    StoragePath path = logFile.getPath();
    HoodieFileFormat fileFormat = HoodieFileFormat.fromFileExtension("." + logFile.getSuffix());
    try (HoodieFileReader fileReader = HoodieIOFactory.getIOFactory(storage)
        .getReaderFactory(HoodieRecord.HoodieRecordType.AVRO)
        .getFileReader(DEFAULT_HUDI_CONFIG_FOR_READER, path, fileFormat, Option.empty());
         ClosableIterator<HoodieRecord> recordIterator = fileReader.getRecordIterator(deleteLogSchema, deleteLogSchema)) {
      while (recordIterator.hasNext()) {
        HoodieRecord record = recordIterator.next();
        String recordKey = record.getRecordKey(deleteLogSchema, HoodieRecord.RECORD_KEY_METADATA_FIELD);
        Comparable<?> orderingValue = record.getOrderingValue(deleteLogSchema, props, orderingFields);
        deleteRecords.add(DeleteRecord.create(recordKey, partitionPath, orderingValue));
      }
    } catch (IOException e) {
      throw new HoodieIOException("Failed to read native delete log file " + logFile, e);
    }
    return deleteRecords.toArray(new DeleteRecord[0]);
  }

  private <T> List<BufferedRecord<T>> readBufferedRecordsToDelete(HoodieReaderContext<T> readerContext) {
    List<BufferedRecord<T>> deleteRecords = new ArrayList<>();
    try (ClosableIterator<T> recordIterator = readerContext.getFileRecordIterator(
        logFile.getPath(), 0, FSUtils.getFileSize(storage, logFile), deleteLogSchema, deleteLogSchema, storage)) {
      while (recordIterator.hasNext()) {
        T record = recordIterator.next();
        Object recordKey = readerContext.getRecordContext().getValue(record, deleteLogSchema, HoodieRecord.RECORD_KEY_METADATA_FIELD);
        Comparable orderingValue = readerContext.getRecordContext().getOrderingValue(record, deleteLogSchema, orderingFieldNames);
        deleteRecords.add(BufferedRecords.createDelete(recordKey.toString(), orderingValue));
      }
    } catch (IOException e) {
      throw new HoodieIOException("Failed to read native delete log file " + logFile, e);
    }
    return deleteRecords;
  }

  private static Option<HoodieLogBlockContentLocation> getContentLocation(HoodieStorage storage, HoodieLogFile logFile) {
    long fileSize = FSUtils.getFileSize(storage, logFile);
    return Option.of(new HoodieLogBlockContentLocation(storage, logFile, 0, fileSize, fileSize));
  }
}
