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
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaCache;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.PartialUpdateMode;
import org.apache.hudi.common.table.read.BaseFileUpdateCallback;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecords;
import org.apache.hudi.common.table.read.DeleteContext;
import org.apache.hudi.common.table.read.HoodieReadStats;
import org.apache.hudi.common.table.read.InputSplit;
import org.apache.hudi.common.table.read.ReaderParameters;
import org.apache.hudi.common.table.read.TableState;
import org.apache.hudi.common.table.read.UpdateProcessor;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.HoodieStorage;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Records based {@link FileGroupRecordBuffer} which takes in a map of records to be merged with a base file of interest.
 * This will be used in write paths for COW merge cases.
 * @param <T> Engine native presentation of the record.
 */
public class StreamingFileGroupRecordBufferLoader<T> implements FileGroupRecordBufferLoader<T> {
  private static final StreamingFileGroupRecordBufferLoader INSTANCE = new StreamingFileGroupRecordBufferLoader<>();
  private static final String EXPRESSION_PAYLOAD_CLASS = "org.apache.spark.sql.hudi.command.payload.ExpressionPayload";

  static <T> StreamingFileGroupRecordBufferLoader<T> getInstance() {
    return INSTANCE;
  }

  @Override
  public Pair<HoodieFileGroupRecordBuffer<T>, List<String>> getRecordBuffer(HoodieReaderContext<T> readerContext, HoodieStorage storage, InputSplit inputSplit,
                                                                            List<String> orderingFieldNames, TableState tableState,
                                                                            TypedProperties props, ReaderParameters readerParameters, HoodieReadStats readStats,
                                                                            Option<BaseFileUpdateCallback<T>> fileGroupUpdateCallback) {
    HoodieSchema recordSchema = HoodieSchemaCache.intern(getRecordSchema(readerContext, props));
    readerContext.getSchemaHandler().setSchemaForUpdates(recordSchema);
    // A MERGE INTO with a global index merges a matched record with its current version while tagging it. The result
    // carries the table's payload in the table schema and is final: it is buffered in that schema, which the
    // ExpressionPayload merger and update processor take as a row to keep rather than an input to evaluate.
    boolean isExpressionPayload = isExpressionPayload(readerContext, props);
    HoodieSchema mergedRecordSchema = isExpressionPayload
        ? HoodieSchemaCache.intern(HoodieSchemaUtils.removeMetadataFields(readerContext.getSchemaHandler().getRequiredSchema()))
        : recordSchema;
    HoodieTableConfig tableConfig = tableState.getTableConfig();
    Option<PartialUpdateMode> partialUpdateModeOpt = tableConfig.getPartialUpdateMode();
    UpdateProcessor<T> updateProcessor = UpdateProcessor.create(readStats, readerContext, readerParameters.isEmitDeletes(), fileGroupUpdateCallback, props);
    FileGroupRecordBuffer<T> recordBuffer;
    if (readerParameters.isSortOutputs()) {
      recordBuffer = new SortedKeyBasedFileGroupRecordBuffer<>(
          readerContext, readerContext.getMergeMode(), partialUpdateModeOpt, props, orderingFieldNames, updateProcessor);
    } else {
      recordBuffer = new KeyBasedFileGroupRecordBuffer<>(
          readerContext, readerContext.getMergeMode(), partialUpdateModeOpt, props, orderingFieldNames, updateProcessor);
    }

    RecordContext<T> recordContext = readerContext.getRecordContext();
    Iterator<HoodieRecord> recordIterator = inputSplit.getRecordIterator();
    String[] orderingFieldsArray = orderingFieldNames.toArray(new String[0]);
    DeleteContext deleteContext = DeleteContext.fromRecordSchema(props, recordSchema);
    DeleteContext mergedRecordDeleteContext = isExpressionPayload ? DeleteContext.fromRecordSchema(props, mergedRecordSchema) : deleteContext;
    while (recordIterator.hasNext()) {
      HoodieRecord<T> hoodieRecord = recordIterator.next();
      try {
        boolean isMergedRecord = isExpressionPayload
            && (hoodieRecord.getData() == null || !EXPRESSION_PAYLOAD_CLASS.equals(hoodieRecord.getData().getClass().getName()));
        BufferedRecord<T> bufferedRecord = isMergedRecord
            ? BufferedRecords.fromHoodieRecord(hoodieRecord, mergedRecordSchema, recordContext, props, orderingFieldsArray, mergedRecordDeleteContext)
            : BufferedRecords.fromHoodieRecord(hoodieRecord, recordSchema, recordContext, props, orderingFieldsArray, deleteContext);
        recordBuffer.processNextDataRecord(bufferedRecord, bufferedRecord.getRecordKey());
      } catch (IOException e) {
        throw new HoodieIOException("Failed to process next buffered record", e);
      }
    }
    return Pair.of(recordBuffer, Collections.emptyList());
  }

  private static <T> boolean isExpressionPayload(HoodieReaderContext<T> readerContext, TypedProperties props) {
    Option<Pair<String, String>> payloadClasses = readerContext.getPayloadClasses(props);
    return payloadClasses.isPresent() && payloadClasses.get().getRight().equals(EXPRESSION_PAYLOAD_CLASS);
  }

  private static <T> HoodieSchema getRecordSchema(HoodieReaderContext<T> readerContext, TypedProperties props) {
    if (isExpressionPayload(readerContext, props)) {
      String schemaStr = props.getString("hoodie.payload.record.schema");
      return HoodieSchema.parse(schemaStr);
    } else {
      return HoodieSchemaUtils.removeMetadataFields(readerContext.getSchemaHandler().getRequestedSchema());
    }
  }
}
