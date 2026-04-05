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

package org.apache.hudi.common.table.read;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.metadata.HoodieMetadataPayload;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.Properties;

/**
 * Interface used within the {@link HoodieFileGroupReader<T>} for processing updates to records in Merge-on-Read tables.
 * Note that the updates are always relative to the base file's current state.
 * @param <T> the engine specific record type
 */
public interface UpdateProcessor<T> {
  /**
   * Processes the update to the record. If the update should not be returned to the caller, the method should return null.
   * @param recordKey the key of the record being updated
   * @param previousRecord the previous version of the record, or null if there is no previous value
   * @param mergedRecord the current version of the record after merging with the existing record, if any exists
   * @param isDelete a flag indicating whether the merge resulted in a delete operation
   * @return the processed record, or null if the record should not be returned to the caller
   */
  BufferedRecord<T> processUpdate(String recordKey, BufferedRecord<T> previousRecord, BufferedRecord<T> mergedRecord, boolean isDelete);

  static <T> UpdateProcessor<T> create(HoodieReadStats readStats, HoodieReaderContext<T> readerContext,
                                       boolean emitDeletes, Option<BaseFileUpdateCallback<T>> updateCallback,
                                       TypedProperties properties) {
    UpdateProcessor<T> handler;
    Option<String> payloadClass = readerContext.getPayloadClasses(properties).map(Pair::getRight);
    boolean hasNonMetadataPayload = payloadClass.map(className -> !className.equals(HoodieMetadataPayload.class.getName())).orElse(false);
    if (readerContext.getMergeMode() == RecordMergeMode.CUSTOM && hasNonMetadataPayload) {
      handler = new PayloadUpdateProcessor<>(readStats, readerContext, emitDeletes, properties, payloadClass.get());
    } else {
      handler = new StandardUpdateProcessor<>(readStats, readerContext, emitDeletes);
    }
    if (updateCallback.isPresent()) {
      return new CallbackProcessor<>(updateCallback.get(), handler);
    }
    return handler;
  }

  /**
   * A standard update processor that increments the read stats and returns the record if applicable.
   * @param <T> the engine specific record type
   */
  class StandardUpdateProcessor<T> implements UpdateProcessor<T> {
    protected final HoodieReadStats readStats;
    protected final HoodieReaderContext<T> readerContext;
    protected final boolean emitDeletes;

    public StandardUpdateProcessor(HoodieReadStats readStats, HoodieReaderContext<T> readerContext,
                                   boolean emitDeletes) {
      this.readStats = readStats;
      this.readerContext = readerContext;
      this.emitDeletes = emitDeletes;
    }

    @Override
    public BufferedRecord<T> processUpdate(String recordKey, BufferedRecord<T> previousRecord, BufferedRecord<T> mergedRecord, boolean isDelete) {
      if (isDelete) {
        readStats.incrementNumDeletes();
        if (emitDeletes) {
          if (!HoodieOperation.isUpdateBefore(mergedRecord.getHoodieOperation())) {
            mergedRecord.setHoodieOperation(HoodieOperation.DELETE);
          }
          if (mergedRecord.isEmpty()) {
            T deleteRow = readerContext.getRecordContext().getDeleteRow(recordKey);
            return deleteRow == null ? null : mergedRecord.replaceRecord(deleteRow);
          } else {
            return mergedRecord;
          }
        }
        return null;
      } else {
        return handleNonDeletes(previousRecord, mergedRecord);
      }
    }

    protected BufferedRecord<T> handleNonDeletes(BufferedRecord<T> previousRecord, BufferedRecord<T> mergedRecord) {
      T prevRow = previousRecord != null ? previousRecord.getRecord() : null;
      T mergedRow = mergedRecord.getRecord();
      if (prevRow != null && prevRow != mergedRow) {
        mergedRecord.setHoodieOperation(HoodieOperation.UPDATE_AFTER);
        readStats.incrementNumUpdates();
      } else if (prevRow == null) {
        mergedRecord.setHoodieOperation(HoodieOperation.INSERT);
        readStats.incrementNumInserts();
      }
      return mergedRecord.seal(readerContext.getRecordContext());
    }
  }

  class PayloadUpdateProcessor<T> extends StandardUpdateProcessor<T> {
    private final String payloadClass;
    private final Properties properties;

    public PayloadUpdateProcessor(HoodieReadStats readStats, HoodieReaderContext<T> readerContext, boolean emitDeletes,
                                  Properties properties, String payloadClass) {
      super(readStats, readerContext, emitDeletes);
      this.payloadClass = payloadClass;
      this.properties = properties;
    }

    @Override
    protected BufferedRecord<T> handleNonDeletes(BufferedRecord<T> previousRecord, BufferedRecord<T> mergedRecord) {
      if (previousRecord == null) {
        // special case for payloads when there is no previous record
        HoodieSchema recordSchema = readerContext.getRecordContext().decodeAvroSchema(mergedRecord.getSchemaId());
        GenericRecord record = readerContext.getRecordContext().convertToAvroRecord(mergedRecord.getRecord(), recordSchema);
        Schema recordAvroSchema = recordSchema.toAvroSchema();

        // If convertToAvroRecord returned a cached record with a different schema (e.g., from
        // extractDataFromRecord caching for ExpressionPayload in the COW write path), the record
        // is already in write-schema format with correctly evaluated expressions. Convert directly.
        // Note: SENTINEL records (used by ExpressionPayload to signal "skip this record") always
        // have null schema (HoodieRecord.EmptyRecord.getSchema() returns null), so they cannot
        // enter this branch and will always go through the payload path where shouldIgnore handles them.
        if (record.getSchema() != null && !record.getSchema().equals(recordAvroSchema)) {
          // NOTE: After replaceRecord(), mergedRecord.getSchemaId() still references the original schema.
          // This is safe because the record is emitted immediately via super.handleNonDeletes() below,
          // which calls seal() and produces the output row. The record is not spilled to disk (via
          // toBinary()) between replaceRecord and emit in this single-record path. If this assumption
          // changes, the schemaId must be updated after replaceRecord.
          mergedRecord.replaceRecord(readerContext.getRecordContext().convertAvroRecord(record));
        } else {
          HoodieAvroRecord hoodieRecord = new HoodieAvroRecord<>(null, HoodieRecordUtils.loadPayload(payloadClass, record, mergedRecord.getOrderingValue()));
          try {
            if (hoodieRecord.shouldIgnore(recordSchema, properties)) {
              return null;
            }
            // Evaluate the payload to get the insert value
            Option<IndexedRecord> insertValueOpt = hoodieRecord.getData().getInsertValue(recordAvroSchema, properties);
            if (insertValueOpt.isPresent()) {
              GenericRecord insertRecord = (GenericRecord) insertValueOpt.get();
              HoodieSchema readerSchema = readerContext.getSchemaHandler().getRequestedSchema();
              GenericRecord finalRecord;
              if (insertRecord.getSchema().equals(recordAvroSchema)) {
                // Payload preserved the schema (e.g., OverwriteWithLatestAvroPayload).
                // Rewrite to full reader schema including meta fields.
                finalRecord = HoodieAvroUtils.rewriteRecordWithNewSchema(insertRecord, readerSchema.toAvroSchema());
              } else {
                // Payload transformed the schema (e.g., ExpressionPayload maps source→target fields).
                // The result has different field names than recordSchema. Use data-only schema as
                // the rewrite target to maintain arity consistency with BufferedRecord's schemaId,
                // which is immutable and corresponds to the data-only recordSchema.
                HoodieSchema dataSchema = HoodieSchemaUtils.removeMetadataFields(readerSchema);
                finalRecord = HoodieAvroUtils.rewriteRecordWithNewSchema(insertRecord, dataSchema.toAvroSchema());
              }
              mergedRecord.replaceRecord(readerContext.getRecordContext().convertAvroRecord(finalRecord));
            } else {
              // Payload returned empty (e.g., soft-deleted record in DefaultHoodieRecordPayload).
              // Suppress the record — do not emit it as an insert.
              return null;
            }
          } catch (IOException e) {
            throw new HoodieIOException("Error processing record with payload class: " + payloadClass, e);
          }
        }
      }
      return super.handleNonDeletes(previousRecord, mergedRecord);
    }
  }

  /**
   * A processor that wraps the standard update processor and invokes a customizable callback for each update.
   * @param <T> the engine specific record type
   */
  class CallbackProcessor<T> implements UpdateProcessor<T> {
    private final BaseFileUpdateCallback<T> callback;
    private final UpdateProcessor<T> delegate;

    public CallbackProcessor(BaseFileUpdateCallback<T> callback, UpdateProcessor<T> delegate) {
      this.callback = callback;
      this.delegate = delegate;
    }

    @Override
    public BufferedRecord<T> processUpdate(String recordKey, BufferedRecord<T> previousRecord, BufferedRecord<T> mergedRecord, boolean isDelete) {
      BufferedRecord<T> result = delegate.processUpdate(recordKey, previousRecord, mergedRecord, isDelete);

      if (isDelete) {
        callback.onDelete(recordKey, previousRecord, mergedRecord.getHoodieOperation());
      } else if (result != null && HoodieOperation.isUpdateAfter(result.getHoodieOperation())) {
        callback.onUpdate(recordKey, previousRecord, mergedRecord);
      } else if (result != null && HoodieOperation.isInsert(result.getHoodieOperation())) {
        callback.onInsert(recordKey, mergedRecord);
      }
      return result;
    }
  }
}
