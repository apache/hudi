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

import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.PartialUpdateMode;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.OrderingValues;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.List;

/**
 * Factory to create a {@link BufferedRecordMerger}.
 */
public class BufferedRecordMergerFactory {

  private BufferedRecordMergerFactory() {
  }

  public static <T> BufferedRecordMerger<T> create(HoodieReaderContext<T> readerContext,
                                                   RecordMergeMode recordMergeMode,
                                                   boolean enablePartialMerging,
                                                   Option<HoodieRecordMerger> recordMerger,
                                                   List<String> orderingFieldNames,
                                                   Option<String> payloadClass,
                                                   Schema readerSchema,
                                                   TypedProperties props,
                                                   Option<PartialUpdateMode> partialUpdateModeOpt) {
    return create(readerContext, recordMergeMode, enablePartialMerging, recordMerger,
        orderingFieldNames, readerSchema, payloadClass.map(p -> Pair.of(p, p)), props, partialUpdateModeOpt);
  }

  public static <T> BufferedRecordMerger<T> create(HoodieReaderContext<T> readerContext,
                                                   RecordMergeMode recordMergeMode,
                                                   boolean enablePartialMerging,
                                                   Option<HoodieRecordMerger> recordMerger,
                                                   List<String> orderingFieldNames,
                                                   Schema readerSchema,
                                                   Option<Pair<String, String>> payloadClasses,
                                                   TypedProperties props,
                                                   Option<PartialUpdateMode> partialUpdateModeOpt) {
    /**
     * This part implements KEEP_VALUES partial update mode, which merges two records that do not have all columns.
     * Other Partial update modes, like IGNORE_DEFAULTS assume all columns exists in the record,
     * but some columns contain specific values that should be replaced by that from older version of the record.
     */
    if (enablePartialMerging) {
      BufferedRecordMerger<T> deleteRecordMerger = create(
          readerContext, recordMergeMode, false, recordMerger, orderingFieldNames, readerSchema, payloadClasses, props, Option.empty());
      return new PartialUpdateBufferedRecordMerger<>(readerContext.getRecordContext(), recordMerger, deleteRecordMerger, orderingFieldNames, readerSchema, props);
    }

    // might need to introduce a merge config for the factory in the future to get rid of this.
    // props = readerContext.getMergeProps(props);
    switch (recordMergeMode) {
      case COMMIT_TIME_ORDERING:
        if (partialUpdateModeOpt.isEmpty()) {
          return new CommitTimeRecordMerger<>();
        }
        return new CommitTimePartialRecordMerger<>(readerContext.getRecordContext(), partialUpdateModeOpt.get(), props);
      case EVENT_TIME_ORDERING:
        if (partialUpdateModeOpt.isEmpty()) {
          return new EventTimeRecordMerger<>();
        }
        return new EventTimePartialRecordMerger<>(readerContext.getRecordContext(), partialUpdateModeOpt.get(), props);
      default:
        if (payloadClasses.isPresent()) {
          if (payloadClasses.get().getRight().equals("org.apache.spark.sql.hudi.command.payload.ExpressionPayload")) {
            return new ExpressionPayloadRecordMerger<>(readerContext.getRecordContext(), recordMerger, orderingFieldNames,
                payloadClasses.get().getLeft(), payloadClasses.get().getRight(), readerSchema, props);
          } else {
            return new CustomPayloadRecordMerger<>(readerContext.getRecordContext(), recordMerger, orderingFieldNames, payloadClasses.get().getLeft(), readerSchema, props);
          }
        } else {
          return new CustomRecordMerger<>(readerContext.getRecordContext(), recordMerger, orderingFieldNames, readerSchema, props);
        }
    }
  }

  /**
   * An implementation of {@link BufferedRecordMerger} which merges {@link BufferedRecord}s
   * based on {@code COMMIT_TIME_ORDERING} merge mode.
   */
  private static class CommitTimeRecordMerger<T> implements BufferedRecordMerger<T> {
    @Override
    public Option<BufferedRecord<T>> deltaMerge(BufferedRecord<T> newRecord, BufferedRecord<T> existingRecord) {
      return Option.of(newRecord);
    }

    @Override
    public Option<DeleteRecord> deltaMerge(DeleteRecord deleteRecord, BufferedRecord<T> existingRecord) {
      return Option.of(deleteRecord);
    }

    @Override
    public BufferedRecord<T> finalMerge(BufferedRecord<T> olderRecord, BufferedRecord<T> newerRecord) {
      return newerRecord;
    }
  }

  /**
   * An implementation of {@link BufferedRecordMerger} which merges {@link BufferedRecord}s
   * based on {@code COMMIT_TIME_ORDERING} merge mode and partial update mode.
   */
  private static class CommitTimePartialRecordMerger<T> extends CommitTimeRecordMerger<T> {
    private final PartialUpdateHandler<T> partialUpdateHandler;
    private final RecordContext<T> recordContext;

    public CommitTimePartialRecordMerger(RecordContext<T> recordContext,
                                         PartialUpdateMode partialUpdateMode,
                                         TypedProperties props) {
      super();
      this.partialUpdateHandler = new PartialUpdateHandler<>(recordContext, partialUpdateMode, props);
      this.recordContext = recordContext;
    }

    @Override
    public Option<BufferedRecord<T>> deltaMerge(BufferedRecord<T> newRecord,
                                                BufferedRecord<T> existingRecord) {
      if (existingRecord != null) {
        Schema newSchema = recordContext.getSchemaFromBufferRecord(newRecord);
        newRecord = partialUpdateHandler.partialMerge(
            newRecord,
            existingRecord,
            newSchema,
            recordContext.getSchemaFromBufferRecord(existingRecord),
            newSchema);
      }
      return Option.of(newRecord);
    }

    @Override
    public BufferedRecord<T> finalMerge(BufferedRecord<T> olderRecord,
                                     BufferedRecord<T> newerRecord) {
      Schema newSchema = recordContext.getSchemaFromBufferRecord(newerRecord);
      newerRecord = partialUpdateHandler.partialMerge(
          newerRecord,
          olderRecord,
          newSchema,
          recordContext.getSchemaFromBufferRecord(olderRecord),
          newSchema);
      return newerRecord;
    }
  }

  /**
   * An implementation of {@link BufferedRecordMerger} which merges {@link BufferedRecord}s
   * based on {@code EVENT_TIME_ORDERING} merge mode.
   */
  private static class EventTimeRecordMerger<T> implements BufferedRecordMerger<T> {
    @Override
    public Option<BufferedRecord<T>> deltaMerge(BufferedRecord<T> newRecord, BufferedRecord<T> existingRecord) {
      if (existingRecord == null || shouldKeepNewerRecord(existingRecord, newRecord)) {
        return Option.of(newRecord);
      }
      return Option.empty();
    }

    @Override
    public Option<DeleteRecord> deltaMerge(DeleteRecord deleteRecord, BufferedRecord<T> existingRecord) {
      return deltaMergeDeleteRecord(deleteRecord, existingRecord);
    }

    @Override
    public BufferedRecord<T> finalMerge(BufferedRecord<T> olderRecord, BufferedRecord<T> newerRecord) {
      if (shouldKeepNewerRecord(olderRecord, newerRecord)) {
        return newerRecord;
      }
      return olderRecord;
    }
  }

  /**
   * An implementation of {@link EventTimeRecordMerger} which merges {@link BufferedRecord}s
   * based on {@code EVENT_TIME_ORDERING} merge mode and partial update mode.
   */
  private static class EventTimePartialRecordMerger<T> extends EventTimeRecordMerger<T> {
    private final PartialUpdateHandler<T> partialUpdateHandler;
    private final RecordContext<T> recordContext;

    public EventTimePartialRecordMerger(RecordContext<T> recordContext,
                                        PartialUpdateMode partialUpdateMode,
                                        TypedProperties props) {
      this.partialUpdateHandler = new PartialUpdateHandler<>(recordContext, partialUpdateMode, props);
      this.recordContext = recordContext;
    }

    @Override
    public Option<BufferedRecord<T>> deltaMerge(BufferedRecord<T> newRecord, BufferedRecord<T> existingRecord) {
      if (existingRecord == null) {
        return Option.of(newRecord);
      } else if (shouldKeepNewerRecord(existingRecord, newRecord)) {
        Schema newSchema = recordContext.getSchemaFromBufferRecord(newRecord);
        newRecord = partialUpdateHandler.partialMerge(
            newRecord,
            existingRecord,
            newSchema,
            recordContext.getSchemaFromBufferRecord(existingRecord),
            newSchema);
        return Option.of(newRecord);
      } else {
        // Use existing record as the base record since existing record has higher ordering value.
        Schema newSchema = recordContext.getSchemaFromBufferRecord(newRecord);
        existingRecord = partialUpdateHandler.partialMerge(
            existingRecord,
            newRecord,
            recordContext.getSchemaFromBufferRecord(existingRecord),
            newSchema,
            newSchema);
        return Option.of(existingRecord);
      }
    }

    @Override
    public BufferedRecord<T> finalMerge(BufferedRecord<T> olderRecord, BufferedRecord<T> newerRecord) {
      if (newerRecord.isCommitTimeOrderingDelete()) {
        return newerRecord;
      }

      Comparable newOrderingValue = newerRecord.getOrderingValue();
      Comparable oldOrderingValue = olderRecord.getOrderingValue();
      Schema newSchema = recordContext.getSchemaFromBufferRecord(newerRecord);
      if (!olderRecord.isCommitTimeOrderingDelete()
          && oldOrderingValue.compareTo(newOrderingValue) > 0) {
        // Use old record as the base record since old record has higher ordering value.
        olderRecord = partialUpdateHandler.partialMerge(
            olderRecord,
            newerRecord,
            recordContext.getSchemaFromBufferRecord(olderRecord),
            newSchema,
            newSchema);
        return olderRecord;
      }

      newerRecord = partialUpdateHandler.partialMerge(
          newerRecord,
          olderRecord,
          newSchema,
          recordContext.getSchemaFromBufferRecord(olderRecord),
          newSchema);
      return newerRecord;
    }
  }

  /**
   * An implementation of {@link BufferedRecordMerger} which merges {@link BufferedRecord}s
   * based on partial update merging.
   */
  private static class PartialUpdateBufferedRecordMerger<T> implements BufferedRecordMerger<T> {
    private final RecordContext<T> recordContext;
    private final Option<HoodieRecordMerger> recordMerger;
    private final BufferedRecordMerger<T> deleteRecordMerger;
    private final Schema readerSchema;
    private final TypedProperties props;
    private final String[] orderingFields;

    public PartialUpdateBufferedRecordMerger(
        RecordContext<T> recordContext,
        Option<HoodieRecordMerger> recordMerger,
        BufferedRecordMerger<T> deleteRecordMerger,
        List<String> orderingFieldNames,
        Schema readerSchema,
        TypedProperties props) {
      this.recordContext = recordContext;
      this.recordMerger = recordMerger;
      this.deleteRecordMerger = deleteRecordMerger;
      this.readerSchema = readerSchema;
      this.props = props;
      this.orderingFields = orderingFieldNames.toArray(new String[0]);
    }

    @Override
    public Option<BufferedRecord<T>> deltaMerge(BufferedRecord<T> newRecord, BufferedRecord<T> existingRecord) throws IOException {
      if (existingRecord == null) {
        return Option.of(newRecord);
      }
      // TODO(HUDI-7843): decouple the merging logic from the merger
      //  and use the record merge mode to control how to merge partial updates
      // Merge and store the combined record
      Pair<HoodieRecord, Schema> combinedRecordAndSchema = recordMerger.get().partialMerge(
          recordContext.constructHoodieRecord(existingRecord),
          recordContext.getSchemaFromBufferRecord(existingRecord),
          recordContext.constructHoodieRecord(newRecord),
          recordContext.getSchemaFromBufferRecord(newRecord),
          readerSchema,
          props);
      HoodieRecord<T> combinedRecord = combinedRecordAndSchema.getLeft();

      // If pre-combine returns existing record, no need to update it
      if (combinedRecord.getData() != existingRecord.getRecord()) {
        return Option.of(BufferedRecords.fromHoodieRecord(combinedRecord, combinedRecordAndSchema.getRight(), recordContext, props, orderingFields));
      }
      return Option.empty();
    }

    @Override
    public Option<DeleteRecord> deltaMerge(DeleteRecord deleteRecord, BufferedRecord<T> existingRecord) {
      return this.deleteRecordMerger.deltaMerge(deleteRecord, existingRecord);
    }

    @Override
    public BufferedRecord<T> finalMerge(BufferedRecord<T> olderRecord, BufferedRecord<T> newerRecord) throws IOException {
      // TODO(HUDI-7843): decouple the merging logic from the merger
      //  and use the record merge mode to control how to merge partial updates
      Pair<HoodieRecord, Schema> mergedRecord = recordMerger.get().partialMerge(
          recordContext.constructHoodieRecord(olderRecord), recordContext.getSchemaFromBufferRecord(olderRecord),
          recordContext.constructHoodieRecord(newerRecord), recordContext.getSchemaFromBufferRecord(newerRecord),
          readerSchema, props);

      if (!mergedRecord.getLeft().isDelete(mergedRecord.getRight(), props)) {
        HoodieRecord hoodieRecord = mergedRecord.getLeft();
        if (!mergedRecord.getRight().equals(readerSchema)) {
          hoodieRecord = hoodieRecord.rewriteRecordWithNewSchema(mergedRecord.getRight(), null, readerSchema);
        }
        return BufferedRecords.fromHoodieRecord(hoodieRecord, readerSchema, recordContext, props, orderingFields, false);
      }
      return BufferedRecords.createDelete(newerRecord.getRecordKey());
    }
  }

  /**
   * An implementation of {@link BufferedRecordMerger} which merges {@link BufferedRecord}s
   * based on {@code CUSTOM} merge mode.
   */
  private static class CustomRecordMerger<T> extends BaseCustomMerger<T> {
    private final String[] orderingFields;

    public CustomRecordMerger(
        RecordContext<T> recordContext,
        Option<HoodieRecordMerger> recordMerger,
        List<String> orderingFieldNames,
        Schema readerSchema,
        TypedProperties props) {
      super(recordContext, recordMerger, readerSchema, props);
      this.orderingFields = orderingFieldNames.toArray(new String[0]);
    }

    @Override
    public Option<BufferedRecord<T>> deltaMergeNonDeleteRecord(BufferedRecord<T> newRecord, BufferedRecord<T> existingRecord) throws IOException {
      Pair<HoodieRecord, Schema> combinedRecordAndSchema = recordMerger.merge(
          recordContext.constructHoodieRecord(existingRecord),
          recordContext.getSchemaFromBufferRecord(existingRecord),
          recordContext.constructHoodieRecord(newRecord),
          recordContext.getSchemaFromBufferRecord(newRecord),
          props);

      HoodieRecord<T> combinedRecord = combinedRecordAndSchema.getLeft();

      // If pre-combine returns existing record, no need to update it
      if (combinedRecord.getData() != existingRecord.getRecord()) {
        return Option.of(BufferedRecords.fromHoodieRecord(combinedRecord, combinedRecordAndSchema.getRight(), recordContext, props, orderingFields));
      }
      return Option.empty();
    }

    @Override
    public BufferedRecord<T> mergeNonDeleteRecord(BufferedRecord<T> olderRecord, BufferedRecord<T> newerRecord) throws IOException {
      Pair<HoodieRecord, Schema> mergedRecord = recordMerger.merge(
          recordContext.constructHoodieRecord(olderRecord), recordContext.getSchemaFromBufferRecord(olderRecord),
          recordContext.constructHoodieRecord(newerRecord), recordContext.getSchemaFromBufferRecord(newerRecord), props);
      if (!mergedRecord.getLeft().isDelete(mergedRecord.getRight(), props)) {
        HoodieRecord hoodieRecord = mergedRecord.getLeft();
        if (!mergedRecord.getRight().equals(readerSchema)) {
          hoodieRecord = hoodieRecord.rewriteRecordWithNewSchema(mergedRecord.getRight(), null, readerSchema);
        }
        return BufferedRecords.fromHoodieRecord(hoodieRecord, readerSchema, recordContext, props, orderingFields, false);
      }
      return BufferedRecords.createDelete(newerRecord.getRecordKey());
    }
  }

  /**
   * An implementation of {@link BufferedRecordMerger} which merges {@link BufferedRecord}s based on the ExpressionPayload.
   * The delta merge expects the incoming records to both be Expression Payload, whereas the final merge expects the existing
   * record payload to match the table's configured payload and the new record to be an Expression Payload.
   */
  private static class ExpressionPayloadRecordMerger<T> extends CustomPayloadRecordMerger<T> {
    private final String basePayloadClass;
    private final HoodieRecordMerger deltaMerger;

    public ExpressionPayloadRecordMerger(RecordContext<T> recordContext, Option<HoodieRecordMerger> recordMerger, List<String> orderingFieldNames, String basePayloadClass, String incomingPayloadClass,
                                         Schema readerSchema, TypedProperties props) {
      super(recordContext, recordMerger, orderingFieldNames, incomingPayloadClass, readerSchema, props);
      this.basePayloadClass = basePayloadClass;
      this.deltaMerger = HoodieRecordUtils.mergerToPreCombineMode(recordMerger.get());
    }

    @Override
    protected Pair<HoodieRecord, HoodieRecord> getFinalMergeRecords(BufferedRecord<T> olderRecord, BufferedRecord<T> newerRecord) {
      HoodieRecord oldHoodieRecord = constructHoodieAvroRecord(recordContext, olderRecord, basePayloadClass);
      HoodieRecord newHoodieRecord = constructHoodieAvroRecord(recordContext, newerRecord, payloadClass);
      return Pair.of(oldHoodieRecord, newHoodieRecord);
    }

    @Override
    protected Pair<HoodieRecord, Schema> getMergedRecord(BufferedRecord<T> olderRecord, BufferedRecord<T> newerRecord, boolean isFinalMerge) throws IOException {
      if (isFinalMerge) {
        return super.getMergedRecord(olderRecord, newerRecord, isFinalMerge);
      } else {
        Pair<HoodieRecord, HoodieRecord> records = getDeltaMergeRecords(olderRecord, newerRecord);
        return deltaMerger.merge(records.getLeft(), getSchemaForAvroPayloadMerge(olderRecord), records.getRight(), getSchemaForAvroPayloadMerge(newerRecord), props);
      }
    }
  }

  /**
   * An implementation of {@link BufferedRecordMerger} which merges {@link BufferedRecord}s
   * based on {@code CUSTOM} merge mode and a given record payload class.
   */
  private static class CustomPayloadRecordMerger<T> extends BaseCustomMerger<T> {
    private final String[] orderingFieldNames;
    protected final String payloadClass;

    public CustomPayloadRecordMerger(
        RecordContext<T> recordContext,
        Option<HoodieRecordMerger> recordMerger,
        List<String> orderingFieldNames,
        String payloadClass,
        Schema readerSchema,
        TypedProperties props) {
      super(recordContext, recordMerger, readerSchema, props);
      this.orderingFieldNames = orderingFieldNames.toArray(new String[0]);
      this.payloadClass = payloadClass;
    }

    @Override
    public Option<BufferedRecord<T>> deltaMergeNonDeleteRecord(BufferedRecord<T> newRecord, BufferedRecord<T> existingRecord) throws IOException {
      Pair<HoodieRecord, Schema> mergedRecordAndSchema = getMergedRecord(existingRecord, newRecord, false);
      HoodieRecord mergedRecord = mergedRecordAndSchema.getLeft();
      Schema mergeResultSchema = mergedRecordAndSchema.getRight();
      // Special handling for SENTINEL record in Expression Payload. This is returned if the condition does not match.
      if (mergedRecord.getData() == HoodieRecord.SENTINEL) {
        return Option.empty();
      }
      T combinedRecordData = recordContext.convertAvroRecord(mergedRecord.toIndexedRecord(mergeResultSchema, props).get().getData());
      // If pre-combine does not return existing record, update it
      if (combinedRecordData != existingRecord.getRecord()) {
        // For pkless we need to use record key from existing record
        return Option.of(BufferedRecords.fromEngineRecord(combinedRecordData, mergeResultSchema, recordContext, orderingFieldNames,
            existingRecord.getRecordKey(), mergedRecord.isDelete(mergeResultSchema, props)));
      }
      return Option.empty();
    }

    @Override
    public BufferedRecord<T> mergeNonDeleteRecord(BufferedRecord<T> olderRecord, BufferedRecord<T> newerRecord) throws IOException {
      Pair<HoodieRecord, Schema> mergedRecordAndSchema = getMergedRecord(olderRecord, newerRecord, true);

      HoodieRecord mergedRecord = mergedRecordAndSchema.getLeft();
      Schema mergeResultSchema = mergedRecordAndSchema.getRight();
      // Special handling for SENTINEL record in Expression Payload
      if (mergedRecord.getData() == HoodieRecord.SENTINEL) {
        return olderRecord;
      }
      if (!mergedRecord.isDelete(mergeResultSchema, props)) {
        IndexedRecord indexedRecord = (IndexedRecord) mergedRecord.getData();
        return BufferedRecords.fromEngineRecord(
            recordContext.convertAvroRecord(indexedRecord), mergeResultSchema, recordContext, orderingFieldNames, newerRecord.getRecordKey(), false);
      }
      return BufferedRecords.createDelete(newerRecord.getRecordKey());
    }

    protected Pair<HoodieRecord, HoodieRecord> getDeltaMergeRecords(BufferedRecord<T> olderRecord, BufferedRecord<T> newerRecord) {
      HoodieRecord oldHoodieRecord = constructHoodieAvroRecord(recordContext, olderRecord, payloadClass);
      HoodieRecord newHoodieRecord = constructHoodieAvroRecord(recordContext, newerRecord, payloadClass);
      return Pair.of(oldHoodieRecord, newHoodieRecord);
    }

    protected Pair<HoodieRecord, HoodieRecord> getFinalMergeRecords(BufferedRecord<T> olderRecord, BufferedRecord<T> newerRecord) {
      return getDeltaMergeRecords(olderRecord, newerRecord);
    }

    protected Pair<HoodieRecord, Schema> getMergedRecord(BufferedRecord<T> olderRecord, BufferedRecord<T> newerRecord, boolean isFinalMerge) throws IOException {
      Pair<HoodieRecord, HoodieRecord> records = isFinalMerge ? getFinalMergeRecords(olderRecord, newerRecord) : getDeltaMergeRecords(olderRecord, newerRecord);
      return recordMerger.merge(records.getLeft(), getSchemaForAvroPayloadMerge(olderRecord), records.getRight(), getSchemaForAvroPayloadMerge(newerRecord), props);
    }

    protected HoodieRecord constructHoodieAvroRecord(RecordContext<T> recordContext, BufferedRecord<T> bufferedRecord, String payloadClass) {
      GenericRecord record = null;
      if (!bufferedRecord.isDelete()) {
        Schema recordSchema = recordContext.getSchemaFromBufferRecord(bufferedRecord);
        record = recordContext.convertToAvroRecord(bufferedRecord.getRecord(), recordSchema);
      }
      HoodieKey hoodieKey = new HoodieKey(bufferedRecord.getRecordKey(), null);
      return new HoodieAvroRecord<>(hoodieKey,
          HoodieRecordUtils.loadPayload(payloadClass, record, bufferedRecord.getOrderingValue()), null);
    }

    protected Schema getSchemaForAvroPayloadMerge(BufferedRecord<T> bufferedRecord) {
      if (bufferedRecord.getSchemaId() == null) {
        return readerSchema;
      }
      return recordContext.getSchemaFromBufferRecord(bufferedRecord);
    }
  }

  /**
   * A base implementation of {@link BufferedRecordMerger} which merges {@link BufferedRecord}s
   * based on {@code CUSTOM} merge mode.
   */
  private abstract static class BaseCustomMerger<T> implements BufferedRecordMerger<T> {
    protected final RecordContext<T> recordContext;
    protected final HoodieRecordMerger recordMerger;
    protected final Schema readerSchema;
    protected final TypedProperties props;

    public BaseCustomMerger(
        RecordContext<T> recordContext,
        Option<HoodieRecordMerger> recordMerger,
        Schema readerSchema,
        TypedProperties props) {
      this.recordContext = recordContext;
      this.recordMerger = recordMerger.orElseThrow(() -> new IllegalArgumentException("RecordMerger must be present for custom merging"));
      this.readerSchema = readerSchema;
      this.props = props;
    }

    @Override
    public Option<BufferedRecord<T>> deltaMerge(BufferedRecord<T> newRecord, BufferedRecord<T> existingRecord) throws IOException {
      if (existingRecord == null) {
        return Option.of(newRecord);
      }
      if (existingRecord.isDelete() || newRecord.isDelete()) {
        if (shouldKeepNewerRecord(existingRecord, newRecord)) {
          // IMPORTANT:
          // this is needed when the fallback HoodieAvroRecordMerger got used, the merger would
          // return Option.empty when the old payload data is empty(a delete) and ignores its ordering value directly.
          return Option.of(newRecord);
        } else {
          return Option.empty();
        }
      }
      return deltaMergeNonDeleteRecord(newRecord, existingRecord);
    }

    public abstract Option<BufferedRecord<T>> deltaMergeNonDeleteRecord(BufferedRecord<T> newRecord, BufferedRecord<T> existingRecord) throws IOException;

    @Override
    public Option<DeleteRecord> deltaMerge(DeleteRecord deleteRecord, BufferedRecord<T> existingRecord) {
      return deltaMergeDeleteRecord(deleteRecord, existingRecord);
    }

    @Override
    public BufferedRecord<T> finalMerge(BufferedRecord<T> olderRecord, BufferedRecord<T> newerRecord) throws IOException {
      if (olderRecord.isDelete() || newerRecord.isDelete()) {
        if (shouldKeepNewerRecord(olderRecord, newerRecord)) {
          // IMPORTANT:
          // this is needed when the fallback HoodieAvroRecordMerger got used, the merger would
          // return Option.empty when the new payload data is empty(a delete) and ignores its ordering value directly.
          return newerRecord;
        } else {
          return olderRecord;
        }
      }
      return mergeNonDeleteRecord(olderRecord, newerRecord);
    }

    public abstract BufferedRecord<T> mergeNonDeleteRecord(BufferedRecord<T> olderRecord, BufferedRecord<T> newerRecord) throws IOException;
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  private static <T> Option<DeleteRecord> deltaMergeDeleteRecord(DeleteRecord deleteRecord, BufferedRecord<T> existingRecord) {
    if (existingRecord == null) {
      return Option.of(deleteRecord);
    }
    if (existingRecord.isCommitTimeOrderingDelete()) {
      return Option.empty();
    }
    Comparable existingOrderingVal = existingRecord.getOrderingValue();
    Comparable deleteOrderingVal = deleteRecord.getOrderingValue();
    // Checks the ordering value does not equal to 0
    // because we use 0 as the default value which means natural order
    boolean chooseExisting = !OrderingValues.isDefault(deleteOrderingVal)
        && OrderingValues.isSameClass(existingOrderingVal, deleteOrderingVal)
        && existingOrderingVal.compareTo(deleteOrderingVal) > 0;
    if (chooseExisting) {
      // The DELETE message is obsolete if the old message has greater orderingVal.
      return Option.empty();
    }
    return Option.of(deleteRecord);
  }

  private static <T> boolean shouldKeepNewerRecord(BufferedRecord<T> oldRecord, BufferedRecord<T> newRecord) {
    if (newRecord.isCommitTimeOrderingDelete() || oldRecord.isCommitTimeOrderingDelete()) {
      // handle records coming from DELETE statements
      // The orderingVal is constant 0 (int) and not guaranteed to match the type of the old or new record's ordering value.
      return true;
    }
    return newRecord.getOrderingValue().compareTo(oldRecord.getOrderingValue()) >= 0;
  }
}
