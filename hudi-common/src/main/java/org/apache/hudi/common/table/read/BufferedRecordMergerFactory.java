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
                                                   PartialUpdateMode partialUpdateMode) {
    /**
     * This part implements KEEP_VALUES partial update mode, which merges two records that do not have all columns.
     * Other Partial update modes, like IGNORE_DEFAULTS assume all columns exists in the record,
     * but some columns contain specific values that should be replaced by that from older version of the record.
     */
    if (enablePartialMerging) {
      BufferedRecordMerger<T> deleteRecordMerger = create(
          readerContext, recordMergeMode, false, recordMerger, orderingFieldNames, payloadClass, readerSchema, props, partialUpdateMode);
      return new PartialUpdateBufferedRecordMerger<>(readerContext.getRecordContext(), recordMerger, deleteRecordMerger, orderingFieldNames, readerSchema, props);
    }

    switch (recordMergeMode) {
      case COMMIT_TIME_ORDERING:
        if (partialUpdateMode == PartialUpdateMode.NONE) {
          return new CommitTimeRecordMerger<>();
        }
        return new CommitTimePartialRecordMerger<>(readerContext.getRecordContext(), partialUpdateMode, props);
      case EVENT_TIME_ORDERING:
        if (partialUpdateMode == PartialUpdateMode.NONE) {
          return new EventTimeRecordMerger<>();
        }
        return new EventTimePartiaRecordMerger<>(readerContext.getRecordContext(), partialUpdateMode, props);
      default:
        if (payloadClass.isPresent()) {
          return new CustomPayloadRecordMerger<>(
              readerContext.getRecordContext(), recordMerger, orderingFieldNames, payloadClass.get(), readerSchema, props);
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
    public MergeResult<T> finalMerge(BufferedRecord<T> olderRecord, BufferedRecord<T> newerRecord) {
      return new MergeResult<>(newerRecord.isDelete(), newerRecord.getRecord());
    }
  }

  /**
   * An implementation of {@link BufferedRecordMerger} which merges {@link BufferedRecord}s
   * based on {@code COMMIT_TIME_ORDERING} merge mode and partial update mode.
   */
  private static class CommitTimePartialRecordMerger<T> extends CommitTimeRecordMerger<T> {
    private final PartialUpdateStrategy<T> partialUpdateStrategy;
    private final RecordContext<T> recordContext;

    public CommitTimePartialRecordMerger(RecordContext<T> recordContext,
                                         PartialUpdateMode partialUpdateMode,
                                         TypedProperties props) {
      super();
      this.partialUpdateStrategy = new PartialUpdateStrategy<>(recordContext, partialUpdateMode, props);
      this.recordContext = recordContext;
    }

    @Override
    public Option<BufferedRecord<T>> deltaMerge(BufferedRecord<T> newRecord,
                                                BufferedRecord<T> existingRecord) {
      if (existingRecord != null) {
        newRecord = partialUpdateStrategy.partialMerge(
            newRecord,
            existingRecord,
            recordContext.getSchemaFromBufferRecord(newRecord),
            recordContext.getSchemaFromBufferRecord(existingRecord),
            false);
      }
      return Option.of(newRecord);
    }

    @Override
    public MergeResult<T> finalMerge(BufferedRecord<T> olderRecord,
                                     BufferedRecord<T> newerRecord) {
      newerRecord = partialUpdateStrategy.partialMerge(
          newerRecord,
          olderRecord,
          recordContext.getSchemaFromBufferRecord(newerRecord),
          recordContext.getSchemaFromBufferRecord(olderRecord),
          false);
      return new MergeResult<>(newerRecord.isDelete(), newerRecord.getRecord());
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
    public MergeResult<T> finalMerge(BufferedRecord<T> olderRecord, BufferedRecord<T> newerRecord) {
      if (shouldKeepNewerRecord(olderRecord, newerRecord)) {
        return new MergeResult<>(newerRecord.isDelete(), newerRecord.getRecord());
      }
      return new MergeResult<>(olderRecord.isDelete(), olderRecord.getRecord());
    }
  }

  /**
   * An implementation of {@link EventTimeRecordMerger} which merges {@link BufferedRecord}s
   * based on {@code EVENT_TIME_ORDERING} merge mode and partial update mode.
   */
  private static class EventTimePartiaRecordMerger<T> extends EventTimeRecordMerger<T> {
    private final PartialUpdateStrategy<T> partialUpdateStrategy;
    private final RecordContext<T> recordContext;

    public EventTimePartiaRecordMerger(RecordContext<T> recordContext,
                                       PartialUpdateMode partialUpdateMode,
                                       TypedProperties props) {
      this.partialUpdateStrategy = new PartialUpdateStrategy<>(recordContext, partialUpdateMode, props);
      this.recordContext = recordContext;
    }

    @Override
    public Option<BufferedRecord<T>> deltaMerge(BufferedRecord<T> newRecord, BufferedRecord<T> existingRecord) {
      if (existingRecord == null) {
        return Option.of(newRecord);
      } else if (shouldKeepNewerRecord(existingRecord, newRecord)) {
        newRecord = partialUpdateStrategy.partialMerge(
            newRecord,
            existingRecord,
            recordContext.getSchemaFromBufferRecord(newRecord),
            recordContext.getSchemaFromBufferRecord(existingRecord),
            false);
        return Option.of(newRecord);
      } else {
        // Use existing record as the base record since existing record has higher ordering value.
        existingRecord = partialUpdateStrategy.partialMerge(
            existingRecord,
            newRecord,
            recordContext.getSchemaFromBufferRecord(existingRecord),
            recordContext.getSchemaFromBufferRecord(newRecord),
            true);
        return Option.of(existingRecord);
      }
    }

    @Override
    public MergeResult<T> finalMerge(BufferedRecord<T> olderRecord, BufferedRecord<T> newerRecord) {
      if (newerRecord.isCommitTimeOrderingDelete()) {
        return new MergeResult<>(true, newerRecord.getRecord());
      }

      Comparable newOrderingValue = newerRecord.getOrderingValue();
      Comparable oldOrderingValue = olderRecord.getOrderingValue();
      if (!olderRecord.isCommitTimeOrderingDelete()
          && oldOrderingValue.compareTo(newOrderingValue) > 0) {
        // Use old record as the base record since old record has higher ordering value.
        olderRecord = partialUpdateStrategy.partialMerge(
            olderRecord,
            newerRecord,
            recordContext.getSchemaFromBufferRecord(olderRecord),
            recordContext.getSchemaFromBufferRecord(newerRecord),
            true);
        return new MergeResult<>(olderRecord.isDelete(), olderRecord.getRecord());
      }

      newerRecord = partialUpdateStrategy.partialMerge(
          newerRecord,
          olderRecord,
          recordContext.getSchemaFromBufferRecord(newerRecord),
          recordContext.getSchemaFromBufferRecord(olderRecord),
          false);
      return new MergeResult<>(newerRecord.isDelete(), newerRecord.getRecord());
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
      Option<Pair<HoodieRecord, Schema>> combinedRecordAndSchemaOpt = recordMerger.get().partialMerge(
          recordContext.constructHoodieRecord(existingRecord),
          recordContext.getSchemaFromBufferRecord(existingRecord),
          recordContext.constructHoodieRecord(newRecord),
          recordContext.getSchemaFromBufferRecord(newRecord),
          readerSchema,
          props);
      if (!combinedRecordAndSchemaOpt.isPresent()) {
        return Option.empty();
      }
      Pair<HoodieRecord, Schema> combinedRecordAndSchema = combinedRecordAndSchemaOpt.get();
      HoodieRecord<T> combinedRecord = combinedRecordAndSchema.getLeft();

      // If pre-combine returns existing record, no need to update it
      if (combinedRecord.getData() != existingRecord.getRecord()) {
        return Option.of(BufferedRecord.forRecordWithContext(combinedRecord, combinedRecordAndSchema.getRight(), recordContext, props, orderingFields));
      }
      return Option.empty();
    }

    @Override
    public Option<DeleteRecord> deltaMerge(DeleteRecord deleteRecord, BufferedRecord<T> existingRecord) {
      return this.deleteRecordMerger.deltaMerge(deleteRecord, existingRecord);
    }

    @Override
    public MergeResult<T> finalMerge(BufferedRecord<T> olderRecord, BufferedRecord<T> newerRecord) throws IOException {
      // TODO(HUDI-7843): decouple the merging logic from the merger
      //  and use the record merge mode to control how to merge partial updates
      Option<Pair<HoodieRecord, Schema>> mergedRecord = recordMerger.get().partialMerge(
          recordContext.constructHoodieRecord(olderRecord), recordContext.getSchemaFromBufferRecord(olderRecord),
          recordContext.constructHoodieRecord(newerRecord), recordContext.getSchemaFromBufferRecord(newerRecord),
          readerSchema, props);

      if (mergedRecord.isPresent()
          && !mergedRecord.get().getLeft().isDelete(mergedRecord.get().getRight(), props)) {
        HoodieRecord hoodieRecord = mergedRecord.get().getLeft();
        if (!mergedRecord.get().getRight().equals(readerSchema)) {
          T data = (T) hoodieRecord.rewriteRecordWithNewSchema(mergedRecord.get().getRight(), null, readerSchema).getData();
          return new MergeResult<>(false, data);
        }
        return new MergeResult<>(false, (T) hoodieRecord.getData());
      }
      return new MergeResult<>(true, null);
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
      Option<Pair<HoodieRecord, Schema>> combinedRecordAndSchemaOpt = recordMerger.merge(
          recordContext.constructHoodieRecord(existingRecord),
          recordContext.getSchemaFromBufferRecord(existingRecord),
          recordContext.constructHoodieRecord(newRecord),
          recordContext.getSchemaFromBufferRecord(newRecord),
          props);

      if (!combinedRecordAndSchemaOpt.isPresent()) {
        // An empty Option indicates that the output represents a delete.
        return Option.of(new BufferedRecord<>(newRecord.getRecordKey(), OrderingValues.getDefault(), null, null, true));
      }

      Pair<HoodieRecord, Schema> combinedRecordAndSchema = combinedRecordAndSchemaOpt.get();
      HoodieRecord<T> combinedRecord = combinedRecordAndSchema.getLeft();

      // If pre-combine returns existing record, no need to update it
      if (combinedRecord.getData() != existingRecord.getRecord()) {
        return Option.of(BufferedRecord.forRecordWithContext(combinedRecord, combinedRecordAndSchema.getRight(), recordContext, props, orderingFields));
      }
      return Option.empty();
    }

    @Override
    public MergeResult<T> mergeNonDeleteRecord(BufferedRecord<T> olderRecord, BufferedRecord<T> newerRecord) throws IOException {
      Option<Pair<HoodieRecord, Schema>> mergedRecord = recordMerger.merge(
          recordContext.constructHoodieRecord(olderRecord), recordContext.getSchemaFromBufferRecord(olderRecord),
          recordContext.constructHoodieRecord(newerRecord), recordContext.getSchemaFromBufferRecord(newerRecord), props);
      if (mergedRecord.isPresent()
          && !mergedRecord.get().getLeft().isDelete(mergedRecord.get().getRight(), props)) {
        HoodieRecord hoodieRecord = mergedRecord.get().getLeft();
        if (!mergedRecord.get().getRight().equals(readerSchema)) {
          return new MergeResult<>(false, (T) hoodieRecord.rewriteRecordWithNewSchema(mergedRecord.get().getRight(), null, readerSchema).getData());
        }
        return new MergeResult<>(false, (T) hoodieRecord.getData());
      }
      return new MergeResult<>(true, null);
    }
  }

  /**
   * An implementation of {@link BufferedRecordMerger} which merges {@link BufferedRecord}s
   * based on {@code CUSTOM} merge mode and a given record payload class.
   */
  private static class CustomPayloadRecordMerger<T> extends BaseCustomMerger<T> {
    private final List<String> orderingFieldNames;
    private final String payloadClass;

    public CustomPayloadRecordMerger(
        RecordContext<T> recordContext,
        Option<HoodieRecordMerger> recordMerger,
        List<String> orderingFieldNames,
        String payloadClass,
        Schema readerSchema,
        TypedProperties props) {
      super(recordContext, recordMerger, readerSchema, props);
      this.orderingFieldNames = orderingFieldNames;
      this.payloadClass = payloadClass;
    }

    @Override
    public Option<BufferedRecord<T>> deltaMergeNonDeleteRecord(BufferedRecord<T> newRecord, BufferedRecord<T> existingRecord) throws IOException {
      Option<Pair<HoodieRecord, Schema>> combinedRecordAndSchemaOpt = getMergedRecord(existingRecord, newRecord);
      if (combinedRecordAndSchemaOpt.isPresent()) {
        T combinedRecordData = recordContext.convertAvroRecord((IndexedRecord) combinedRecordAndSchemaOpt.get().getLeft().getData());
        // If pre-combine does not return existing record, update it
        if (combinedRecordData != existingRecord.getRecord()) {
          Pair<HoodieRecord, Schema> combinedRecordAndSchema = combinedRecordAndSchemaOpt.get();
          return Option.of(BufferedRecord.forRecordWithContext(combinedRecordData, combinedRecordAndSchema.getRight(), recordContext, orderingFieldNames, false));
        }
        return Option.empty();
      }
      // An empty Option indicates that the output represents a delete.
      return Option.of(new BufferedRecord<>(newRecord.getRecordKey(), OrderingValues.getDefault(), null, null, true));
    }

    @Override
    public MergeResult<T> mergeNonDeleteRecord(BufferedRecord<T> olderRecord, BufferedRecord<T> newerRecord) throws IOException {
      Option<Pair<HoodieRecord, Schema>> mergedRecord =
          getMergedRecord(olderRecord, newerRecord);
      if (mergedRecord.isPresent()
          && !mergedRecord.get().getLeft().isDelete(mergedRecord.get().getRight(), props)) {
        IndexedRecord indexedRecord;
        if (!mergedRecord.get().getRight().equals(readerSchema)) {
          indexedRecord = (IndexedRecord) mergedRecord.get().getLeft().rewriteRecordWithNewSchema(mergedRecord.get().getRight(), null, readerSchema).getData();
        } else {
          indexedRecord = (IndexedRecord) mergedRecord.get().getLeft().getData();
        }
        return new MergeResult<>(false, recordContext.convertAvroRecord(indexedRecord));
      }
      return new MergeResult<>(true, null);
    }

    private Option<Pair<HoodieRecord, Schema>> getMergedRecord(BufferedRecord<T> olderRecord, BufferedRecord<T> newerRecord) throws IOException {
      HoodieRecord oldHoodieRecord = constructHoodieAvroRecord(recordContext, olderRecord);
      HoodieRecord newHoodieRecord = constructHoodieAvroRecord(recordContext, newerRecord);
      Option<Pair<HoodieRecord, Schema>> mergedRecord = recordMerger.merge(
          oldHoodieRecord, getSchemaForAvroPayloadMerge(oldHoodieRecord, olderRecord),
          newHoodieRecord, getSchemaForAvroPayloadMerge(newHoodieRecord, newerRecord), props);
      return mergedRecord;
    }

    private HoodieRecord constructHoodieAvroRecord(RecordContext<T> recordContext, BufferedRecord<T> bufferedRecord) {
      GenericRecord record = null;
      if (!bufferedRecord.isDelete()) {
        Schema recordSchema = recordContext.getSchemaFromBufferRecord(bufferedRecord);
        record = recordContext.convertToAvroRecord(bufferedRecord.getRecord(), recordSchema);
      }
      HoodieKey hoodieKey = new HoodieKey(bufferedRecord.getRecordKey(), null);
      return new HoodieAvroRecord<>(hoodieKey,
          HoodieRecordUtils.loadPayload(payloadClass, record, bufferedRecord.getOrderingValue()), null);
    }

    private Schema getSchemaForAvroPayloadMerge(HoodieRecord record, BufferedRecord<T> bufferedRecord) throws IOException {
      if (record.isDelete(readerSchema, props)) {
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
    public MergeResult<T> finalMerge(BufferedRecord<T> olderRecord, BufferedRecord<T> newerRecord) throws IOException {
      if (olderRecord.isDelete() || newerRecord.isDelete()) {
        if (shouldKeepNewerRecord(olderRecord, newerRecord)) {
          // IMPORTANT:
          // this is needed when the fallback HoodieAvroRecordMerger got used, the merger would
          // return Option.empty when the new payload data is empty(a delete) and ignores its ordering value directly.
          return new MergeResult<>(newerRecord.isDelete(), newerRecord.getRecord());
        } else {
          return new MergeResult<>(olderRecord.isDelete(), olderRecord.getRecord());
        }
      }
      return mergeNonDeleteRecord(olderRecord, newerRecord);
    }

    public abstract MergeResult<T> mergeNonDeleteRecord(BufferedRecord<T> olderRecord, BufferedRecord<T> newerRecord) throws IOException;
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
