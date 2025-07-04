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
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.Objects;

/**
 * Factory to create a {@link BufferedRecordMerger}.
 */
public class BufferedRecordMergerFactory {
  public static <T> BufferedRecordMerger<T> create(HoodieReaderContext<T> readerContext,
                                                   RecordMergeMode recordMergeMode,
                                                   boolean enablePartialMerging,
                                                   Option<HoodieRecordMerger> recordMerger,
                                                   Option<String> orderingFieldName,
                                                   Option<String> payloadClass,
                                                   Schema readerSchema,
                                                   TypedProperties props) {
    if (enablePartialMerging) {
      BufferedRecordMerger<T> deleteRecordMerger = create(
          readerContext, recordMergeMode, false, recordMerger, orderingFieldName, payloadClass, readerSchema, props);
      return new PartialUpdateBufferedRecordMerger<>(readerContext, recordMerger, deleteRecordMerger, readerSchema, props);
    }
    switch (recordMergeMode) {
      case COMMIT_TIME_ORDERING:
        return new CommitTimeBufferedRecordMerger<>();
      case EVENT_TIME_ORDERING:
        return new EventTimeBufferedRecordMerger<>();
      default:
        if (payloadClass.isPresent()) {
          return new CustomPayloadBufferedRecordMerger<>(readerContext, recordMerger, orderingFieldName, payloadClass.get(), readerSchema, props);
        } else {
          return new CustomBufferedRecordMerger<>(readerContext, recordMerger, readerSchema, props);
        }
    }
  }

  /**
   * An implementation of {@link BufferedRecordMerger} which merges {@link BufferedRecord}s
   * based on {@code COMMIT_TIME_ORDERING} merge mode.
   */
  private static class CommitTimeBufferedRecordMerger<T> implements BufferedRecordMerger<T> {
    @Override
    public Option<BufferedRecord<T>> deltaMerge(BufferedRecord<T> newRecord, BufferedRecord<T> existingRecord) {
      return Option.of(newRecord);
    }

    @Override
    public Option<DeleteRecord> deltaMerge(DeleteRecord deleteRecord, BufferedRecord<T> existingRecord) {
      return Option.of(deleteRecord);
    }

    @Override
    public Pair<Boolean, T> finalMerge(BufferedRecord<T> olderRecord, BufferedRecord<T> newerRecord) {
      return Pair.of(newerRecord.isDelete(), newerRecord.getRecord());
    }
  }

  /**
   * An implementation of {@link BufferedRecordMerger} which merges {@link BufferedRecord}s
   * based on {@code EVENT_TIME_ORDERING} merge mode.
   */
  private static class EventTimeBufferedRecordMerger<T> implements BufferedRecordMerger<T> {
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
    public Pair<Boolean, T> finalMerge(BufferedRecord<T> olderRecord, BufferedRecord<T> newerRecord) {
      if (newerRecord.isCommitTimeOrderingDelete()) {
        return Pair.of(true, newerRecord.getRecord());
      }
      Comparable newOrderingValue = newerRecord.getOrderingValue();
      Comparable oldOrderingValue = olderRecord.getOrderingValue();
      if (!olderRecord.isCommitTimeOrderingDelete()
          && oldOrderingValue.compareTo(newOrderingValue) > 0) {
        return Pair.of(olderRecord.isDelete(), olderRecord.getRecord());
      }
      return Pair.of(newerRecord.isDelete(), newerRecord.getRecord());
    }
  }

  /**
   * An implementation of {@link BufferedRecordMerger} which merges {@link BufferedRecord}s
   * based on partial update merging.
   */
  private static class PartialUpdateBufferedRecordMerger<T> implements BufferedRecordMerger<T> {
    private final HoodieReaderContext<T> readerContext;
    private final Option<HoodieRecordMerger> recordMerger;
    private final BufferedRecordMerger<T> deleteRecordMerger;
    private final Schema readerSchema;
    private final TypedProperties props;

    public PartialUpdateBufferedRecordMerger(
        HoodieReaderContext<T> readerContext,
        Option<HoodieRecordMerger> recordMerger,
        BufferedRecordMerger<T> deleteRecordMerger,
        Schema readerSchema,
        TypedProperties props) {
      this.readerContext = readerContext;
      this.recordMerger = recordMerger;
      this.deleteRecordMerger = deleteRecordMerger;
      this.readerSchema = readerSchema;
      this.props = props;
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
          readerContext.constructHoodieRecord(existingRecord),
          readerContext.getSchemaFromBufferRecord(existingRecord),
          readerContext.constructHoodieRecord(newRecord),
          readerContext.getSchemaFromBufferRecord(newRecord),
          readerSchema,
          props);
      if (!combinedRecordAndSchemaOpt.isPresent()) {
        return Option.empty();
      }
      Pair<HoodieRecord, Schema> combinedRecordAndSchema = combinedRecordAndSchemaOpt.get();
      HoodieRecord<T> combinedRecord = combinedRecordAndSchema.getLeft();

      // If pre-combine returns existing record, no need to update it
      if (combinedRecord.getData() != existingRecord.getRecord()) {
        return Option.of(BufferedRecord.forRecordWithContext(combinedRecord, combinedRecordAndSchema.getRight(), readerContext, props));
      }
      return Option.empty();
    }

    @Override
    public Option<DeleteRecord> deltaMerge(DeleteRecord deleteRecord, BufferedRecord<T> existingRecord) {
      return this.deleteRecordMerger.deltaMerge(deleteRecord, existingRecord);
    }

    @Override
    public Pair<Boolean, T> finalMerge(BufferedRecord<T> olderRecord, BufferedRecord<T> newerRecord) throws IOException {
      // TODO(HUDI-7843): decouple the merging logic from the merger
      //  and use the record merge mode to control how to merge partial updates
      Option<Pair<HoodieRecord, Schema>> mergedRecord = recordMerger.get().partialMerge(
          readerContext.constructHoodieRecord(olderRecord), readerContext.getSchemaFromBufferRecord(olderRecord),
          readerContext.constructHoodieRecord(newerRecord), readerContext.getSchemaFromBufferRecord(newerRecord),
          readerSchema, props);

      if (mergedRecord.isPresent()
          && !mergedRecord.get().getLeft().isDelete(mergedRecord.get().getRight(), props)) {
        HoodieRecord hoodieRecord = mergedRecord.get().getLeft();
        if (!mergedRecord.get().getRight().equals(readerSchema)) {
          T data = (T) hoodieRecord.rewriteRecordWithNewSchema(mergedRecord.get().getRight(), null, readerSchema).getData();
          return Pair.of(false, data);
        }
        return Pair.of(false, (T) hoodieRecord.getData());
      }
      return Pair.of(true, null);
    }
  }

  /**
   * An implementation of {@link BufferedRecordMerger} which merges {@link BufferedRecord}s
   * based on {@code CUSTOM} merge mode.
   */
  private static class CustomBufferedRecordMerger<T> extends BaseCustomMerger<T> {
    public CustomBufferedRecordMerger(
        HoodieReaderContext<T> readerContext,
        Option<HoodieRecordMerger> recordMerger,
        Schema readerSchema,
        TypedProperties props) {
      super(readerContext, recordMerger, readerSchema, props);
    }

    @Override
    public Option<BufferedRecord<T>> deltaMergeNonDeleteRecord(BufferedRecord<T> newRecord, BufferedRecord<T> existingRecord) throws IOException {
      Option<Pair<HoodieRecord, Schema>> combinedRecordAndSchemaOpt = recordMerger.get().merge(
          readerContext.constructHoodieRecord(existingRecord),
          readerContext.getSchemaFromBufferRecord(existingRecord),
          readerContext.constructHoodieRecord(newRecord),
          readerContext.getSchemaFromBufferRecord(newRecord),
          props);

      if (!combinedRecordAndSchemaOpt.isPresent()) {
        return Option.empty();
      }

      Pair<HoodieRecord, Schema> combinedRecordAndSchema = combinedRecordAndSchemaOpt.get();
      HoodieRecord<T> combinedRecord = combinedRecordAndSchema.getLeft();

      // If pre-combine returns existing record, no need to update it
      if (combinedRecord.getData() != existingRecord.getRecord()) {
        return Option.of(BufferedRecord.forRecordWithContext(combinedRecord, combinedRecordAndSchema.getRight(), readerContext, props));
      }
      return Option.empty();
    }

    @Override
    public Pair<Boolean, T> mergeNonDeleteRecord(BufferedRecord<T> olderRecord, BufferedRecord<T> newerRecord) throws IOException {
      Option<Pair<HoodieRecord, Schema>> mergedRecord = recordMerger.get().merge(
          readerContext.constructHoodieRecord(olderRecord), readerContext.getSchemaFromBufferRecord(olderRecord),
          readerContext.constructHoodieRecord(newerRecord), readerContext.getSchemaFromBufferRecord(newerRecord), props);
      if (mergedRecord.isPresent()
          && !mergedRecord.get().getLeft().isDelete(mergedRecord.get().getRight(), props)) {
        HoodieRecord hoodieRecord = mergedRecord.get().getLeft();
        if (!mergedRecord.get().getRight().equals(readerSchema)) {
          return Pair.of(false, (T) hoodieRecord.rewriteRecordWithNewSchema(mergedRecord.get().getRight(), null, readerSchema).getData());
        }
        return Pair.of(false, (T) hoodieRecord.getData());
      }
      return Pair.of(true, null);
    }
  }

  /**
   * An implementation of {@link BufferedRecordMerger} which merges {@link BufferedRecord}s
   * based on {@code CUSTOM} merge mode and a given record payload class.
   */
  private static class CustomPayloadBufferedRecordMerger<T> extends BaseCustomMerger<T> {
    private final Option<String> orderingFieldName;
    private final String payloadClass;

    public CustomPayloadBufferedRecordMerger(
        HoodieReaderContext<T> readerContext,
        Option<HoodieRecordMerger> recordMerger,
        Option<String> orderingFieldName,
        String payloadClass,
        Schema readerSchema,
        TypedProperties props) {
      super(readerContext, recordMerger, readerSchema, props);
      this.orderingFieldName = orderingFieldName;
      this.payloadClass = payloadClass;
    }

    @Override
    public Option<BufferedRecord<T>> deltaMergeNonDeleteRecord(BufferedRecord<T> newRecord, BufferedRecord<T> existingRecord) throws IOException {
      Option<Pair<HoodieRecord, Schema>> combinedRecordAndSchemaOpt = getMergedRecord(existingRecord, newRecord);
      if (combinedRecordAndSchemaOpt.isPresent()) {
        T combinedRecordData = readerContext.convertAvroRecord((IndexedRecord) combinedRecordAndSchemaOpt.get().getLeft().getData());
        // If pre-combine does not return existing record, update it
        if (combinedRecordData != existingRecord.getRecord()) {
          Pair<HoodieRecord, Schema> combinedRecordAndSchema = combinedRecordAndSchemaOpt.get();
          return Option.of(BufferedRecord.forRecordWithContext(combinedRecordData, combinedRecordAndSchema.getRight(), readerContext, orderingFieldName, false));
        }
      }
      return Option.empty();
    }

    @Override
    public Pair<Boolean, T> mergeNonDeleteRecord(BufferedRecord<T> olderRecord, BufferedRecord<T> newerRecord) throws IOException {
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
        return Pair.of(false, readerContext.convertAvroRecord(indexedRecord));
      }
      return Pair.of(true, null);
    }

    private Option<Pair<HoodieRecord, Schema>> getMergedRecord(BufferedRecord<T> olderRecord, BufferedRecord<T> newerRecord) throws IOException {
      ValidationUtils.checkArgument(!Objects.equals(payloadClass, OverwriteWithLatestAvroPayload.class.getCanonicalName())
          && !Objects.equals(payloadClass, DefaultHoodieRecordPayload.class.getCanonicalName()));
      HoodieRecord oldHoodieRecord = constructHoodieAvroRecord(readerContext, olderRecord);
      HoodieRecord newHoodieRecord = constructHoodieAvroRecord(readerContext, newerRecord);
      Option<Pair<HoodieRecord, Schema>> mergedRecord = recordMerger.get().merge(
          oldHoodieRecord, getSchemaForAvroPayloadMerge(oldHoodieRecord, olderRecord),
          newHoodieRecord, getSchemaForAvroPayloadMerge(newHoodieRecord, newerRecord), props);
      return mergedRecord;
    }

    private HoodieRecord constructHoodieAvroRecord(HoodieReaderContext<T> readerContext, BufferedRecord<T> bufferedRecord) {
      GenericRecord record = null;
      if (!bufferedRecord.isDelete()) {
        Schema recordSchema = readerContext.getSchemaFromBufferRecord(bufferedRecord);
        record = readerContext.convertToAvroRecord(bufferedRecord.getRecord(), recordSchema);
      }
      HoodieKey hoodieKey = new HoodieKey(bufferedRecord.getRecordKey(), null);
      return new HoodieAvroRecord<>(hoodieKey,
          HoodieRecordUtils.loadPayload(payloadClass, record, bufferedRecord.getOrderingValue()), null);
    }

    private Schema getSchemaForAvroPayloadMerge(HoodieRecord record, BufferedRecord<T> bufferedRecord) throws IOException {
      if (record.isDelete(readerSchema, props)) {
        return readerSchema;
      }
      return readerContext.getSchemaFromBufferRecord(bufferedRecord);
    }
  }

  /**
   * A base implementation of {@link BufferedRecordMerger} which merges {@link BufferedRecord}s
   * based on {@code CUSTOM} merge mode.
   */
  private abstract static class BaseCustomMerger<T> implements BufferedRecordMerger<T> {
    protected final HoodieReaderContext<T> readerContext;
    protected final Option<HoodieRecordMerger> recordMerger;
    protected final Schema readerSchema;
    protected final TypedProperties props;

    public BaseCustomMerger(
        HoodieReaderContext<T> readerContext,
        Option<HoodieRecordMerger> recordMerger,
        Schema readerSchema,
        TypedProperties props) {
      this.readerContext = readerContext;
      this.recordMerger = recordMerger;
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
    public Pair<Boolean, T> finalMerge(BufferedRecord<T> olderRecord, BufferedRecord<T> newerRecord) throws IOException {
      if (olderRecord.isDelete() || newerRecord.isDelete()) {
        if (shouldKeepNewerRecord(olderRecord, newerRecord)) {
          // IMPORTANT:
          // this is needed when the fallback HoodieAvroRecordMerger got used, the merger would
          // return Option.empty when the new payload data is empty(a delete) and ignores its ordering value directly.
          return Pair.of(newerRecord.isDelete(), newerRecord.getRecord());
        } else {
          return Pair.of(olderRecord.isDelete(), olderRecord.getRecord());
        }
      }
      return mergeNonDeleteRecord(olderRecord, newerRecord);
    }

    public abstract Pair<Boolean, T> mergeNonDeleteRecord(BufferedRecord<T> olderRecord, BufferedRecord<T> newerRecord) throws IOException;
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
    boolean chooseExisting = !deleteOrderingVal.equals(0)
        && ReflectionUtils.isSameClass(existingOrderingVal, deleteOrderingVal)
        && existingOrderingVal.compareTo(deleteOrderingVal) > 0;
    if (chooseExisting) {
      // The DELETE message is obsolete if the old message has greater orderingVal.
      return Option.empty();
    }
    return Option.of(deleteRecord);
  }

  private static <T> boolean shouldKeepNewerRecord(BufferedRecord<T> oldRecord, BufferedRecord<T> newRecord) {
    if (newRecord.isCommitTimeOrderingDelete()) {
      // handle records coming from DELETE statements(the orderingVal is constant 0)
      return true;
    }
    return newRecord.getOrderingValue().compareTo(oldRecord.getOrderingValue()) >= 0;
  }
}
