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
import org.apache.hudi.common.model.HoodieAvroRecordMerger;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.PartialUpdateMode;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.OrderingValues;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.avro.Schema;

import java.io.IOException;

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
                                                   Option<String> payloadClass,
                                                   Schema readerSchema,
                                                   TypedProperties props,
                                                   Option<PartialUpdateMode> partialUpdateModeOpt) {
    return create(readerContext, recordMergeMode, enablePartialMerging, recordMerger,
        readerSchema, payloadClass.map(p -> Pair.of(p, p)), props, partialUpdateModeOpt);
  }

  public static <T> BufferedRecordMerger<T> create(HoodieReaderContext<T> readerContext,
                                                   RecordMergeMode recordMergeMode,
                                                   boolean enablePartialMerging,
                                                   Option<HoodieRecordMerger> recordMerger,
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
          readerContext, recordMergeMode, false, recordMerger, readerSchema, payloadClasses, props, Option.empty());
      return new PartialUpdateBufferedRecordMerger<>(readerContext.getRecordContext(), recordMerger, deleteRecordMerger, readerSchema, props);
    }

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
            return new ExpressionPayloadRecordMerger<>(readerContext.getRecordContext(), recordMerger, payloadClasses.get().getRight(), readerSchema, props);
          } else {
            return new CustomPayloadRecordMerger<>(readerContext.getRecordContext(), recordMerger, payloadClasses.get().getLeft(), readerSchema, props);
          }
        } else {
          return new CustomRecordMerger<>(readerContext.getRecordContext(), recordMerger, readerSchema, props);
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

    public PartialUpdateBufferedRecordMerger(
        RecordContext<T> recordContext,
        Option<HoodieRecordMerger> recordMerger,
        BufferedRecordMerger<T> deleteRecordMerger,
        Schema readerSchema,
        TypedProperties props) {
      this.recordContext = recordContext;
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
      BufferedRecord<T> mergedRecord = recordMerger.get().partialMerge(
          existingRecord,
          newRecord,
          readerSchema,
          recordContext,
          props);

      // If pre-combine returns existing record, no need to update it
      if (mergedRecord.getRecord() != existingRecord.getRecord()) {
        return Option.of(mergedRecord);
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
      return recordMerger.get().partialMerge(olderRecord, newerRecord, readerSchema, recordContext, props);
    }
  }

  /**
   * An implementation of {@link BufferedRecordMerger} which merges {@link BufferedRecord}s
   * based on {@code CUSTOM} merge mode.
   */
  private static class CustomRecordMerger<T> extends BaseCustomMerger<T> {

    public CustomRecordMerger(
        RecordContext<T> recordContext,
        Option<HoodieRecordMerger> recordMerger,
        Schema readerSchema,
        TypedProperties props) {
      super(recordContext, recordMerger, readerSchema, props);
    }

    @Override
    public Option<BufferedRecord<T>> deltaMergeRecords(BufferedRecord<T> newRecord, BufferedRecord<T> existingRecord) throws IOException {
      BufferedRecord<T> mergedRecord = recordMerger.merge(existingRecord, newRecord, recordContext, props);

      // If pre-combine returns existing record, no need to update it
      if (mergedRecord.getRecord() != existingRecord.getRecord()) {
        return Option.of(mergedRecord);
      }
      return Option.empty();
    }

    @Override
    public BufferedRecord<T> mergeRecords(BufferedRecord<T> olderRecord, BufferedRecord<T> newerRecord) throws IOException {
      return recordMerger.merge(olderRecord, newerRecord, recordContext, props);
    }
  }

  /**
   * An implementation of {@link BufferedRecordMerger} which merges {@link BufferedRecord}s based on the ExpressionPayload.
   * The delta merge expects the incoming records to both be Expression Payload, whereas the final merge expects the existing
   * record payload to match the table's configured payload and the new record to be an Expression Payload.
   */
  private static class ExpressionPayloadRecordMerger<T> extends CustomPayloadRecordMerger<T> {
    private final HoodieRecordMerger deltaMerger;

    public ExpressionPayloadRecordMerger(RecordContext<T> recordContext, Option<HoodieRecordMerger> recordMerger, String incomingPayloadClass,
                                         Schema readerSchema, TypedProperties props) {
      super(recordContext, recordMerger, incomingPayloadClass, readerSchema, props);
      this.deltaMerger = HoodieRecordUtils.mergerToPreCombineMode(recordMerger.get());
    }

    @Override
    protected BufferedRecord<T> getMergedRecord(BufferedRecord<T> olderRecord, BufferedRecord<T> newerRecord, boolean isFinalMerge) throws IOException {
      if (isFinalMerge) {
        return super.getMergedRecord(olderRecord, newerRecord, isFinalMerge);
      } else {
        return deltaMerger.merge(olderRecord, newerRecord, recordContext, props);
      }
    }
  }

  /**
   * An implementation of {@link BufferedRecordMerger} which merges {@link BufferedRecord}s
   * based on {@code CUSTOM} merge mode and a given record payload class.
   */
  private static class CustomPayloadRecordMerger<T> extends BaseCustomMerger<T> {
    protected final String payloadClass;

    public CustomPayloadRecordMerger(
        RecordContext<T> recordContext,
        Option<HoodieRecordMerger> recordMerger,
        String payloadClass,
        Schema readerSchema,
        TypedProperties props) {
      super(recordContext, recordMerger, readerSchema, props);
      this.payloadClass = payloadClass;
      props.setProperty(HoodieAvroRecordMerger.PAYLOAD_CLASS_PROP, payloadClass);
    }

    @Override
    public Option<BufferedRecord<T>> deltaMergeRecords(BufferedRecord<T> newRecord, BufferedRecord<T> existingRecord) throws IOException {
      BufferedRecord<T> mergedRecord = getMergedRecord(existingRecord, newRecord, false);
      // If pre-combine does not return existing record, update it
      if (mergedRecord.getRecord() != existingRecord.getRecord()) {
        return Option.of(mergedRecord);
      }
      return Option.empty();
    }

    @Override
    public BufferedRecord<T> mergeRecords(BufferedRecord<T> olderRecord, BufferedRecord<T> newerRecord) throws IOException {
      return getMergedRecord(olderRecord, newerRecord, true);
    }

    protected BufferedRecord<T> getMergedRecord(BufferedRecord<T> olderRecord, BufferedRecord<T> newerRecord, boolean isFinalMerge) throws IOException {
      return recordMerger.merge(olderRecord, newerRecord, recordContext, props);
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
      return deltaMergeRecords(newRecord, existingRecord);
    }

    public abstract Option<BufferedRecord<T>> deltaMergeRecords(BufferedRecord<T> newRecord, BufferedRecord<T> existingRecord) throws IOException;

    @Override
    public Option<DeleteRecord> deltaMerge(DeleteRecord deleteRecord, BufferedRecord<T> existingRecord) {
      BufferedRecord<T> deleteBufferedRecord = BufferedRecords.fromDeleteRecord(deleteRecord, recordContext);
      try {
        Option<BufferedRecord<T>> merged = deltaMerge(deleteBufferedRecord, existingRecord);
        // If the delete record is chosen, return an option with the delete record, otherwise return empty.
        return merged.isPresent() ? Option.of(deleteRecord) : Option.empty();
      } catch (IOException e) {
        throw new HoodieIOException("Failed to process delete record", e);
      }
    }

    @Override
    public BufferedRecord<T> finalMerge(BufferedRecord<T> olderRecord, BufferedRecord<T> newerRecord) throws IOException {
      return mergeRecords(olderRecord, newerRecord);
    }

    public abstract BufferedRecord<T> mergeRecords(BufferedRecord<T> olderRecord, BufferedRecord<T> newerRecord) throws IOException;
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
