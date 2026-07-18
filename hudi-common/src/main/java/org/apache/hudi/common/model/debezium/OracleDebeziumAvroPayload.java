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

package org.apache.hudi.common.model.debezium;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.model.PartialUpdateAvroPayload;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.OrderingValues;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.IndexedRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static org.apache.hudi.common.model.debezium.PostgresDebeziumAvroPayload.DEBEZIUM_TOASTED_VALUE;

/**
 * Payload for Oracle Debezium CDC events, designed to work with {@code OracleDebeziumTransformer}.
 *
 * <p>Extends {@link PartialUpdateAvroPayload} for preCombine scaffolding and ordering support,
 * but <b>fully overrides</b> the merge logic in {@link #combineAndGetUpdateValue} and
 * {@link #preCombine} with Oracle-specific handling:
 * <ul>
 *   <li><b>changed_columns awareness:</b> The transformer populates a {@code _changed_columns} field
 *       with comma-separated names of columns whose before/after values differ. During merge, if a
 *       field is in {@code _changed_columns}, the incoming value is used (even if null); otherwise the
 *       previous stored value is preserved (the column was unchanged or absent from the after image).</li>
 *   <li><b>Toasted value handling:</b> Columns with {@value PostgresDebeziumAvroPayload#DEBEZIUM_TOASTED_VALUE}
 *       are replaced with the existing stored value.</li>
 * </ul>
 *
 * <p>Ordering is based on the composite {@code _event_ordering} field (zero-padded commit_scn.scn),
 * configured via {@code hoodie.payload.ordering.field}.
 *
 * <p><b>Note on table versions.</b> On table version 9 (and beyond) Oracle CDC merging is handled by
 * the built-in {@code EVENT_TIME_ORDERING} merge mode combined with the {@code FILL_UNCHANGED} partial
 * update mode (see {@code HoodieTableConfig.handlePartialUpdateModeConfigs}); this payload is not
 * invoked at read time there. It remains the merge implementation for older (payload-based) tables and
 * the identifier the table-config inference keys on to derive the v9 merge configuration.
 *
 * <p><b>Scope of use — Debezium ingestion only.</b> This payload's correctness arguments depend on
 * source-side invariants that only the Oracle Debezium connector guarantees:
 * <ul>
 *   <li>Every record carries the Debezium envelope fields ({@code op}, {@code _changed_columns},
 *       {@code _event_ordering}, {@code _hoodie_is_deleted}) populated by {@code OracleDebeziumTransformer}.</li>
 *   <li>Event semantics follow Oracle redo-log DML: an INSERT (op='c') carries full real column
 *       values in its {@code after} image; an UPDATE (op='u') carries the diff via
 *       {@code _changed_columns}; a DELETE (op='d') under PK-only supplemental logging carries
 *       only the PK plus placeholder zeros for other columns. In particular, an UPDATE is never
 *       emitted for a non-existent PK — Oracle requires the row to exist, so any UPDATE has a
 *       preceding INSERT.</li>
 *   <li>Ordering values come from monotonically-increasing Oracle SCNs.</li>
 * </ul>
 * <b>Do not use this payload for non-Debezium write paths</b> — direct Spark SQL writes, the
 * Hudi DataSource API, bulk inserts, backfills, custom transformer chains, or any path that
 * doesn't go through {@code OracleDebeziumTransformer}. Such paths can easily violate the assumptions
 * above (missing op field, missing {@code _changed_columns}, UPDATE emitted without a prior INSERT,
 * non-SCN ordering values) and produce silently incorrect merge results. For non-CDC writes, use the
 * standard Hudi payloads (e.g., {@link OverwriteWithLatestAvroPayload}).
 */
public class OracleDebeziumAvroPayload extends PartialUpdateAvroPayload {

  private static final Logger LOG = LoggerFactory.getLogger(OracleDebeziumAvroPayload.class);

  /**
   * Metadata/internal fields that are not user data columns. These should always take the newer
   * record's value during merge since they reflect the event that was applied, not user data
   * subject to supplemental logging fallbacks.
   */
  private static final Set<String> METADATA_FIELDS = new HashSet<>(Arrays.asList(
      // Note: CHANGED_COLUMNS_FIELD is intentionally excluded — it has custom merge logic above.
      DebeziumConstants.FLATTENED_SCN_COL_NAME,
      DebeziumConstants.FLATTENED_COMMIT_SCN_COL_NAME,
      DebeziumConstants.FLATTENED_ORDERING_COL_NAME,
      // Nested-mode layout (the production default): scn/commit_scn/timestamps/shard arrive inside
      // this single struct column instead of the flat columns above. It must take the newer event's
      // value like the rest.
      DebeziumConstants.DEBEZIUM_METADATA_FIELD,
      DebeziumConstants.FLATTENED_OP_COL_NAME,
      DebeziumConstants.UPSTREAM_PROCESSING_TS_COL_NAME,
      DebeziumConstants.FLATTENED_SHARD_NAME,
      DebeziumConstants.FLATTENED_TS_COL_NAME,
      HoodieRecord.HOODIE_IS_DELETED_FIELD
  ));

  public OracleDebeziumAvroPayload(GenericRecord record, Comparable orderingVal) {
    super(record, orderingVal);
  }

  public OracleDebeziumAvroPayload(Option<GenericRecord> record) {
    super(record);
  }

  /**
   * Recognize a Debezium delete via the op field ({@code _change_operation_type == "d"}) in
   * addition to the inherited {@code _hoodie_is_deleted}-based check. Mirrors
   * {@code AbstractDebeziumAvroPayload}'s Postgres/MySQL behavior.
   *
   * <p>Why this is necessary: Hudi's MOR snapshot-read delete filter consults the table's
   * read schema via {@code DeleteContext.hasBuiltInDeleteField}, which only fires when the
   * read schema contains {@code _hoodie_is_deleted}. If a pipeline's schema provider strips
   * that meta-field from the target schema for any reason, Debezium delete events flow
   * through MOR snapshot reads as "zombie" rows (NULL data + the original recordkey) until
   * compaction merges them away.
   *
   * <p>By overriding {@code isDeleteRecord} to also check the Debezium op field — which is
   * always part of the data columns produced by {@code AbstractDebeziumTransformer} — the
   * {@code isDeletedRecord} flag on {@code BaseAvroPayload} is set correctly at payload
   * construction time, even if {@code _hoodie_is_deleted} is absent. That flag drives
   * {@code BaseAvroPayload.isDeleted(schema, props)}, which the reader uses for snapshot-read
   * filtering, so deletes are filtered before compaction.
   *
   * <p>{@link #mergeOldRecordWithModifiedColumns} also benefits via polymorphism: its
   * {@code isDeleteRecord(newerRecord)} / {@code isDeleteRecord(olderRecord)} calls
   * resolve to this override, so the compaction-time merge path picks up the op-based
   * signal too.
   */
  @Override
  protected boolean isDeleteRecord(GenericRecord record) {
    return isDebeziumDeleteRecord(record) || super.isDeleteRecord(record);
  }

  private static boolean isDebeziumDeleteRecord(GenericRecord record) {
    if (record == null) {
      return false;
    }
    Schema.Field opField = record.getSchema().getField(DebeziumConstants.FLATTENED_OP_COL_NAME);
    if (opField == null) {
      return false;
    }
    Object value = record.get(DebeziumConstants.FLATTENED_OP_COL_NAME);
    return value != null && DebeziumConstants.DELETE_OP.equalsIgnoreCase(value.toString());
  }

  /**
   * Bypass the inherited delete-check for the (Schema, Properties) overload: return the actual
   * record bytes even when the payload is flagged as a delete. This is a workaround for
   * unchecked {@code .get()} call sites in {@code HoodieAvroRecord} that throw
   * {@code NoSuchElementException} when a delete payload flows through schema-evolution paths
   * ({@code rewriteRecordWithNewSchema}, {@code truncateRecordKey},
   * {@code wrapIntoHoodieRecordPayloadWithParams}, {@code prependMetaFields}).
   *
   * <p>The delete marker ({@code _hoodie_is_deleted=true}) is preserved on the returned record,
   * so downstream callers that check {@code BaseAvroPayload.isDeleted(...)} (used by
   * {@code HoodieAvroRecord.isDelete} for BaseAvroPayload subclasses) still treat it as a
   * delete. The single-argument {@code getInsertValue(Schema)} is unchanged — callers that
   * rely on empty-return for delete filtering continue to get the standard behavior.
   *
   * <p>This override is a bandaid — the proper fix is to add {@code isPresent()} guards in
   * {@code HoodieAvroRecord}. Remove this override once that fix ships.
   */
  @Override
  public Option<IndexedRecord> getInsertValue(Schema schema, Properties props) throws IOException {
    byte[] bytes = getRecordBytes();
    if (bytes.length == 0) {
      // Payload was constructed with a null record — genuinely nothing to return.
      return Option.empty();
    }
    // Return the record regardless of isDeletedRecord flag.
    return Option.of((IndexedRecord) HoodieAvroUtils.bytesToAvro(bytes, schema));
  }

  @Override
  public OracleDebeziumAvroPayload preCombine(OverwriteWithLatestAvroPayload oldValue, Schema schema, Properties properties) {
    if (isEmptyRecord()) {
      return this;
    }
    final boolean shouldPickOldRecord = oldValue.getOrderingVal().compareTo(getOrderingVal()) > 0;
    try {
      Option<IndexedRecord> oldRecordOpt = oldValue.getInsertValue(schema);
      if (!oldRecordOpt.isPresent()) {
        // oldValue is a delete record
        if (shouldPickOldRecord) {
          // Delete has higher ordering — it should win. Return the delete payload as-is.
          return (OracleDebeziumAvroPayload) oldValue;
        }
        // This (non-delete) has higher ordering — it wins
        return this;
      }
      GenericRecord oldRecord = (GenericRecord) oldRecordOpt.get();
      Option<IndexedRecord> mergedRecord = mergeOldRecordWithModifiedColumns(oldRecord, schema, shouldPickOldRecord, true);
      if (mergedRecord.isPresent()) {
        return new OracleDebeziumAvroPayload((GenericRecord) mergedRecord.get(),
            shouldPickOldRecord ? oldValue.getOrderingVal() : this.getOrderingVal());
      }
    } catch (Exception ex) {
      LOG.error("OracleDebeziumAvroPayload preCombine failed, falling back to incoming record", ex);
      return this;
    }
    return this;
  }

  /**
   * Merges incoming record with the current stored record. Without Properties, the ordering field
   * cannot be extracted from the current record, so the incoming record is always treated as newer.
   * Use the Properties overload for correct out-of-order handling.
   */
  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema) throws IOException {
    return mergeOldRecordWithModifiedColumns(currentValue, schema, false, false);
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema, Properties prop) throws IOException {
    return mergeOldRecordWithModifiedColumns(currentValue, schema, isCurrentRecordNewer(currentValue, prop), false);
  }

  private boolean isCurrentRecordNewer(IndexedRecord currentRecord, Properties prop) {
    String[] orderingFields = ConfigUtils.getOrderingFields(prop);
    if (orderingFields != null) {
      boolean consistentLogicalTimestampEnabled = Boolean.parseBoolean(prop.getProperty(
          KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.key(),
          KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.defaultValue()));
      Comparable currentOrderingVal = OrderingValues.create(
          orderingFields,
          field -> (Comparable) HoodieAvroUtils.getNestedFieldVal((GenericRecord) currentRecord, field, true, consistentLogicalTimestampEnabled));
      return currentOrderingVal != null
          && OrderingValues.isSameClass(currentOrderingVal, getOrderingVal())
          && currentOrderingVal.compareTo(getOrderingVal()) > 0;
    }
    return false;
  }

  private Option<IndexedRecord> mergeOldRecordWithModifiedColumns(
      IndexedRecord oldRecord, Schema schema, boolean isOldRecordNewer, boolean isPreCombining) throws IOException {
    // Pass isPreCombining=true to bypass the inherited delete-check in
    // PartialUpdateAvroPayload.getInsertValue, so a delete payload's content is still
    // accessible here; actual delete signalling happens at the isDeleteRecord(newerRecord)
    // branch below. Empty here means the payload was constructed with a null record (no
    // bytes) — match Postgres/MySQL and signal delete via empty-return.
    Option<IndexedRecord> recordOption = getInsertValue(schema, true);
    if (!recordOption.isPresent()) {
      return Option.empty();
    }

    GenericRecord incomingRecord = (GenericRecord) recordOption.get();
    GenericRecord storedRecord = (GenericRecord) oldRecord;

    // newerRecord: the record with higher _event_ordering — its non-null values take priority.
    // olderRecord: provides fallback values when newerRecord's fields are null/toasted.
    GenericRecord newerRecord = isOldRecordNewer ? storedRecord : incomingRecord;
    GenericRecord olderRecord = isOldRecordNewer ? incomingRecord : storedRecord;

    if (isDeleteRecord(newerRecord)) {
      if (isPreCombining) {
        // preCombine: propagate the delete marker forward in the in-memory buffer so it can
        // be persisted as a tombstone and re-applied during the next merge.
        return Option.of(newerRecord);
      }
      // combineAndGetUpdateValue: emit empty — this is Hudi's standard delete signal. The
      // reader / compaction path drops the row from the output when the merge returns empty.
      // Emitting a non-empty tombstone record instead would leave the row visible in reads
      // (a soft-delete marker with _hoodie_is_deleted=true isn't filtered by default).
      return Option.empty();
    }

    // If the older record is a delete tombstone, its non-PK field values are unreliable
    // (Oracle PK-only supplemental logging leaves null / zero-value placeholders in the
    // `before` image of delete events). Skip the field merge entirely and emit the newer
    // record as-is. Under the Debezium CDC contract (see class-level javadoc), newerRecord
    // in this branch is always an INSERT (op='c') or SNAPSHOT (op='r') because Oracle never
    // emits an UPDATE for a non-existent PK — any UPDATE has a preceding INSERT that
    // collapses the tombstone before this pairing occurs. Both INSERT and SNAPSHOT events
    // carry full real column values in their `after` image (PK-only supplemental logging
    // does not affect INSERTs), so emitting newerRecord as-is is correct.
    // WARNING: this argument holds only for the Debezium ingestion path. If this payload
    // is ever used with non-Debezium writes (Spark SQL, DataSource API, bulk inserts), the
    // newerRecord-is-always-an-INSERT invariant no longer holds.
    if (isDeleteRecord(olderRecord)) {
      return Option.of(newerRecord);
    }

    // Extract _changed_columns from both records.
    // newerChangedCols lists columns whose values changed in the newer record's CDC event.
    Set<String> newerChangedCols = extractChangedColumns(newerRecord);
    Set<String> olderChangedCols = extractChangedColumns(olderRecord);

    GenericRecordBuilder builder = new GenericRecordBuilder(schema);
    for (Schema.Field field : schema.getFields()) {
      String fieldName = field.name();
      Object newerValue = newerRecord.get(fieldName);
      Object olderValue = olderRecord.get(fieldName);

      if (fieldName.equals(DebeziumConstants.CHANGED_COLUMNS_FIELD)) {
        // _changed_columns on the merged record = union of both sides' changed sets.
        // See mergeChangedColumnsSets javadoc for why we do NOT trim columns here.
        Set<String> resultChanged = mergeChangedColumnsSets(newerChangedCols, olderChangedCols);
        builder.set(field, resultChanged.isEmpty() ? null : String.join(",", resultChanged));
        continue;
      }

      if (METADATA_FIELDS.contains(fieldName)) {
        // Metadata/internal fields always take the newer record's value — they reflect the
        // event that was applied. The transformer guarantees these fields are populated.
        builder.set(field, newerValue);
      } else if (isToastedValue(newerValue, field)) {
        // Toasted value in newer record -> fall back to older record's value.
        builder.set(field, olderValue);
      } else if (newerChangedCols.contains(fieldName)) {
        // Field was changed in the newer CDC event -> use incoming value (even if null).
        builder.set(field, newerValue);
      } else if (!newerChangedCols.isEmpty()) {
        // Update event (has _changed_columns) but this field is NOT in the changed set.
        // Preserve the older value — the stored value is the source of truth for unchanged
        // columns, including a legitimate null.
        builder.set(field, olderValue);
      } else {
        // No _changed_columns present (insert, snapshot, or legacy record without change
        // tracking). Inserts/snapshots are authoritative — the `after` image is the full row
        // state. Use newerValue as-is, including a legitimate null.
        builder.set(field, newerValue);
      }
    }

    return Option.of(builder.build());
  }

  /**
   * Extracts the set of column names from the {@code _changed_columns} field.
   * These are columns whose values changed between before and after images.
   */
  private Set<String> extractChangedColumns(GenericRecord record) {
    Schema.Field field = record.getSchema().getField(DebeziumConstants.CHANGED_COLUMNS_FIELD);
    if (field == null) {
      return Collections.emptySet();
    }
    Object value = record.get(DebeziumConstants.CHANGED_COLUMNS_FIELD);
    if (value == null) {
      return Collections.emptySet();
    }
    String str = value.toString();
    if (str.isEmpty()) {
      return Collections.emptySet();
    }
    return new HashSet<>(Arrays.asList(str.split(",")));
  }

  /**
   * Merges two sets of changed columns into the union. We intentionally do NOT trim columns
   * that already have real values in the newer record: {@code _changed_columns} tracks which
   * columns were reliably delivered by a CDC event, and downstream merges depend on this
   * tracking to distinguish real values from placeholder values for non-PK columns under
   * Oracle PK-only supplemental logging. Trimming it would cause unchanged NOT NULL columns
   * to be overwritten by zero-value placeholders in subsequent merges (the next
   * {@code mergeOldRecordWithModifiedColumns} call would see an empty
   * {@code newerChangedCols} and fall into the insert/snapshot branch that emits
   * {@code newerValue} as-is).
   */
  private Set<String> mergeChangedColumnsSets(Set<String> newerChanged, Set<String> olderChanged) {
    Set<String> result = new HashSet<>(newerChanged);
    result.addAll(olderChanged);
    return result;
  }

  private boolean isToastedValue(Object value, Schema.Field field) {
    if (value == null) {
      return false;
    }
    return isStringToasted(value, field) || isBytesToasted(value, field);
  }

  private boolean isStringToasted(Object value, Schema.Field field) {
    Schema.Type type = field.schema().getType();
    if (type != Schema.Type.STRING
        && !(type == Schema.Type.UNION && hasType(field.schema(), Schema.Type.STRING))) {
      return false;
    }
    CharSequence charSeq = (CharSequence) value;
    return charSeq.length() == DEBEZIUM_TOASTED_VALUE.length()
        && DEBEZIUM_TOASTED_VALUE.equals(charSeq.toString());
  }

  private boolean isBytesToasted(Object value, Schema.Field field) {
    Schema.Type type = field.schema().getType();
    if (type != Schema.Type.BYTES
        && !(type == Schema.Type.UNION && hasType(field.schema(), Schema.Type.BYTES))) {
      return false;
    }
    byte[] bytes = ((ByteBuffer) value).array();
    return bytes.length == DEBEZIUM_TOASTED_VALUE.length()
        && DEBEZIUM_TOASTED_VALUE.equals(new String(bytes, StandardCharsets.UTF_8));
  }

  private static boolean hasType(Schema unionSchema, Schema.Type targetType) {
    for (Schema s : unionSchema.getTypes()) {
      if (s.getType() == targetType) {
        return true;
      }
    }
    return false;
  }
}
