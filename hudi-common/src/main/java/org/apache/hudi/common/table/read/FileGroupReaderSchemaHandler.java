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

package org.apache.hudi.common.table.read;

import org.apache.hudi.avro.AvroSchemaCache;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.LocalAvroSchemaCache;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.collection.Triple;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter;

import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.avro.AvroSchemaUtils.appendFieldsToSchemaDedupNested;
import static org.apache.hudi.avro.AvroSchemaUtils.createNewSchemaFromFieldsWithReference;
import static org.apache.hudi.avro.AvroSchemaUtils.findNestedField;
import static org.apache.hudi.common.model.DefaultHoodieRecordPayload.DELETE_KEY;
import static org.apache.hudi.common.model.DefaultHoodieRecordPayload.DELETE_MARKER;
import static org.apache.hudi.common.model.HoodieRecord.HOODIE_IS_DELETED_FIELD;

/**
 * This class is responsible for handling the schema for the file group reader.
 */
public class FileGroupReaderSchemaHandler<T> {

  protected final Schema tableSchema;

  // requestedSchema: the schema that the caller requests
  protected final Schema requestedSchema;

  // requiredSchema: the requestedSchema with any additional columns required for merging etc
  protected final Schema requiredSchema;

  protected final InternalSchema internalSchema;

  protected final Option<InternalSchema> internalSchemaOpt;

  protected final HoodieTableConfig hoodieTableConfig;

  protected final HoodieReaderContext<T> readerContext;

  protected final TypedProperties properties;

  protected final Option<HoodieRecordMerger> recordMerger;

  protected final boolean hasBootstrapBaseFile;
  protected boolean needsBootstrapMerge;

  protected final boolean needsMORMerge;

  private final LocalAvroSchemaCache localAvroSchemaCache;

  private final Option<Pair<String, String>> customDeleteMarkerKeyValue;
  private final boolean hasBuiltInDelete;
  private final int hoodieOperationPos;

  public FileGroupReaderSchemaHandler(HoodieReaderContext<T> readerContext,
                                      Schema tableSchema,
                                      Schema requestedSchema,
                                      Option<InternalSchema> internalSchemaOpt,
                                      HoodieTableConfig hoodieTableConfig,
                                      TypedProperties properties) {
    this.properties = properties;
    this.readerContext = readerContext;
    this.hasBootstrapBaseFile = readerContext.getHasBootstrapBaseFile();
    this.needsMORMerge = readerContext.getHasLogFiles();
    this.recordMerger = readerContext.getRecordMerger();
    this.tableSchema = tableSchema;
    this.requestedSchema = AvroSchemaCache.intern(requestedSchema);
    this.hoodieTableConfig = hoodieTableConfig;
    Pair<Option<Pair<String, String>>, Boolean> deleteConfigs = getDeleteConfigs(properties, tableSchema);
    this.customDeleteMarkerKeyValue = deleteConfigs.getLeft();
    this.hasBuiltInDelete = deleteConfigs.getRight();
    this.requiredSchema = AvroSchemaCache.intern(prepareRequiredSchema());
    this.hoodieOperationPos = Option.ofNullable(requiredSchema.getField(HoodieRecord.OPERATION_METADATA_FIELD)).map(Schema.Field::pos).orElse(-1);
    this.internalSchema = pruneInternalSchema(requiredSchema, internalSchemaOpt);
    this.internalSchemaOpt = getInternalSchemaOpt(internalSchemaOpt);
    readerContext.setNeedsBootstrapMerge(this.needsBootstrapMerge);
    this.localAvroSchemaCache = LocalAvroSchemaCache.getInstance();
  }

  public Schema getTableSchema() {
    return this.tableSchema;
  }

  public Schema getRequestedSchema() {
    return this.requestedSchema;
  }

  public Schema getRequiredSchema() {
    return this.requiredSchema;
  }

  public InternalSchema getInternalSchema() {
    return this.internalSchema;
  }

  public Option<InternalSchema> getInternalSchemaOpt() {
    return this.internalSchemaOpt;
  }

  public Option<UnaryOperator<T>> getOutputConverter() {
    if (!requestedSchema.equals(requiredSchema)) {
      return Option.of(readerContext.projectRecord(requiredSchema, requestedSchema));
    }
    return Option.empty();
  }

  public Option<Pair<String, String>> getCustomDeleteMarkerKeyValue() {
    return customDeleteMarkerKeyValue;
  }

  public boolean hasBuiltInDelete() {
    return hasBuiltInDelete;
  }

  public int getHoodieOperationPos() {
    return hoodieOperationPos;
  }

  private InternalSchema pruneInternalSchema(Schema requiredSchema, Option<InternalSchema> internalSchemaOption) {
    if (!internalSchemaOption.isPresent()) {
      return InternalSchema.getEmptyInternalSchema();
    }
    InternalSchema notPruned = internalSchemaOption.get();
    if (notPruned == null || notPruned.isEmptySchema()) {
      return InternalSchema.getEmptyInternalSchema();
    }

    return doPruneInternalSchema(requiredSchema, notPruned);
  }

  protected Option<InternalSchema> getInternalSchemaOpt(Option<InternalSchema> internalSchemaOpt) {
    return internalSchemaOpt;
  }

  protected InternalSchema doPruneInternalSchema(Schema requiredSchema, InternalSchema internalSchema) {
    return AvroInternalSchemaConverter.pruneAvroSchemaToInternalSchema(requiredSchema, internalSchema);
  }

  @VisibleForTesting
  Schema generateRequiredSchema() {
    boolean hasInstantRange = readerContext.getInstantRange().isPresent();
    if (!needsMORMerge) {
      if (hasInstantRange && !findNestedField(requestedSchema, HoodieRecord.COMMIT_TIME_METADATA_FIELD).isPresent()) {
        List<Schema.Field> addedFields = new ArrayList<>();
        addedFields.add(getField(tableSchema, HoodieRecord.COMMIT_TIME_METADATA_FIELD));
        return appendFieldsToSchemaDedupNested(requestedSchema, addedFields);
      }
      return requestedSchema;
    }

    if (hoodieTableConfig.getRecordMergeMode() == RecordMergeMode.CUSTOM) {
      if (!recordMerger.get().isProjectionCompatible()) {
        return tableSchema;
      }
    }

    List<Schema.Field> addedFields = new ArrayList<>();
    for (String field : getMandatoryFieldsForMerging(
        hoodieTableConfig, properties, tableSchema, recordMerger,
        hasBuiltInDelete, customDeleteMarkerKeyValue, hasInstantRange)) {
      if (!findNestedField(requestedSchema, field).isPresent()) {
        addedFields.add(getField(tableSchema, field));
      }
    }

    if (addedFields.isEmpty()) {
      return requestedSchema;
    }

    return appendFieldsToSchemaDedupNested(requestedSchema, addedFields);
  }

  private static String[] getMandatoryFieldsForMerging(HoodieTableConfig cfg,
                                                       TypedProperties props,
                                                       Schema tableSchema,
                                                       Option<HoodieRecordMerger> recordMerger,
                                                       boolean hasBuiltInDelete,
                                                       Option<Pair<String, String>> customDeleteMarkerKeyAndValue,
                                                       boolean hasInstantRange) {
    Triple<RecordMergeMode, String, String> mergingConfigs = HoodieTableConfig.inferCorrectMergingBehavior(
        cfg.getRecordMergeMode(),
        cfg.getPayloadClass(),
        cfg.getRecordMergeStrategyId(),
        cfg.getPreCombineField(),
        cfg.getTableVersion());

    if (mergingConfigs.getLeft() == RecordMergeMode.CUSTOM) {
      return recordMerger.get().getMandatoryFieldsForMerging(tableSchema, cfg, props);
    }

    // Use Set to avoid duplicated fields.
    Set<String> requiredFields = new HashSet<>();

    if (hasInstantRange) {
      requiredFields.add(HoodieRecord.COMMIT_TIME_METADATA_FIELD);
    }

    // Add record key fields.
    if (cfg.populateMetaFields()) {
      requiredFields.add(HoodieRecord.RECORD_KEY_METADATA_FIELD);
    } else {
      Option<String[]> fields = cfg.getRecordKeyFields();
      if (fields.isPresent()) {
        requiredFields.addAll(Arrays.asList(fields.get()));
      }
    }
    // Add precombine field for event time ordering merge mode.
    if (mergingConfigs.getLeft() == RecordMergeMode.EVENT_TIME_ORDERING) {
      String preCombine = cfg.getPreCombineField();
      if (!StringUtils.isNullOrEmpty(preCombine)) {
        requiredFields.add(preCombine);
      }
    }
    // Add `HOODIE_IS_DELETED_FIELD` field if exists.
    if (hasBuiltInDelete) {
      requiredFields.add(HoodieRecord.HOODIE_IS_DELETED_FIELD);
    }
    // Add custom delete key field if exists.
    if (customDeleteMarkerKeyAndValue.isPresent()) {
      requiredFields.add(customDeleteMarkerKeyAndValue.get().getLeft());
    }
    // Add _hoodie_operation if it exists in table schema
    if (tableSchema.getField(HoodieRecord.OPERATION_METADATA_FIELD) != null) {
      requiredFields.add(HoodieRecord.OPERATION_METADATA_FIELD);
    }

    return requiredFields.toArray(new String[0]);
  }

  protected Schema prepareRequiredSchema() {
    Schema preReorderRequiredSchema = generateRequiredSchema();
    Pair<List<Schema.Field>, List<Schema.Field>> requiredFields = getDataAndMetaCols(preReorderRequiredSchema);
    this.needsBootstrapMerge = hasBootstrapBaseFile && !requiredFields.getLeft().isEmpty() && !requiredFields.getRight().isEmpty();
    return needsBootstrapMerge
        ? createSchemaFromFields(Stream.concat(requiredFields.getLeft().stream(), requiredFields.getRight().stream()).collect(Collectors.toList()))
        : preReorderRequiredSchema;
  }

  public Pair<List<Schema.Field>,List<Schema.Field>> getBootstrapRequiredFields() {
    return getDataAndMetaCols(requiredSchema);
  }

  public Pair<List<Schema.Field>,List<Schema.Field>> getBootstrapDataFields() {
    return getDataAndMetaCols(tableSchema);
  }

  @VisibleForTesting
  static Pair<List<Schema.Field>, List<Schema.Field>> getDataAndMetaCols(Schema schema) {
    Map<Boolean, List<Schema.Field>> fieldsByMeta = schema.getFields().stream()
        //if there are no data fields, then we don't want to think the temp col is a data col
        .filter(f -> !Objects.equals(f.name(), PositionBasedFileGroupRecordBuffer.ROW_INDEX_TEMPORARY_COLUMN_NAME))
        .collect(Collectors.partitioningBy(f -> HoodieRecord.HOODIE_META_COLUMNS_WITH_OPERATION.contains(f.name())));
    return Pair.of(fieldsByMeta.getOrDefault(true, Collections.emptyList()),
        fieldsByMeta.getOrDefault(false, Collections.emptyList()));
  }

  public Schema createSchemaFromFields(List<Schema.Field> fields) {
    //fields have positions set, so we need to remove them due to avro setFields implementation
    for (int i = 0; i < fields.size(); i++) {
      Schema.Field curr = fields.get(i);
      fields.set(i, new Schema.Field(curr.name(), curr.schema(), curr.doc(), curr.defaultVal()));
    }
    return createNewSchemaFromFieldsWithReference(tableSchema, fields);
  }

  /**
   * Get {@link Schema.Field} from {@link Schema} by field name.
   */
  private static Schema.Field getField(Schema schema, String fieldName) {
    Option<Schema.Field> foundFieldOpt = findNestedField(schema, fieldName);
    if (!foundFieldOpt.isPresent()) {
      throw new IllegalArgumentException("Field: " + fieldName + " does not exist in the table schema");
    }
    return foundFieldOpt.get();
  }

  /**
   * Fetches the delete configs from the configs.
   *
   * @param props write and table configs that contain delete related properties
   * @param tableSchema table schema
   * @return a pair of custom delete marker key, value, and whether built-in delete marker
   * (`_hoodie_is_deleted`) is included.
   */
  private static Pair<Option<Pair<String, String>>, Boolean> getDeleteConfigs(TypedProperties props,
                                                                              Schema tableSchema) {
    String deleteKey = props.getProperty(DELETE_KEY);
    String deleteMarker = props.getProperty(DELETE_MARKER);
    boolean deleteKeyExists = !StringUtils.isNullOrEmpty(deleteKey);
    boolean deleteMarkerExists = !StringUtils.isNullOrEmpty(deleteMarker);

    Option<Pair<String, String>> customDeleteMarkerKeyAndValue;
    // DELETE_KEY and DELETE_MARKER both should be set.
    if (deleteKeyExists && deleteMarkerExists) {
      // DELETE_KEY field exists in the schema.
      customDeleteMarkerKeyAndValue = Option.of(Pair.of(deleteKey, deleteMarker));
    } else if (!deleteKeyExists && !deleteMarkerExists) {
      // Normal case.
      customDeleteMarkerKeyAndValue = Option.empty();
    } else {
      throw new IllegalArgumentException("Either custom delete key or marker is not specified");
    }
    return Pair.of(customDeleteMarkerKeyAndValue, hasBuiltInDeleteField(tableSchema));
  }

  /**
   * Check if "_hoodie_is_deleted" field (built-in deletes) exists in the schema.
   * Assume the type of this column is boolean.
   *
   * @param schema table schema to check
   * @return whether built-in delete field is included in the table schema
   */
  private static boolean hasBuiltInDeleteField(Schema schema) {
    return schema.getField(HOODIE_IS_DELETED_FIELD) != null;
  }
}
