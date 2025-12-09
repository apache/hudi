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

import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaCache;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.read.buffer.PositionBasedFileGroupRecordBuffer;
import org.apache.hudi.common.util.InternalSchemaCache;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.collection.Triple;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.action.InternalSchemaMerger;
import org.apache.hudi.internal.schema.convert.InternalSchemaConverter;
import org.apache.hudi.storage.StoragePath;

import lombok.Getter;
import lombok.Setter;
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

import static org.apache.hudi.common.schema.HoodieSchemaCompatibility.areSchemasProjectionEquivalent;
import static org.apache.hudi.common.schema.HoodieSchemaUtils.appendFieldsToSchemaDedupNested;
import static org.apache.hudi.common.schema.HoodieSchemaUtils.createNewSchemaField;
import static org.apache.hudi.common.schema.HoodieSchemaUtils.createNewSchemaFromFieldsWithReference;
import static org.apache.hudi.common.schema.HoodieSchemaUtils.findNestedField;
import static org.apache.hudi.common.table.HoodieTableConfig.inferMergingConfigsForPreV9Table;

/**
 * This class is responsible for handling the schema for the file group reader.
 */
public class FileGroupReaderSchemaHandler<T> {

  @Getter
  protected final HoodieSchema tableSchema;

  @Getter
  // requestedSchema: the schema that the caller requests
  protected final HoodieSchema requestedSchema;

  @Getter
  // requiredSchema: the requestedSchema with any additional columns required for merging etc
  protected final HoodieSchema requiredSchema;

  /**
   * -- SETTER --
   *  This is a special case for incoming records, which do not have metadata fields in schema.
   */
  // the schema for updates, usually it equals with the requiredSchema,
  // the only exception is for incoming records, which do not include the metadata fields.
  @Setter
  @Getter
  protected HoodieSchema schemaForUpdates;

  @Getter
  protected final InternalSchema internalSchema;

  @Getter
  protected final Option<InternalSchema> internalSchemaOpt;

  protected final HoodieTableConfig hoodieTableConfig;

  protected final HoodieReaderContext<T> readerContext;

  protected final TypedProperties properties;
  @Getter
  private final DeleteContext deleteContext;
  private final HoodieTableMetaClient metaClient;

  public FileGroupReaderSchemaHandler(HoodieReaderContext<T> readerContext,
                                      HoodieSchema tableSchema,
                                      HoodieSchema requestedSchema,
                                      Option<InternalSchema> internalSchemaOpt,
                                      TypedProperties properties,
                                      HoodieTableMetaClient metaClient) {
    this.properties = properties;
    this.readerContext = readerContext;
    this.tableSchema = tableSchema;
    this.requestedSchema = HoodieSchemaCache.intern(requestedSchema);
    this.hoodieTableConfig = metaClient.getTableConfig();
    this.deleteContext = new DeleteContext(properties, tableSchema);
    this.requiredSchema = HoodieSchemaCache.intern(prepareRequiredSchema(this.deleteContext));
    this.schemaForUpdates = requiredSchema;
    this.internalSchema = pruneInternalSchema(requiredSchema, internalSchemaOpt);
    this.internalSchemaOpt = getInternalSchemaOpt(internalSchemaOpt);
    this.metaClient = metaClient;
  }

  public Option<UnaryOperator<T>> getOutputConverter() {
    if (!areSchemasProjectionEquivalent(requiredSchema, requestedSchema)) {
      return Option.of(readerContext.getRecordContext().projectRecord(requiredSchema, requestedSchema));
    }
    return Option.empty();
  }

  public Pair<HoodieSchema, Map<String, String>> getRequiredSchemaForFileAndRenamedColumns(StoragePath path) {
    if (internalSchema.isEmptySchema()) {
      return Pair.of(requiredSchema, Collections.emptyMap());
    }
    long commitInstantTime = Long.parseLong(FSUtils.getCommitTime(path.getName()));
    InternalSchema fileSchema = InternalSchemaCache.searchSchemaAndCache(commitInstantTime, metaClient);
    Pair<InternalSchema, Map<String, String>> mergedInternalSchema = new InternalSchemaMerger(fileSchema, internalSchema,
        true, false, false).mergeSchemaGetRenamed();
    HoodieSchema mergedAvroSchema = HoodieSchemaCache.intern(InternalSchemaConverter.convert(mergedInternalSchema.getLeft(), requiredSchema.getFullName()));
    return Pair.of(mergedAvroSchema, mergedInternalSchema.getRight());
  }

  private InternalSchema pruneInternalSchema(HoodieSchema requiredSchema, Option<InternalSchema> internalSchemaOption) {
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

  protected InternalSchema doPruneInternalSchema(HoodieSchema requiredSchema, InternalSchema internalSchema) {
    return InternalSchemaConverter.pruneHoodieSchemaToInternalSchema(requiredSchema, internalSchema);
  }

  @VisibleForTesting
  HoodieSchema generateRequiredSchema(DeleteContext deleteContext) {
    boolean hasInstantRange = readerContext.getInstantRange().isPresent();
    //might need to change this if other queries than mor have mandatory fields
    if (!readerContext.getHasLogFiles()) {
      if (hasInstantRange && !findNestedField(requestedSchema, HoodieRecord.COMMIT_TIME_METADATA_FIELD).isPresent()) {
        List<HoodieSchemaField> addedFields = new ArrayList<>();
        addedFields.add(getField(this.tableSchema, HoodieRecord.COMMIT_TIME_METADATA_FIELD));
        return appendFieldsToSchemaDedupNested(requestedSchema, addedFields);
      }
      return requestedSchema;
    }

    if (hoodieTableConfig.getRecordMergeMode() == RecordMergeMode.CUSTOM) {
      if (!readerContext.getRecordMerger().get().isProjectionCompatible()) {
        return this.tableSchema;
      }
    }

    List<HoodieSchemaField> addedFields = new ArrayList<>();
    for (String field : getMandatoryFieldsForMerging(
        hoodieTableConfig, this.properties, this.tableSchema, readerContext.getRecordMerger(),
        deleteContext.hasBuiltInDeleteField(), deleteContext.getCustomDeleteMarkerKeyValue(), hasInstantRange)) {
      if (!findNestedField(requestedSchema, field).isPresent()) {
        addedFields.add(getField(this.tableSchema, field));
      }
    }

    if (addedFields.isEmpty()) {
      return requestedSchema;
    }

    return appendFieldsToSchemaDedupNested(requestedSchema, addedFields);
  }

  private static String[] getMandatoryFieldsForMerging(HoodieTableConfig cfg,
                                                       TypedProperties props,
                                                       HoodieSchema tableSchema,
                                                       Option<HoodieRecordMerger> recordMerger,
                                                       boolean hasBuiltInDelete,
                                                       Option<Pair<String, String>> customDeleteMarkerKeyAndValue,
                                                       boolean hasInstantRange) {
    RecordMergeMode mergeMode = cfg.getRecordMergeMode();
    if (cfg.getTableVersion().lesserThan(HoodieTableVersion.NINE)) {
      Triple<RecordMergeMode, String, String> mergingConfigs = inferMergingConfigsForPreV9Table(
          cfg.getRecordMergeMode(),
          cfg.getPayloadClass(),
          cfg.getRecordMergeStrategyId(),
          cfg.getOrderingFieldsStr().orElse(null),
          cfg.getTableVersion());
      mergeMode = mergingConfigs.getLeft();
    }

    if (mergeMode == RecordMergeMode.CUSTOM) {
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
    if (mergeMode == RecordMergeMode.EVENT_TIME_ORDERING) {
      List<String> preCombineFields = cfg.getOrderingFields();
      requiredFields.addAll(preCombineFields);
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

  protected HoodieSchema prepareRequiredSchema(DeleteContext deleteContext) {
    HoodieSchema preReorderRequiredSchema = generateRequiredSchema(deleteContext);
    Pair<List<HoodieSchemaField>, List<HoodieSchemaField>> requiredFields = getDataAndMetaCols(preReorderRequiredSchema);
    readerContext.setNeedsBootstrapMerge(readerContext.getHasBootstrapBaseFile()
        && !requiredFields.getLeft().isEmpty() && !requiredFields.getRight().isEmpty());
    return readerContext.getNeedsBootstrapMerge()
        ? createSchemaFromFields(Stream.concat(requiredFields.getLeft().stream(), requiredFields.getRight().stream()).collect(Collectors.toList()))
        : preReorderRequiredSchema;
  }

  public Pair<List<HoodieSchemaField>, List<HoodieSchemaField>> getBootstrapRequiredFields() {
    return getDataAndMetaCols(requiredSchema);
  }

  public Pair<List<HoodieSchemaField>, List<HoodieSchemaField>> getBootstrapDataFields() {
    return getDataAndMetaCols(tableSchema);
  }

  @VisibleForTesting
  static Pair<List<HoodieSchemaField>, List<HoodieSchemaField>> getDataAndMetaCols(HoodieSchema schema) {
    Map<Boolean, List<HoodieSchemaField>> fieldsByMeta = schema.getFields().stream()
        //if there are no data fields, then we don't want to think the temp col is a data col
        .filter(f -> !Objects.equals(f.name(), PositionBasedFileGroupRecordBuffer.ROW_INDEX_TEMPORARY_COLUMN_NAME))
        .collect(Collectors.partitioningBy(f -> HoodieRecord.HOODIE_META_COLUMNS_WITH_OPERATION.contains(f.name())));
    return Pair.of(fieldsByMeta.getOrDefault(true, Collections.emptyList()),
        fieldsByMeta.getOrDefault(false, Collections.emptyList()));
  }

  public HoodieSchema createSchemaFromFields(List<HoodieSchemaField> fields) {
    //fields have positions set, so we need to remove them due to avro setFields implementation
    for (int i = 0; i < fields.size(); i++) {
      HoodieSchemaField curr = fields.get(i);
      fields.set(i, createNewSchemaField(curr));
    }
    return createNewSchemaFromFieldsWithReference(tableSchema, fields);
  }

  /**
   * Get {@link Schema.Field} from {@link Schema} by field name.
   */
  private static HoodieSchemaField getField(HoodieSchema schema, String fieldName) {
    Option<HoodieSchemaField> foundFieldOpt = findNestedField(schema, fieldName);
    if (!foundFieldOpt.isPresent()) {
      throw new IllegalArgumentException("Field: " + fieldName + " does not exist in the table schema");
    }
    return foundFieldOpt.get();
  }
}
