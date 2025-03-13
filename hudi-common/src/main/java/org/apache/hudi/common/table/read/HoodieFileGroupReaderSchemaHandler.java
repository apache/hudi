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
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.AvroSchemaCache;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter;

import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.avro.AvroSchemaUtils.appendFieldsToSchemaDedupNested;
import static org.apache.hudi.avro.AvroSchemaUtils.createNewSchemaFromFieldsWithReference;
import static org.apache.hudi.avro.AvroSchemaUtils.findNestedField;

/**
 * This class is responsible for handling the schema for the file group reader.
 */
public class HoodieFileGroupReaderSchemaHandler<T> {

  protected final Schema dataSchema;

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

  private final AvroSchemaCache avroSchemaCache;

  public HoodieFileGroupReaderSchemaHandler(HoodieReaderContext<T> readerContext,
                                            Schema dataSchema,
                                            Schema requestedSchema,
                                            Option<InternalSchema> internalSchemaOpt,
                                            HoodieTableConfig hoodieTableConfig,
                                            TypedProperties properties) {
    this.properties = properties;
    this.readerContext = readerContext;
    this.hasBootstrapBaseFile = readerContext.getHasBootstrapBaseFile();
    this.needsMORMerge = readerContext.getHasLogFiles();
    this.recordMerger = readerContext.getRecordMerger();
    this.dataSchema = dataSchema;
    this.requestedSchema = requestedSchema;
    this.hoodieTableConfig = hoodieTableConfig;
    this.requiredSchema = prepareRequiredSchema();
    this.internalSchema = pruneInternalSchema(requiredSchema, internalSchemaOpt);
    this.internalSchemaOpt = getInternalSchemaOpt(internalSchemaOpt);
    readerContext.setNeedsBootstrapMerge(this.needsBootstrapMerge);
    this.avroSchemaCache = AvroSchemaCache.getInstance();
  }

  public Schema getDataSchema() {
    return this.dataSchema;
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
    //might need to change this if other queries than mor have mandatory fields
    if (!needsMORMerge) {
      return requestedSchema;
    }

    if (hoodieTableConfig.getRecordMergeMode() == RecordMergeMode.CUSTOM) {
      if (!recordMerger.get().isProjectionCompatible()) {
        return dataSchema;
      }
    }

    List<Schema.Field> addedFields = new ArrayList<>();
    for (String field : getMandatoryFieldsForMerging(hoodieTableConfig, properties, dataSchema, recordMerger)) {
      if (!findNestedField(requestedSchema, field).isPresent()) {
        Option<Schema.Field> foundFieldOpt  = findNestedField(dataSchema, field);
        if (!foundFieldOpt.isPresent()) {
          throw new IllegalArgumentException("Field: " + field + " does not exist in the table schema");
        }
        Schema.Field foundField = foundFieldOpt.get();
        addedFields.add(foundField);
      }
    }

    if (addedFields.isEmpty()) {
      return requestedSchema;
    }

    return appendFieldsToSchemaDedupNested(requestedSchema, addedFields);
  }

  private static String[] getMandatoryFieldsForMerging(HoodieTableConfig cfg, TypedProperties props,
                                                       Schema dataSchema, Option<HoodieRecordMerger> recordMerger) {
    if (cfg.getRecordMergeMode() == RecordMergeMode.CUSTOM) {
      return recordMerger.get().getMandatoryFieldsForMerging(dataSchema, cfg, props);
    }

    ArrayList<String> requiredFields = new ArrayList<>();

    if (cfg.populateMetaFields()) {
      requiredFields.add(HoodieRecord.RECORD_KEY_METADATA_FIELD);
    } else {
      Option<String[]> fields = cfg.getRecordKeyFields();
      if (fields.isPresent()) {
        requiredFields.addAll(Arrays.asList(fields.get()));
      }
    }

    if (cfg.getRecordMergeMode() == RecordMergeMode.EVENT_TIME_ORDERING) {
      String preCombine = cfg.getPreCombineField();
      if (!StringUtils.isNullOrEmpty(preCombine)) {
        requiredFields.add(preCombine);
      }
    }

    if (dataSchema.getField(HoodieRecord.HOODIE_IS_DELETED_FIELD) != null) {
      requiredFields.add(HoodieRecord.HOODIE_IS_DELETED_FIELD);
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
    return getDataAndMetaCols(dataSchema);
  }

  @VisibleForTesting
  static Pair<List<Schema.Field>, List<Schema.Field>> getDataAndMetaCols(Schema schema) {
    Map<Boolean, List<Schema.Field>> fieldsByMeta = schema.getFields().stream()
        //if there are no data fields, then we don't want to think the temp col is a data col
        .filter(f -> !Objects.equals(f.name(), HoodiePositionBasedFileGroupRecordBuffer.ROW_INDEX_TEMPORARY_COLUMN_NAME))
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
    return createNewSchemaFromFieldsWithReference(dataSchema, fields);
  }
}
