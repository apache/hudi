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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.Pair;

import java.util.Collections;
import java.util.List;

import static org.apache.hudi.common.schema.HoodieSchemaUtils.appendFieldsToSchemaDedupNested;
import static org.apache.hudi.common.table.read.buffer.PositionBasedFileGroupRecordBuffer.ROW_INDEX_TEMPORARY_COLUMN_NAME;

/**
 * This class is responsible for handling the schema for the file group reader that supports row index based positional merge.
 */
public class ParquetRowIndexBasedSchemaHandler<T> extends FileGroupReaderSchemaHandler<T> {
  public ParquetRowIndexBasedSchemaHandler(HoodieReaderContext<T> readerContext,
                                           HoodieSchema dataSchema,
                                           HoodieSchema requestedSchema,
                                           Option<HoodieSchema> evolutionSchemaOpt,
                                           TypedProperties properties,
                                           HoodieTableMetaClient metaClient) {
    super(readerContext, dataSchema, requestedSchema, evolutionSchemaOpt, properties, metaClient);
    if (!readerContext.getRecordContext().supportsParquetRowIndex()) {
      throw new IllegalStateException("Using " + this.getClass().getName() + " but context does not support parquet row index");
    }
  }

  @Override
  protected HoodieSchema prepareRequiredSchema(DeleteContext deleteContext) {
    HoodieSchema preMergeSchema = super.prepareRequiredSchema(deleteContext);
    return readerContext.getShouldMergeUseRecordPosition()
        ? addPositionalMergeCol(preMergeSchema)
        : preMergeSchema;
  }

  @Override
  protected Option<HoodieSchema> mapEvolutionSchemaOpt(Option<HoodieSchema> evolutionSchemaOpt) {
    return evolutionSchemaOpt.map(ParquetRowIndexBasedSchemaHandler::addPositionalMergeCol);
  }

  @Override
  protected HoodieSchema doPruneEvolutionSchema(HoodieSchema requiredSchema, HoodieSchema evolutionSchema) {
    if (!readerContext.getShouldMergeUseRecordPosition()) {
      return super.doPruneEvolutionSchema(requiredSchema, evolutionSchema);
    }

    HoodieSchema withRowIndex = addPositionalMergeCol(evolutionSchema);
    return super.doPruneEvolutionSchema(requiredSchema, withRowIndex);
  }

  @Override
  public Pair<List<HoodieSchemaField>, List<HoodieSchemaField>> getBootstrapRequiredFields() {
    Pair<List<HoodieSchemaField>, List<HoodieSchemaField>> dataAndMetaCols = super.getBootstrapRequiredFields();
    if (readerContext.getNeedsBootstrapMerge() || readerContext.getShouldMergeUseRecordPosition()) {
      if (!dataAndMetaCols.getLeft().isEmpty()) {
        dataAndMetaCols.getLeft().add(getPositionalMergeField());
      }
      if (!dataAndMetaCols.getRight().isEmpty()) {
        dataAndMetaCols.getRight().add(getPositionalMergeField());
      }
    }
    return dataAndMetaCols;
  }

  @VisibleForTesting
  static HoodieSchema addPositionalMergeCol(HoodieSchema input) {
    return appendFieldsToSchemaDedupNested(input, Collections.singletonList(getPositionalMergeField(input)));
  }

  @VisibleForTesting
  static HoodieSchemaField getPositionalMergeField() {
    return getPositionalMergeField(null);
  }

  /**
   * Builds the synthetic positional-merge field. When a parent schema is supplied
   * we stamp a field-id above its {@code maxColumnId} so the new field can't
   * collide with existing column ids — otherwise a fresh id-assignment pass
   * could alias the synthetic field onto an inner map's key/value id.
   */
  private static HoodieSchemaField getPositionalMergeField(HoodieSchema parent) {
    HoodieSchemaField field = HoodieSchemaField.of(ROW_INDEX_TEMPORARY_COLUMN_NAME,
        HoodieSchema.create(HoodieSchemaType.LONG), "", -1L);
    if (parent != null) {
      int maxId = parent.maxColumnId();
      int allIdsMax = parent.getAllIds().stream().mapToInt(Integer::intValue).max().orElse(-1);
      field.addProp(HoodieSchema.FIELD_ID_PROP, (maxId >= 0 ? maxId : allIdsMax) + 1);
    }
    return field;
  }
}
