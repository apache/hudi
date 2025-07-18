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
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.Types;
import org.apache.hudi.internal.schema.action.TableChanges;
import org.apache.hudi.internal.schema.utils.SchemaChangeUtils;

import org.apache.avro.Schema;

import java.util.Collections;
import java.util.List;

import static org.apache.hudi.avro.AvroSchemaUtils.appendFieldsToSchemaDedupNested;
import static org.apache.hudi.common.table.read.PositionBasedFileGroupRecordBuffer.ROW_INDEX_TEMPORARY_COLUMN_NAME;

/**
 * This class is responsible for handling the schema for the file group reader that supports row index based positional merge.
 */
public class ParquetRowIndexBasedSchemaHandler<T> extends FileGroupReaderSchemaHandler<T> {
  public ParquetRowIndexBasedSchemaHandler(HoodieReaderContext<T> readerContext,
                                           Schema dataSchema,
                                           Schema requestedSchema,
                                           Option<InternalSchema> internalSchemaOpt,
                                           HoodieTableConfig hoodieTableConfig,
                                           TypedProperties properties) {
    super(readerContext, dataSchema, requestedSchema, internalSchemaOpt, hoodieTableConfig, properties);
    if (!readerContext.supportsParquetRowIndex()) {
      throw new IllegalStateException("Using " + this.getClass().getName() + " but context does not support parquet row index");
    }
  }

  @Override
  protected Schema prepareRequiredSchema() {
    Schema preMergeSchema = super.prepareRequiredSchema();
    return readerContext.getShouldMergeUseRecordPosition()
        ? addPositionalMergeCol(preMergeSchema)
        : preMergeSchema;
  }

  @Override
  protected Option<InternalSchema> getInternalSchemaOpt(Option<InternalSchema> internalSchemaOpt) {
    return internalSchemaOpt.map(ParquetRowIndexBasedSchemaHandler::addPositionalMergeCol);
  }

  @Override
  protected InternalSchema doPruneInternalSchema(Schema requiredSchema, InternalSchema internalSchema) {
    if (!readerContext.getShouldMergeUseRecordPosition()) {
      return super.doPruneInternalSchema(requiredSchema, internalSchema);
    }

    InternalSchema withRowIndex = addPositionalMergeCol(internalSchema);
    return super.doPruneInternalSchema(requiredSchema, withRowIndex);
  }

  private static InternalSchema addPositionalMergeCol(InternalSchema internalSchema) {
    TableChanges.ColumnAddChange addChange = TableChanges.ColumnAddChange.get(internalSchema);
    addChange.addColumns("", ROW_INDEX_TEMPORARY_COLUMN_NAME, Types.LongType.get(), null);
    return SchemaChangeUtils.applyTableChanges2Schema(internalSchema, addChange);
  }

  @Override
  public Pair<List<Schema.Field>,List<Schema.Field>> getBootstrapRequiredFields() {
    Pair<List<Schema.Field>,List<Schema.Field>> dataAndMetaCols = super.getBootstrapRequiredFields();
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
  static Schema addPositionalMergeCol(Schema input) {
    return appendFieldsToSchemaDedupNested(input, Collections.singletonList(getPositionalMergeField()));
  }

  @VisibleForTesting
  static Schema.Field getPositionalMergeField() {
    return new Schema.Field(ROW_INDEX_TEMPORARY_COLUMN_NAME,
        Schema.create(Schema.Type.LONG), "", -1L);
  }
}
