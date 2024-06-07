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

import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.internal.schema.InternalSchema;

import org.apache.avro.Schema;

import java.util.Collections;
import java.util.List;

import static org.apache.hudi.avro.AvroSchemaUtils.appendFieldsToSchemaDedupNested;

/**
 * This class is responsible for handling the schema for the file group reader that supports positional merge.
 */
public class HoodiePositionBasedSchemaHandler<T> extends HoodieFileGroupReaderSchemaHandler<T> {
  public HoodiePositionBasedSchemaHandler(HoodieReaderContext<T> readerContext,
                                          Schema dataSchema,
                                          Schema requestedSchema,
                                          Option<InternalSchema> internalSchemaOpt,
                                          HoodieTableConfig hoodieTableConfig) {
    super(readerContext, dataSchema, requestedSchema, internalSchemaOpt, hoodieTableConfig);

  }

  @Override
  protected Schema prepareRequiredSchema() {
    Schema preMergeSchema = super.prepareRequiredSchema();
    return readerContext.getShouldMergeUseRecordPosition() && readerContext.getHasLogFiles()
        ? addPositionalMergeCol(preMergeSchema)
        : preMergeSchema;
  }

  @Override
  public Pair<List<Schema.Field>,List<Schema.Field>> getBootstrapRequiredFields() {
    Pair<List<Schema.Field>,List<Schema.Field>> dataAndMetaCols = super.getBootstrapRequiredFields();
    if (readerContext.supportsParquetRowIndex()) {
      if (!dataAndMetaCols.getLeft().isEmpty() && !dataAndMetaCols.getRight().isEmpty()) {
        dataAndMetaCols.getLeft().add(getPositionalMergeField());
        dataAndMetaCols.getRight().add(getPositionalMergeField());
      }
    }
    return dataAndMetaCols;
  }

  private static Schema addPositionalMergeCol(Schema input) {
    return appendFieldsToSchemaDedupNested(input, Collections.singletonList(getPositionalMergeField()));
  }

  private static Schema.Field getPositionalMergeField() {
    return new Schema.Field(HoodiePositionBasedFileGroupRecordBuffer.ROW_INDEX_TEMPORARY_COLUMN_NAME,
        Schema.create(Schema.Type.LONG), "", -1L);
  }
}
