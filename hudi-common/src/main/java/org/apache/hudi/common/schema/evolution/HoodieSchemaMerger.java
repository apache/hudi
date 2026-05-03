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

package org.apache.hudi.common.schema.evolution;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.action.InternalSchemaMerger;

import java.util.Map;

/**
 * HoodieSchema-shaped façade for the read-path schema merger that combines a
 * file's stored schema with the query schema (and tracks renamed fields) to
 * produce the schema actually used to decode rows from a base file or log block.
 *
 * <p>Mirrors {@link InternalSchemaMerger}: same constructor flags
 * ({@code ignoreRequiredAttribute}, {@code useColumnTypeFromFileSchema},
 * {@code useColNameFromFileSchema}) with the same documented semantics, and the
 * two entry points {@link #mergeSchema()} and {@link #mergeSchemaGetRenamed()}.
 * Field ids are preserved end-to-end via {@link HoodieSchemaInternalSchemaBridge}.</p>
 *
 * <p>This is the read-path linchpin: {@code HoodieFileGroupReader},
 * {@code AbstractHoodieLogRecordScanner}, and the parquet readers all converge
 * on this interface. During the migration callsites swap from
 * {@code InternalSchemaMerger} to {@code HoodieSchemaMerger} one module at a time.</p>
 */
public class HoodieSchemaMerger {

  private final HoodieSchema fileSchema;
  private final HoodieSchema querySchema;
  private final InternalSchemaMerger delegate;

  public HoodieSchemaMerger(HoodieSchema fileSchema,
                            HoodieSchema querySchema,
                            boolean ignoreRequiredAttribute,
                            boolean useColumnTypeFromFileSchema,
                            boolean useColNameFromFileSchema) {
    this.fileSchema = fileSchema;
    this.querySchema = querySchema;
    InternalSchema fileInternal = HoodieSchemaInternalSchemaBridge.toInternalSchema(fileSchema);
    InternalSchema queryInternal = HoodieSchemaInternalSchemaBridge.toInternalSchema(querySchema);
    this.delegate = new InternalSchemaMerger(
        fileInternal, queryInternal,
        ignoreRequiredAttribute, useColumnTypeFromFileSchema, useColNameFromFileSchema);
  }

  public HoodieSchemaMerger(HoodieSchema fileSchema,
                            HoodieSchema querySchema,
                            boolean ignoreRequiredAttribute,
                            boolean useColumnTypeFromFileSchema) {
    this(fileSchema, querySchema, ignoreRequiredAttribute, useColumnTypeFromFileSchema, true);
  }

  /**
   * Produces the merged read schema. Field ids carry through from the query schema;
   * column names and types follow the {@code useCol*FromFileSchema} flags set at
   * construction time.
   */
  public HoodieSchema mergeSchema() {
    InternalSchema merged = delegate.mergeSchema();
    return HoodieSchemaInternalSchemaBridge.toHoodieSchema(merged, querySchema.getFullName());
  }

  /**
   * Same as {@link #mergeSchema()} but additionally returns the rename map
   * (query-side full name → file-side leaf name) so downstream record rewriters
   * can project correctly across renames.
   */
  public Pair<HoodieSchema, Map<String, String>> mergeSchemaGetRenamed() {
    Pair<InternalSchema, Map<String, String>> result = delegate.mergeSchemaGetRenamed();
    HoodieSchema mergedSchema = HoodieSchemaInternalSchemaBridge.toHoodieSchema(
        result.getLeft(), querySchema.getFullName());
    return Pair.of(mergedSchema, result.getRight());
  }

  public HoodieSchema getFileSchema() {
    return fileSchema;
  }

  public HoodieSchema getQuerySchema() {
    return querySchema;
  }
}
