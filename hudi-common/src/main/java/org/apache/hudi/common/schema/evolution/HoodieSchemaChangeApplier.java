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
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.Type;
import org.apache.hudi.internal.schema.action.InternalSchemaChangeApplier;
import org.apache.hudi.internal.schema.convert.InternalSchemaConverter;

/**
 * HoodieSchema-shaped façade for column-level schema evolution operations
 * (ADD / DELETE / RENAME / UPDATE / REORDER). Mirrors the seven entry points of
 * {@link InternalSchemaChangeApplier} but exposes them on {@link HoodieSchema} so
 * callers can migrate off direct {@code InternalSchema} usage.
 *
 * <p><b>Migration status:</b> this façade currently delegates to the existing
 * InternalSchema-based applier — converting the input HoodieSchema down to an
 * InternalSchema, applying the change, and converting the result back via
 * {@link HoodieSchemaInternalSchemaBridge} so field ids and version metadata are
 * preserved on the returned HoodieSchema. Phase 5 of the InternalSchema removal
 * rewrites the implementation in pure HoodieSchema terms behind this stable
 * interface.</p>
 *
 * <p>All methods return a new {@link HoodieSchema}; the input is not mutated.</p>
 */
public class HoodieSchemaChangeApplier {

  private final HoodieSchema latestSchema;
  private final InternalSchemaChangeApplier delegate;
  private final String recordName;

  public HoodieSchemaChangeApplier(HoodieSchema latestSchema) {
    this.latestSchema = latestSchema;
    // Use the id-preserving bridge so existing field ids carried as Avro
    // custom properties survive the round trip into the legacy applier.
    InternalSchema internal = HoodieSchemaInternalSchemaBridge.toInternalSchema(latestSchema);
    this.delegate = new InternalSchemaChangeApplier(internal);
    this.recordName = latestSchema.getFullName();
  }

  /**
   * Add a column to the table. For nested fields, use a dot-separated full path in {@code colName}.
   *
   * @param colName     fully-qualified name of the column to add
   * @param colType     HoodieSchema type for the new column
   * @param doc         optional doc string
   * @param position    reference column for AFTER/BEFORE positioning (empty for FIRST/NO_OPERATION)
   * @param positionType placement strategy
   */
  public HoodieSchema applyAddChange(String colName,
                                     HoodieSchema colType,
                                     String doc,
                                     String position,
                                     ColumnPositionType positionType) {
    Type internalType = InternalSchemaConverter.convertToField(colType);
    InternalSchema result = delegate.applyAddChange(
        colName, internalType, doc, position, positionType.toLegacy());
    return HoodieSchemaInternalSchemaBridge.toHoodieSchema(result, recordName);
  }

  /**
   * Delete one or more columns. For nested fields, use dot-separated full paths.
   */
  public HoodieSchema applyDeleteChange(String... colNames) {
    InternalSchema result = delegate.applyDeleteChange(colNames);
    return HoodieSchemaInternalSchemaBridge.toHoodieSchema(result, recordName);
  }

  /**
   * Rename a column without changing its id, type, or position.
   */
  public HoodieSchema applyRenameChange(String colName, String newName) {
    InternalSchema result = delegate.applyRenameChange(colName, newName);
    return HoodieSchemaInternalSchemaBridge.toHoodieSchema(result, recordName);
  }

  /**
   * Toggle a column's nullability. Only required → optional is allowed by default;
   * the inverse must be forced explicitly through the underlying applier.
   */
  public HoodieSchema applyColumnNullabilityChange(String colName, boolean nullable) {
    InternalSchema result = delegate.applyColumnNullabilityChange(colName, nullable);
    return HoodieSchemaInternalSchemaBridge.toHoodieSchema(result, recordName);
  }

  /**
   * Promote a column to a wider type. The legal promotions are defined by
   * {@code SchemaChangeUtils.isTypeUpdateAllow}.
   */
  public HoodieSchema applyColumnTypeChange(String colName, HoodieSchema newType) {
    Type internalType = InternalSchemaConverter.convertToField(newType);
    InternalSchema result = delegate.applyColumnTypeChange(colName, internalType);
    return HoodieSchemaInternalSchemaBridge.toHoodieSchema(result, recordName);
  }

  /**
   * Update a column's documentation string.
   */
  public HoodieSchema applyColumnCommentChange(String colName, String doc) {
    InternalSchema result = delegate.applyColumnCommentChange(colName, doc);
    return HoodieSchemaInternalSchemaBridge.toHoodieSchema(result, recordName);
  }

  /**
   * Reorder a column relative to a sibling within the same enclosing struct.
   *
   * @param colName       the column to move
   * @param referColName  the reference column (ignored for FIRST)
   * @param positionType  placement strategy
   */
  public HoodieSchema applyReOrderColPositionChange(String colName,
                                                    String referColName,
                                                    ColumnPositionType positionType) {
    InternalSchema result = delegate.applyReOrderColPositionChange(
        colName, referColName, positionType.toLegacy());
    return HoodieSchemaInternalSchemaBridge.toHoodieSchema(result, recordName);
  }

  /**
   * Returns the schema this applier was constructed with.
   */
  public HoodieSchema getLatestSchema() {
    return latestSchema;
  }
}
