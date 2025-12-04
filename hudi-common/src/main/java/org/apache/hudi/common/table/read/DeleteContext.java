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

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;

import java.io.Serializable;
import java.util.Properties;

import static org.apache.hudi.common.model.DefaultHoodieRecordPayload.DELETE_KEY;
import static org.apache.hudi.common.model.DefaultHoodieRecordPayload.DELETE_MARKER;
import static org.apache.hudi.common.model.HoodieRecord.HOODIE_IS_DELETED_FIELD;

/**
 * Schema context for deletes.
 */
public class DeleteContext implements Serializable {
  private static final long serialVersionUID = 1L;

  private final Option<Pair<String, String>> customDeleteMarkerKeyValue;
  private final boolean hasBuiltInDeleteField;
  private int hoodieOperationPos;
  private HoodieSchema readerSchema;

  public DeleteContext(Properties props, HoodieSchema tableSchema) {
    this.customDeleteMarkerKeyValue = getCustomDeleteMarkerKeyValue(props);
    this.hasBuiltInDeleteField = hasBuiltInDeleteField(tableSchema);
  }

  /**
   * Creates a DeleteContext for the writer path where it is assumed the HoodieOperation field is not set in the incoming record.
   * @param properties the properties defining how deletes are handled for the table
   * @param recordSchema the schema of the incoming record
   * @return the delete context
   */
  public static DeleteContext fromRecordSchema(Properties properties, HoodieSchema recordSchema) {
    DeleteContext deleteContext = new DeleteContext(properties, recordSchema);
    deleteContext.hoodieOperationPos = -1;
    deleteContext.readerSchema = recordSchema;
    return deleteContext;
  }

  /**
   * Returns an option pair containing delete key and corresponding marker value. Delete key represents
   * the field which contains the corresponding delete-marker if a record is deleted. If no delete key and marker
   * are configured, the function returns an empty option.
   */
  private static Option<Pair<String, String>> getCustomDeleteMarkerKeyValue(Properties props) {
    String deleteKey = props.getProperty(DELETE_KEY);
    String deleteMarker = props.getProperty(DELETE_MARKER);
    boolean deleteKeyExists = !StringUtils.isNullOrEmpty(deleteKey);
    boolean deleteMarkerExists = !StringUtils.isNullOrEmpty(deleteMarker);

    Option<Pair<String, String>> customDeleteMarkerKeyValue;
    // DELETE_KEY and DELETE_MARKER both should be set.
    if (deleteKeyExists && deleteMarkerExists) {
      // DELETE_KEY field exists in the schema.
      customDeleteMarkerKeyValue = Option.of(Pair.of(deleteKey, deleteMarker));
    } else if (!deleteKeyExists && !deleteMarkerExists) {
      // Normal case.
      customDeleteMarkerKeyValue = Option.empty();
    } else {
      throw new IllegalArgumentException("Either custom delete key or marker is not specified");
    }
    return customDeleteMarkerKeyValue;
  }

  /**
   * Check if "_hoodie_is_deleted" field (built-in deletes) exists in the schema.
   * Assume the type of this column is boolean.
   *
   * @param schema table schema to check
   * @return whether built-in delete field is included in the table schema
   */
  private static boolean hasBuiltInDeleteField(HoodieSchema schema) {
    return schema.getType() != HoodieSchemaType.NULL && schema.getField(HOODIE_IS_DELETED_FIELD).isPresent();
  }

  /**
   * Returns position of hoodie operation meta field in the schema
   */
  private static int getHoodieOperationPos(HoodieSchema schema) {
    return Option.ofNullable(schema.getField(HoodieRecord.OPERATION_METADATA_FIELD))
        // Safely flattens nested options; if inner is empty, the whole chain becomes empty
        .flatMap(field -> field)
        .map(HoodieSchemaField::pos)
        .orElseGet(() -> -1);
  }

  public Option<Pair<String, String>> getCustomDeleteMarkerKeyValue() {
    return customDeleteMarkerKeyValue;
  }

  public boolean hasBuiltInDeleteField() {
    return hasBuiltInDeleteField;
  }

  public int getHoodieOperationPos() {
    return hoodieOperationPos;
  }

  public DeleteContext withReaderSchema(HoodieSchema readerSchema) {
    this.readerSchema = readerSchema;
    this.hoodieOperationPos = getHoodieOperationPos(readerSchema);
    return this;
  }

  public HoodieSchema getReaderSchema() {
    return readerSchema;
  }
}
