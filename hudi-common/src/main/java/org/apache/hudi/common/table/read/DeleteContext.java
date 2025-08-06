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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;

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
  private Schema readerSchema;

  public DeleteContext(Properties props, Schema tableSchema) {
    this.customDeleteMarkerKeyValue = getCustomDeleteMarkerKevValue(props);
    this.hasBuiltInDeleteField = hasBuiltInDeleteField(tableSchema);
    this.hoodieOperationPos = getHoodieOperationPos(tableSchema);
  }

  /**
   * Returns an option pair containing delete key and corresponding marker value. Delete key represents
   * the field which contains the corresponding delete-marker if a record is deleted. If no delete key and marker
   * are configured, the function returns an empty option.
   */
  private static Option<Pair<String, String>> getCustomDeleteMarkerKevValue(Properties props) {
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
  private static boolean hasBuiltInDeleteField(Schema schema) {
    return schema.getField(HOODIE_IS_DELETED_FIELD) != null;
  }

  /**
   * Returns position of hoodie operation meta field in the schema
   */
  private static Integer getHoodieOperationPos(Schema schema) {
    return Option.ofNullable(schema.getField(HoodieRecord.OPERATION_METADATA_FIELD)).map(Schema.Field::pos).orElse(-1);
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

  public DeleteContext withReaderSchema(Schema readerSchema) {
    this.readerSchema = readerSchema;
    this.hoodieOperationPos = getHoodieOperationPos(readerSchema);
    return this;
  }

  public Schema getReaderSchema() {
    return readerSchema;
  }
}
