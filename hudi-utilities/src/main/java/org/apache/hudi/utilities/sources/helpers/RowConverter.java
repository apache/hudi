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

package org.apache.hudi.utilities.sources.helpers;

import org.apache.hudi.internal.schema.HoodieSchemaException;

import org.apache.avro.Schema;
import org.apache.spark.sql.Row;

import java.io.Serializable;

import scala.util.Either;
import scala.util.Left;
import scala.util.Right;

import static org.apache.hudi.utilities.config.HoodieStreamerConfig.SANITIZE_SCHEMA_FIELD_NAMES;
import static org.apache.hudi.utilities.config.HoodieStreamerConfig.SCHEMA_FIELD_NAME_INVALID_CHAR_MASK;

public class RowConverter implements Serializable {
  private static final long serialVersionUID = 1L;
  /**
   * To be lazily initialized on executors.
   */
  private transient Schema schema;

  private final String schemaStr;
  private final String invalidCharMask;
  private final boolean shouldSanitize;

  /**
   * To be lazily initialized on executors.
   */
  private transient MercifulJsonToRowConverter jsonConverter;

  public RowConverter(String schemaStr) {
    this(schemaStr, SANITIZE_SCHEMA_FIELD_NAMES.defaultValue(), SCHEMA_FIELD_NAME_INVALID_CHAR_MASK.defaultValue());
  }

  public RowConverter(String schemaStr, boolean shouldSanitize, String invalidCharMask) {
    this.schemaStr = schemaStr;
    this.shouldSanitize = shouldSanitize;
    this.invalidCharMask = invalidCharMask;
  }

  public RowConverter(Schema schema) {
    this(schema, SANITIZE_SCHEMA_FIELD_NAMES.defaultValue(), SCHEMA_FIELD_NAME_INVALID_CHAR_MASK.defaultValue());
  }

  public RowConverter(Schema schema, boolean shouldSanitize, String invalidCharMask) {
    this.schemaStr = schema.toString();
    this.schema = schema;
    this.shouldSanitize = shouldSanitize;
    this.invalidCharMask = invalidCharMask;
  }

  private void initSchema() {
    if (schema == null) {
      Schema.Parser parser = new Schema.Parser();
      schema = parser.parse(schemaStr);
    }
  }

  private void initJsonConvertor() {
    if (jsonConverter == null) {
      jsonConverter = new MercifulJsonToRowConverter(this.shouldSanitize, this.invalidCharMask);
    }
  }

  public Row fromJson(String json) {
    try {
      initSchema();
      initJsonConvertor();
      return jsonConverter.convertToRow(json, schema);
    } catch (Exception e) {
      if (json != null) {
        throw new HoodieSchemaException("Failed to convert schema from json to avro: " + json, e);
      } else {
        throw new HoodieSchemaException("Failed to convert schema from json to avro. Schema string was null.", e);
      }
    }
  }

  public Either<Row, String> fromJsonToRowWithError(String json) {
    Row row;
    try {
      row = fromJson(json);
    } catch (Exception e) {
      return new Right<>(json);
    }
    return new Left<>(row);
  }

  public Schema getSchema() {
    try {
      return new Schema.Parser().parse(schemaStr);
    } catch (Exception e) {
      throw new HoodieSchemaException("Failed to parse json schema: " + schemaStr, e);
    }
  }
}
