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

/**
 * Convert a variety of datum into Row. Has a bunch of lazy fields to circumvent issues around
 * serializing these objects from driver to executors
 */
public class RowConverter implements Serializable {
  private static final long serialVersionUID = 1L;
  /**
   * To be lazily initialized on executors.
   */
  private transient Schema schema;

  private final String schemaStr;
  private final String invalidCharMask;
  private final boolean shouldSanitize;
  private final boolean useJava8api;

  /**
   * To be lazily initialized on executors.
   */
  private transient MercifulJsonToRowConverter jsonConverter;

  public RowConverter(Schema schema, boolean shouldSanitize, String invalidCharMask, boolean useJava8api) {
    this.schemaStr = schema.toString();
    this.schema = schema;
    this.shouldSanitize = shouldSanitize;
    this.invalidCharMask = invalidCharMask;
    this.useJava8api = useJava8api;
  }

  private void initSchema() {
    if (schema == null) {
      Schema.Parser parser = new Schema.Parser();
      schema = parser.parse(schemaStr);
    }
  }

  private void initJsonConvertor() {
    if (jsonConverter == null) {
      jsonConverter = new MercifulJsonToRowConverter(this.shouldSanitize, this.invalidCharMask, this.useJava8api);
    }
  }

  public Row fromJson(String json) {
    try {
      initSchema();
      initJsonConvertor();
      return jsonConverter.convertToRow(json, schema);
    } catch (IllegalArgumentException e) {
      throw new HoodieSchemaException("Failed to convert schema from json to avro. json string is invalid.", e);
    } catch (Exception e) {
      throw new HoodieSchemaException("Failed to convert schema from json to avro: " + json, e);
    }
  }

  /**
   * Converts a JSON string to a Row object.
   * If the conversion fails, it returns the original JSON string as an error.
   *
   * @param json The input JSON string to be converted.
   * @return Either a Row object on successful conversion (Left) or the original JSON string on error (Right).
   */
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
