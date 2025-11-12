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

package org.apache.hudi.exception;

import org.apache.hudi.avro.AvroSchemaUtils;

import org.apache.avro.Schema;

import java.util.List;

/**
 * Thrown when the schema of the incoming data is missing fields that are in the table schema.
 */
public class MissingSchemaFieldException extends SchemaCompatibilityException {

  public MissingSchemaFieldException(List<String> missingFields, Schema writerSchema, Schema tableSchema) {
    super(constructExceptionMessage(missingFields, writerSchema, tableSchema));
  }

  private static String constructExceptionMessage(List<String> missingFields, Schema writerSchema, Schema tableSchema) {
    return AvroSchemaUtils.createSchemaErrorString(
        "Schema validation failed due to missing field. Fields missing from incoming schema: {"
        + String.join(", ", missingFields) + "}", writerSchema, tableSchema);
  }
}
