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

package org.apache.hudi.exception;

import org.apache.hudi.avro.AvroSchemaCompatibility;
import org.apache.hudi.avro.AvroSchemaUtils;

import org.apache.avro.Schema;

import java.util.stream.Collectors;

/**
 * Thrown when there is a backwards compatibility issue with the incoming schema.
 * i.e. when the incoming schema cannot be used to read older data files
 */
public class SchemaBackwardsCompatibilityException extends SchemaCompatibilityException {

  public SchemaBackwardsCompatibilityException(AvroSchemaCompatibility.SchemaPairCompatibility compatibility, Schema writerSchema, Schema tableSchema) {
    super(constructExceptionMessage(compatibility, writerSchema, tableSchema));
  }

  private static String constructExceptionMessage(AvroSchemaCompatibility.SchemaPairCompatibility compatibility, Schema writerSchema, Schema tableSchema) {
    return AvroSchemaUtils.createSchemaErrorString("Schema validation backwards compatibility check failed with the following issues: {"
        + compatibility.getResult().getIncompatibilities().stream()
            .map(incompatibility -> incompatibility.getType().name() + ": " + incompatibility.getMessage())
            .collect(Collectors.joining(", ")) + "}", writerSchema, tableSchema);
  }
}
