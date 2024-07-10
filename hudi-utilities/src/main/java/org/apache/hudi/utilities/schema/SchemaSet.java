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

package org.apache.hudi.utilities.schema;

import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * Tracks already processed schemas.
 */
public class SchemaSet implements Serializable {

  private final Set<Long> processedSchema = new HashSet<>();

  public boolean isSchemaPresent(Schema schema) {
    long schemaKey = SchemaNormalization.parsingFingerprint64(schema);
    return processedSchema.contains(schemaKey);
  }

  public void addSchema(Schema schema) {
    long schemaKey = SchemaNormalization.parsingFingerprint64(schema);
    processedSchema.add(schemaKey);
  }
}
