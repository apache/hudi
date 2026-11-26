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

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.Option;

import org.apache.avro.Schema;

/**
 * A schema provider which applies schema post process hook on schema.
 */
public class SchemaProviderWithPostProcessor extends SchemaProvider {

  private final SchemaProvider schemaProvider;
  private final Option<SchemaPostProcessor> schemaPostProcessor;

  public SchemaProviderWithPostProcessor(SchemaProvider schemaProvider,
      Option<SchemaPostProcessor> schemaPostProcessor) {
    super(null, null);
    this.schemaProvider = schemaProvider;
    this.schemaPostProcessor = schemaPostProcessor;
  }

  @Override
  public HoodieSchema getSourceHoodieSchema() {
    return resolveSourceHoodieSchema(SchemaProviderWithPostProcessor.class, this::processedSourceSchema);
  }

  @Override
  public HoodieSchema getTargetHoodieSchema() {
    return resolveTargetHoodieSchema(SchemaProviderWithPostProcessor.class, this::processedTargetSchema);
  }

  /**
   * @deprecated since 1.2.0, use {@link #getSourceHoodieSchema()} instead.
   */
  @Override
  @Deprecated
  public Schema getSourceSchema() {
    return resolveSourceAvroSchema(SchemaProviderWithPostProcessor.class, this::processedSourceSchema);
  }

  /**
   * @deprecated since 1.2.0, use {@link #getTargetHoodieSchema()} instead.
   */
  @Override
  @Deprecated
  public Schema getTargetSchema() {
    return resolveTargetAvroSchema(SchemaProviderWithPostProcessor.class, this::processedTargetSchema);
  }

  private HoodieSchema processedSourceSchema() {
    return process(schemaProvider.getSourceHoodieSchema());
  }

  private HoodieSchema processedTargetSchema() {
    return process(schemaProvider.getTargetHoodieSchema());
  }

  private HoodieSchema process(HoodieSchema schema) {
    // A null schema (e.g. a provider that leaves the target to be inferred from the table) stays null.
    if (schema == null) {
      return null;
    }
    return schemaPostProcessor.map(processor -> processor.processSchema(schema)).orElse(schema);
  }

  public SchemaProvider getOriginalSchemaProvider() {
    return schemaProvider;
  }
}
