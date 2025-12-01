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
    HoodieSchema sourceSchema = schemaProvider.getSourceHoodieSchema();
    return schemaPostProcessor.map(processor -> processor.processSchema(sourceSchema))
        .orElse(sourceSchema);
  }

  @Override
  public HoodieSchema getTargetHoodieSchema() {
    HoodieSchema targetSchema = schemaProvider.getTargetHoodieSchema();
    return schemaPostProcessor.map(processor -> processor.processSchema(targetSchema))
        .orElse(targetSchema);
  }

  public SchemaProvider getOriginalSchemaProvider() {
    return schemaProvider;
  }
}
