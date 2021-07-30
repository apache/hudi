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

import org.apache.hudi.common.config.TypedProperties;

import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * SchemaProvider which uses separate Schema Providers for source and target.
 */
public class DelegatingSchemaProvider extends SchemaProvider {

  private final SchemaProvider sourceSchemaProvider;
  private final SchemaProvider targetSchemaProvider;

  public DelegatingSchemaProvider(TypedProperties props,
      JavaSparkContext jssc,
      SchemaProvider sourceSchemaProvider, SchemaProvider targetSchemaProvider) {
    super(props, jssc);
    this.sourceSchemaProvider = sourceSchemaProvider;
    this.targetSchemaProvider = targetSchemaProvider;
  }

  @Override
  public Schema getSourceSchema() {
    return sourceSchemaProvider.getSourceSchema();
  }

  @Override
  public Schema getTargetSchema() {
    return targetSchemaProvider.getTargetSchema();
  }

  public SchemaProvider getSourceSchemaProvider() {
    return sourceSchemaProvider;
  }

  public SchemaProvider getTargetSchemaProvider() {
    return targetSchemaProvider;
  }
}
