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

package org.apache.hudi.utilities.sources.pulsar;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.schema.SchemaProvider;

import org.apache.avro.Schema;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroSchema;
import org.apache.spark.api.java.JavaSparkContext;

public class PulsarSchemaProvider extends SchemaProvider {

  private SerializableSchemaInfo schemaInfo;

  public PulsarSchemaProvider(TypedProperties props, JavaSparkContext jssc, SerializableSchemaInfo schemaInfo) {
    super(props, jssc);
    this.schemaInfo = schemaInfo;
  }

  @Override
  public Schema getSourceSchema() {
    org.apache.pulsar.shade.org.apache.avro.Schema shadedSchema = GenericAvroSchema.of(schemaInfo.get()).getAvroSchema();
    return new Schema.Parser().parse(shadedSchema.toString());
  }
}
