/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.utilities.schema.converter;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.schema.SchemaRegistryProvider;
import org.apache.hudi.utilities.sources.helpers.ProtoConversionUtil;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;

import java.io.IOException;

/**
 * Converts a protobuf schema from the schema registry to an Avro schema.
 */
public class ProtoSchemaToAvroSchemaConverter implements SchemaRegistryProvider.SchemaConverter {
  private final ProtoConversionUtil.SchemaConfig schemaConfig;

  public ProtoSchemaToAvroSchemaConverter(TypedProperties config) {
    this.schemaConfig = ProtoConversionUtil.SchemaConfig.fromProperties(config);
  }

  public String convert(ParsedSchema schema) throws IOException {
    ProtobufSchema protobufSchema = (ProtobufSchema) schema;
    return ProtoConversionUtil.getAvroSchemaForMessageDescriptor(protobufSchema.toDescriptor(), schemaConfig).toString();
  }
}
