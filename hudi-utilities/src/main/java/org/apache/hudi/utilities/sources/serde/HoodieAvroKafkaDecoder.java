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

package org.apache.hudi.utilities.sources.serde;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDecoder;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.schema.SchemaProvider;

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import kafka.utils.VerifiableProperties;
import org.apache.avro.Schema;
import org.apache.kafka.common.errors.SerializationException;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * This is a custom implementation of kafka.serializer.Decoder<T> which aims at deserializing all the incoming messages
 * with same schema (which is latest).
 */
public class HoodieAvroKafkaDecoder extends KafkaAvroDecoder {

  private final Schema sourceSchema;
  public static final String SCHEMA_PROVIDER_CLASS_PROP = "hoodie.deltastreamer.schemaprovider.class";

  public HoodieAvroKafkaDecoder(SchemaRegistryClient client, VerifiableProperties properties) {
    super(client);
    TypedProperties typedProperties = new TypedProperties();
    copyProperties(typedProperties, properties.props());
    try {
      SchemaProvider schemaProvider = UtilHelpers.createSchemaProvider(
          typedProperties.getString(SCHEMA_PROVIDER_CLASS_PROP), typedProperties);
      this.sourceSchema = Objects.requireNonNull(schemaProvider).getSourceSchema();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public HoodieAvroKafkaDecoder(VerifiableProperties properties) {
    super(properties);
    this.configure(new KafkaAvroDeserializerConfig(properties.props()));
    TypedProperties typedProperties = new TypedProperties();
    copyProperties(typedProperties, properties.props());
    try {
      SchemaProvider schemaProvider = UtilHelpers.createSchemaProvider(
          typedProperties.getString(SCHEMA_PROVIDER_CLASS_PROP), typedProperties);
      this.sourceSchema = Objects.requireNonNull(schemaProvider).getSourceSchema();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected Object deserialize(boolean includeSchemaAndVersion, String topic, Boolean isKey, byte[] payload, Schema readerSchema) throws SerializationException {
    return super.deserialize(includeSchemaAndVersion, topic, isKey, payload, sourceSchema);
  }

  private void copyProperties(TypedProperties typedProperties, Properties properties) {
    for (Map.Entry<Object, Object> entry : properties.entrySet()) {
      typedProperties.put(entry.getKey(), entry.getValue());
    }
  }
}
