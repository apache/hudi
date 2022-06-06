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

package org.apache.hudi.utilities.deser;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.exception.HoodieException;

import org.apache.avro.Schema;
import org.apache.hudi.utilities.sources.AvroKafkaSource;
import org.apache.kafka.common.errors.SerializationException;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;

import java.util.Map;
import java.util.Map.Entry;

/**
 * Extending {@link KafkaAvroSchemaDeserializer} as we need to be able to inject reader schema during deserialization.
 */
public class KafkaAvroSchemaDeserializer extends KafkaAvroDeserializer {

  private Schema sourceSchema;

  public KafkaAvroSchemaDeserializer() {}

  public KafkaAvroSchemaDeserializer(SchemaRegistryClient client, Map<String, ?> props) {
    super(client, props);
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    super.configure(configs, isKey);
    try {
      TypedProperties props = getConvertToTypedProperties(configs);
      sourceSchema = new Schema.Parser().parse(props.getString(AvroKafkaSource.KAFKA_AVRO_VALUE_DESERIALIZER_SCHEMA));
    } catch (Throwable e) {
      throw new HoodieException(e);
    }
  }

  /**
   * We need to inject sourceSchema instead of reader schema during deserialization or later stages of the pipeline.
   *
   * @param topic
   * @param isKey
   * @param payload
   * @param readerSchema
   * @return
   * @throws SerializationException
   */
  @Override
  protected Object deserialize(
      String topic,
      Boolean isKey,
      byte[] payload,
      Schema readerSchema)
      throws SerializationException {
    return super.deserialize(topic, isKey, payload, sourceSchema);
  }

  protected TypedProperties getConvertToTypedProperties(Map<String, ?> configs) {
    TypedProperties typedProperties = new TypedProperties();
    for (Entry<String, ?> entry : configs.entrySet()) {
      typedProperties.put(entry.getKey(), entry.getValue());
    }
    return typedProperties;
  }
}
