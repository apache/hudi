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

package org.apache.hudi.utilities.sources.helpers;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.deser.KafkaAvroSchemaDeserializer;
import org.apache.hudi.utilities.schema.SchemaProvider;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * KafkaAvroSchemaDeserializer for tests using {@link MockSchemaRegistryClient}. Since in tests we can't use schema registry,
 * have to override calls especially super.configure(). So, had to introduce these test classes. Impl of configure() should
 * be in line with {@link KafkaAvroSchemaDeserializer} except for super.configure().
 */
public class KafkaAvroTestCustomDeserializer extends KafkaAvroSchemaDeserializer {

  public static Schema schemaToReturn = HoodieTestDataGenerator.AVRO_SCHEMA;

  public KafkaAvroTestCustomDeserializer() {
    this.schemaRegistry = new MockSchemaRegistryClient() {
      @Override
      public synchronized Schema getByID(int id) throws IOException, RestClientException {
        return schemaToReturn;
      }
    };
  }

  public KafkaAvroTestCustomDeserializer(SchemaRegistryClient client) {
    this.schemaRegistry = new MockSchemaRegistryClient() {
      @Override
      public synchronized Schema getByID(int id) throws IOException, RestClientException {
        return schemaToReturn;
      }
    };
  }

  public KafkaAvroTestCustomDeserializer(SchemaRegistryClient client, Map<String, ?> props) {
    this.schemaRegistry = client;
    this.configure(this.deserializerConfig(props));
  }

  public void configure(Map<String, ?> configs, boolean isKey) {
    try {
      TypedProperties props = getConvertToTypedProperties(configs);
      String className = props.getString("hoodie.deltastreamer.schemaprovider.class");
      SchemaProvider schemaProvider = (SchemaProvider) ReflectionUtils.loadClass(className, props);
      sourceSchema = Objects.requireNonNull(schemaProvider).getSourceSchema();
    } catch (Throwable e) {
      throw new HoodieException(e);
    }
  }
}
