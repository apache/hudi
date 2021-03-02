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

import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.exception.HoodieIOException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.hudi.utilities.sources.helpers.AvroKafkaSourceHelpers;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;

/**
 * Obtains latest schema from the Confluent/Kafka schema-registry.
 *
 * https://github.com/confluentinc/schema-registry
 */
public class SchemaRegistryProvider extends SchemaProvider {

  /**
   * Configs supported.
   */
  public static class Config {
    private static final String SRC_SCHEMA_REGISTRY_URL_PROP = "hoodie.deltastreamer.schemaprovider.registry.url";
    private static final String TARGET_SCHEMA_REGISTRY_URL_PROP = "hoodie.deltastreamer.schemaprovider.registry.targetUrl";
    private static final String CACHE_SCHEMAS = "hoodie.deltastreamer.schemaprovider.registry.cache_enabled";
  }

  private Schema sourceSchema;
  private Schema targetSchema;
  private final boolean cacheDisabled;
  private final boolean injectKafkaFieldSchema;
  private final String registryUrl;
  private final String targetRegistryUrl;
  private final boolean noTargetSchema;

  public SchemaRegistryProvider(TypedProperties props) {
    this(props, null);
  }

  public SchemaRegistryProvider(TypedProperties props, JavaSparkContext jssc) {
    super(props, jssc);
    DataSourceUtils.checkRequiredProperties(props, Collections.singletonList(Config.SRC_SCHEMA_REGISTRY_URL_PROP));
    this.cacheDisabled = !props.getBoolean(Config.CACHE_SCHEMAS, false);
    this.injectKafkaFieldSchema = props.getBoolean(AvroKafkaSourceHelpers.INJECT_KAFKA_META_FIELDS, false);
    this.registryUrl = config.getString(Config.SRC_SCHEMA_REGISTRY_URL_PROP);
    this.targetRegistryUrl = config.getString(Config.TARGET_SCHEMA_REGISTRY_URL_PROP, registryUrl);
    this.noTargetSchema = targetRegistryUrl.equals("null");
  }

  public String fetchSchemaFromRegistry(String registryUrl) throws IOException {
    URL registry = new URL(registryUrl);
    ObjectMapper mapper = new ObjectMapper();
    JsonNode node = mapper.readTree(registry.openStream());
    return node.get("schema").asText();
  }

  private Schema getSchema(String registryUrl) throws IOException {
    Schema schema = new Schema.Parser().parse(fetchSchemaFromRegistry(registryUrl));
    if (injectKafkaFieldSchema) {
      schema = AvroKafkaSourceHelpers.addKafkaMetadataFields(schema);
    }
    return schema;
  }

  @Override
  public Schema getSourceSchema() {
    if (cacheDisabled) {
      return getSourceSchemaFromRegistry();
    }
    if (sourceSchema == null) {
      synchronized (this) {
        if (sourceSchema == null) {
          sourceSchema = getSourceSchemaFromRegistry();
        }
      }
    }
    return sourceSchema;
  }

  @Override
  public Schema getTargetSchema() {
    if (noTargetSchema) {
      return null;
    }
    if (cacheDisabled) {
      return getTargetSchemaFromRegistry();
    }
    if (targetSchema == null) {
      synchronized (this) {
        if (targetSchema == null) {
          targetSchema = getTargetSchemaFromRegistry();
        }
      }
    }
    return targetSchema;
  }

  private Schema getSourceSchemaFromRegistry() {
    try {
      return getSchema(registryUrl);
    } catch (IOException ioe) {
      throw new HoodieIOException("Error reading source schema from registry :" + registryUrl, ioe);
    }
  }

  private Schema getTargetSchemaFromRegistry() {
    try {
      return getSchema(targetRegistryUrl);
    } catch (IOException ioe) {
      throw new HoodieIOException("Error reading target schema from registry :" + targetRegistryUrl, ioe);
    }
  }
}
