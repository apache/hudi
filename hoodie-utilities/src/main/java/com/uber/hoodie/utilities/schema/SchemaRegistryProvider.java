/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.utilities.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.uber.hoodie.DataSourceUtils;
import com.uber.hoodie.common.util.TypedProperties;
import com.uber.hoodie.exception.HoodieIOException;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Obtains latest schema from the Confluent/Kafka schema-registry
 *
 * https://github.com/confluentinc/schema-registry
 */
public class SchemaRegistryProvider extends SchemaProvider {

  /**
   * Configs supported
   */
  public static class Config {

    private static final String SCHEMA_REGISTRY_URL_PROP = "hoodie.deltastreamer.schemaprovider.registry.url";
  }

  private final Schema schema;

  private String fetchSchemaFromRegistry(String registryUrl) throws IOException {
    URL registry = new URL(registryUrl);
    ObjectMapper mapper = new ObjectMapper();
    JsonNode node = mapper.readTree(registry.openStream());
    return node.get("schema").asText();
  }

  public SchemaRegistryProvider(TypedProperties props, JavaSparkContext jssc) {
    super(props, jssc);
    DataSourceUtils.checkRequiredProperties(props, Arrays.asList(Config.SCHEMA_REGISTRY_URL_PROP));
    String registryUrl = props.getString(Config.SCHEMA_REGISTRY_URL_PROP);
    try {
      this.schema = new Schema.Parser().parse(fetchSchemaFromRegistry(registryUrl));
    } catch (IOException ioe) {
      throw new HoodieIOException("Error reading schema from registry :" + registryUrl, ioe);
    }
  }

  @Override
  public Schema getSourceSchema() {
    return schema;
  }
}
