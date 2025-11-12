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

package org.apache.hudi.utilities.schema.converter;

import org.apache.hudi.common.config.ConfigProperty;

/**
 * Configs for `JsonToAvroSchemaConverter`
 */
public class JsonToAvroSchemaConverterConfig {
  public static final ConfigProperty<Boolean> CONVERT_DEFAULT_VALUE_TYPE = ConfigProperty
      .key("hoodie.streamer.schemaconverter.json_to_avro.convert_default_value_type")
      .defaultValue(true)
      .sinceVersion("1.1.0")
      .withDocumentation("In `org.apache.hudi.utilities.schema.converter.JsonToAvroSchemaConverter`, "
          + "whether to automatically convert the default value based on the field type in the "
          + "schema. For example, the field schema is "
          + "`\"col1\":{\"type\":\"integer\",\"default\":\"0\"`, and in this case, the default "
          + "value is a String in JSON. After converting the type of the default value from JSON, "
          + "the converter can properly insert the default value to the Avro schema without "
          + "hitting Avro schema validation error. This should be turned on by default.");

  public static final ConfigProperty<Boolean> STRIP_DEFAULT_VALUE_QUOTES = ConfigProperty
      .key("hoodie.streamer.schemaconverter.json_to_avro.strip_default_value_quotes")
      .defaultValue(true)
      .sinceVersion("1.1.0")
      .withDocumentation("In `org.apache.hudi.utilities.schema.converter.JsonToAvroSchemaConverter`, "
          + "whether to automatically strip the surrounding quotes on the default value of the "
          + "String-typed field in the schema. For example, the field schema is "
          + "`\"col1\":{\"type\":\"string\",\"default\":\"\\\"abc\\\"\"`, and in this case, the "
          + "default string value is surrounded by additional quotes (`\"`) in JSON. The converter "
          + "can strip the quotes, if exist, as they are redundant before adding the default value "
          + "to the Avro schema. This should be turned on by default.");
}
