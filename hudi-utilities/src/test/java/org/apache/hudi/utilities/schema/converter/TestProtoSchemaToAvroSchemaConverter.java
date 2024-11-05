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
import org.apache.hudi.utilities.config.ProtoClassBasedSchemaProviderConfig;
import org.apache.hudi.utilities.test.proto.Parent;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestProtoSchemaToAvroSchemaConverter {
  @Test
  void testConvert() throws Exception {
    TypedProperties properties = new TypedProperties();
    properties.setProperty(ProtoClassBasedSchemaProviderConfig.PROTO_SCHEMA_CLASS_NAME.key(), Parent.class.getName());
    Schema.Parser parser = new Schema.Parser();
    String actual = new ProtoSchemaToAvroSchemaConverter(properties).convert(getProtoSchemaString());
    Schema actualSchema = new Schema.Parser().parse(actual);

    Schema expectedSchema = parser.parse(getClass().getClassLoader().getResourceAsStream("schema-provider/proto/parent_schema_recursive_default_limit.avsc"));
    assertEquals(expectedSchema, actualSchema);
  }

  private String getProtoSchemaString() throws IOException, URISyntaxException {
    return new String(Files.readAllBytes(Paths.get(getClass().getClassLoader().getResource("schema-provider/proto/recursive.proto").toURI())));
  }
}
