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
import org.apache.hudi.exception.HoodieAvroSchemaException;
import org.apache.hudi.utilities.config.FilebasedSchemaProviderConfig;
import org.apache.hudi.utilities.config.HoodieSchemaProviderConfig;
import org.apache.hudi.utilities.exception.HoodieSchemaProviderException;
import org.apache.hudi.utilities.schema.converter.JsonToAvroSchemaConverter;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.apache.hudi.utilities.config.HoodieStreamerConfig.SANITIZE_SCHEMA_FIELD_NAMES;
import static org.apache.hudi.utilities.config.HoodieStreamerConfig.SCHEMA_FIELD_NAME_INVALID_CHAR_MASK;
import static org.apache.hudi.utilities.testutils.SanitizationTestUtils.generateProperFormattedSchema;
import static org.apache.hudi.utilities.testutils.SanitizationTestUtils.generateRenamedSchemaWithConfiguredReplacement;
import static org.apache.hudi.utilities.testutils.SanitizationTestUtils.generateRenamedSchemaWithDefaultReplacement;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link FilebasedSchemaProvider}.
 */
class TestFilebasedSchemaProvider extends UtilitiesTestBase {

  private FilebasedSchemaProvider schemaProvider;

  @BeforeAll
  public static void initClass() throws Exception {
    UtilitiesTestBase.initTestServices(false, false, false);
  }

  @AfterAll
  public static void cleanUpUtilitiesTestServices() {
    UtilitiesTestBase.cleanUpUtilitiesTestServices();
  }

  @Test
  void properlyFormattedNestedSchemaTest() throws IOException {
    this.schemaProvider = new FilebasedSchemaProvider(
        Helpers.setupSchemaOnDFS("streamer-config", "file_schema_provider_valid.avsc"), jsc);
    assertEquals(this.schemaProvider.getSourceHoodieSchema(), generateProperFormattedSchema());
  }

  @Test
  void renameBadlyFormattedSchemaTest() throws IOException {
    TypedProperties props = Helpers.setupSchemaOnDFS("streamer-config", "file_schema_provider_invalid.avsc");
    props.put(SANITIZE_SCHEMA_FIELD_NAMES.key(), "true");
    this.schemaProvider = new FilebasedSchemaProvider(props, jsc);
    assertEquals(this.schemaProvider.getSourceHoodieSchema(), generateRenamedSchemaWithDefaultReplacement());
  }

  @Test
  void renameBadlyFormattedSchemaWithPropertyDisabledTest() {
    assertThrows(HoodieAvroSchemaException.class, () -> {
      new FilebasedSchemaProvider(
          Helpers.setupSchemaOnDFS("streamer-config", "file_schema_provider_invalid.avsc"), jsc);
    });
  }

  @Test
  void renameBadlyFormattedSchemaWithAltCharMaskConfiguredTest() throws IOException {
    TypedProperties props = Helpers.setupSchemaOnDFS("streamer-config", "file_schema_provider_invalid.avsc");
    props.put(SANITIZE_SCHEMA_FIELD_NAMES.key(), "true");
    props.put(SCHEMA_FIELD_NAME_INVALID_CHAR_MASK.key(), "_");
    this.schemaProvider = new FilebasedSchemaProvider(props, jsc);
    assertEquals(this.schemaProvider.getSourceHoodieSchema(), generateRenamedSchemaWithConfiguredReplacement());
  }

  @Test
  void testJsonSchema() throws IOException {
    TypedProperties jsonProps = Helpers.setupSchemaOnDFS("streamer-config", "source_uber_encoded_decimal.json");
    jsonProps.setProperty(HoodieSchemaProviderConfig.SCHEMA_CONVERTER.key(), JsonToAvroSchemaConverter.class.getName());
    FilebasedSchemaProvider jsonFilebasedSchemaProvider = new FilebasedSchemaProvider(jsonProps, jsc);
    FilebasedSchemaProvider filebasedSchemaProvider = new FilebasedSchemaProvider(
        Helpers.setupSchemaOnDFS("streamer-config", "source_uber_encoded_decimal.avsc"), jsc);

    assertEquals(filebasedSchemaProvider.getSourceHoodieSchema(), jsonFilebasedSchemaProvider.getSourceHoodieSchema());
  }

  @Test
  void testJsonSchemaWithUnknownConverterClass() throws IOException {
    TypedProperties props = Helpers.setupSchemaOnDFS("streamer-config", "source_uber_encoded_decimal.json");
    props.setProperty(HoodieSchemaProviderConfig.SCHEMA_CONVERTER.key(), "org.apache.hudi.utilities.NoSuchSchemaConverter");
    Throwable t = assertThrows(HoodieSchemaProviderException.class, () -> new FilebasedSchemaProvider(props, jsc));
    assertTrue(t.getMessage().contains("Error loading json schema converter"), t.getMessage());
  }

  @Test
  void testJsonSchemaWithFailingConverter() throws IOException {
    TypedProperties props = Helpers.setupSchemaOnDFS("streamer-config", "source_uber_encoded_decimal.json");
    props.setProperty(HoodieSchemaProviderConfig.SCHEMA_CONVERTER.key(), FailingSchemaConverter.class.getName());
    Throwable t = assertThrows(HoodieSchemaProviderException.class, () -> new FilebasedSchemaProvider(props, jsc));
    assertTrue(t.getMessage().contains("Error converting json schema"), t.getMessage());
  }

  @Test
  void testMissingSchemaFile() {
    TypedProperties props = new TypedProperties();
    props.setProperty(FilebasedSchemaProviderConfig.SOURCE_SCHEMA_FILE.key(), basePath + "/no_such_schema.avsc");
    Throwable t = assertThrows(HoodieSchemaProviderException.class, () -> new FilebasedSchemaProvider(props, jsc));
    assertTrue(t.getMessage().contains("Error reading schema from file"), t.getMessage());
  }

  @Test
  void testRefreshRereadsSourceAndTargetSchemaFiles() throws IOException {
    TypedProperties targetProps = Helpers.setupSchemaOnDFS("streamer-config", "source_uber_encoded_decimal.avsc");
    TypedProperties props = Helpers.setupSchemaOnDFS("streamer-config", "file_schema_provider_valid.avsc");
    props.setProperty(FilebasedSchemaProviderConfig.TARGET_SCHEMA_FILE.key(),
        targetProps.getString(FilebasedSchemaProviderConfig.SOURCE_SCHEMA_FILE.key()));
    this.schemaProvider = new FilebasedSchemaProvider(props, jsc);

    this.schemaProvider.refresh();

    assertEquals(this.schemaProvider.getSourceHoodieSchema(), generateProperFormattedSchema());
    assertEquals(this.schemaProvider.getTargetHoodieSchema(),
        new FilebasedSchemaProvider(targetProps, jsc).getSourceHoodieSchema());
  }

  /**
   * Json schema converter whose conversion always fails, to exercise the conversion failure path.
   */
  public static class FailingSchemaConverter implements SchemaRegistryProvider.SchemaConverter {

    public FailingSchemaConverter(TypedProperties props) {
    }

    @Override
    public String convert(ParsedSchema schema) throws IOException {
      throw new IOException("simulated json schema conversion failure");
    }
  }
}