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
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaParseException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for {@link FilebasedSchemaProvider}.
 */
public class TestFilebasedSchemaProvider extends UtilitiesTestBase {

  private FilebasedSchemaProvider schemaProvider;

  @BeforeAll
  public static void initClass() throws Exception {
    UtilitiesTestBase.initTestServices(false, false);
  }

  @AfterAll
  public static void cleanupClass() {
    UtilitiesTestBase.cleanupClass();
  }

  @BeforeEach
  public void setup() throws Exception {
    super.setup();
  }

  @AfterEach
  public void teardown() throws Exception {
    super.teardown();
  }

  private Schema generateProperFormattedSchema() {
    Schema addressSchema = SchemaBuilder.record("Address").fields()
        .requiredString("streetaddress")
        .requiredString("city")
        .endRecord();
    Schema personSchema = SchemaBuilder.record("Person").fields()
        .requiredString("firstname")
        .requiredString("lastname")
        .name("address").type(addressSchema).noDefault()
        .endRecord();
    return personSchema;
  }

  // replacement mask is "__".
  private Schema generateRenamedSchemaWithDefaultReplacement() {
    Schema addressSchema = SchemaBuilder.record("__Address").fields()
        .nullableString("__stree9add__ress", "@@@any_address")
        .requiredString("cit__y__")
        .endRecord();
    Schema personSchema = SchemaBuilder.record("Person").fields()
        .requiredString("__firstname")
        .requiredString("__lastname")
        .name("address").type(addressSchema).noDefault()
        .endRecord();
    return personSchema;
  }

  // replacement mask is "_".
  private Schema generateRenamedSchemaWithConfiguredReplacement() {
    Schema addressSchema = SchemaBuilder.record("_Address").fields()
        .nullableString("_stree9add_ress", "@@@any_address")
        .requiredString("cit_y_")
        .endRecord();
    Schema personSchema = SchemaBuilder.record("Person").fields()
        .requiredString("_firstname")
        .requiredString("_lastname")
        .name("address").type(addressSchema).noDefault()
        .endRecord();
    return personSchema;
  }

  @Test
  public void properlyFormattedNestedSchemaTest() throws IOException {
    this.schemaProvider = new FilebasedSchemaProvider(
        Helpers.setupSchemaOnDFS("delta-streamer-config", "file_schema_provider_valid.avsc"), jsc);
    assertEquals(this.schemaProvider.getSourceSchema(), generateProperFormattedSchema());
  }

  @Test
  public void renameBadlyFormattedSchemaTest() throws IOException {
    TypedProperties props = Helpers.setupSchemaOnDFS("delta-streamer-config", "file_schema_provider_invalid.avsc");
    props.put("hoodie.deltastreamer.source.sanitize.invalid.column.names", "true");
    this.schemaProvider = new FilebasedSchemaProvider(props, jsc);
    assertEquals(this.schemaProvider.getSourceSchema(), generateRenamedSchemaWithDefaultReplacement());
  }

  @Test
  public void renameBadlyFormattedSchemaWithProperyDisabledTest() {
    assertThrows(SchemaParseException.class, () -> {
      new FilebasedSchemaProvider(
          Helpers.setupSchemaOnDFS("delta-streamer-config", "file_schema_provider_invalid.avsc"), jsc);
    });
  }

  @Test
  public void renameBadlyFormattedSchemaWithInvalidCharMaskConfiguredTest() throws IOException {
    TypedProperties props = Helpers.setupSchemaOnDFS("delta-streamer-config", "file_schema_provider_invalid.avsc");
    props.put("hoodie.deltastreamer.source.sanitize.invalid.column.names", "true");
    props.put("hoodie.deltastreamer.source.sanitize.invalid.char.mask", "_");
    this.schemaProvider = new FilebasedSchemaProvider(props, jsc);
    assertEquals(this.schemaProvider.getSourceSchema(), generateRenamedSchemaWithConfiguredReplacement());
  }
}