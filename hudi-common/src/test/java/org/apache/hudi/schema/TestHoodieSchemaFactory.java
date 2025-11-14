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

package org.apache.hudi.schema;

import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test class for {@link HoodieSchemaFactory}.
 */
public class TestHoodieSchemaFactory {

  @Test
  public void testCreateRecord() {
    // Create fields
    HoodieSchemaField idField = HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.LONG));
    HoodieSchemaField nameField = HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING));
    List<HoodieSchemaField> fields = Arrays.asList(idField, nameField);

    // Create record schema
    HoodieSchema recordSchema = HoodieSchemaFactory.createRecord("User", fields);

    assertNotNull(recordSchema);
    assertEquals(HoodieSchemaType.RECORD, recordSchema.getType());
    assertEquals(Option.of("User"), recordSchema.getName());
    assertEquals(2, recordSchema.getFields().size());

    // Verify fields
    Option<HoodieSchemaField> retrievedIdField = recordSchema.getField("id");
    assertTrue(retrievedIdField.isPresent());
    assertEquals(HoodieSchemaType.LONG, retrievedIdField.get().schema().getType());

    Option<HoodieSchemaField> retrievedNameField = recordSchema.getField("name");
    assertTrue(retrievedNameField.isPresent());
    assertEquals(HoodieSchemaType.STRING, retrievedNameField.get().schema().getType());
  }

  @Test
  public void testCreateRecordWithNamespaceAndDoc() {
    HoodieSchemaField field = HoodieSchemaField.of("value", HoodieSchema.create(HoodieSchemaType.STRING));
    List<HoodieSchemaField> fields = Collections.singletonList(field);

    HoodieSchema recordSchema = HoodieSchemaFactory.createRecord("TestRecord", "com.example",
        "Test record schema", fields);

    assertNotNull(recordSchema);
    assertEquals(HoodieSchemaType.RECORD, recordSchema.getType());
    assertEquals(Option.of("TestRecord"), recordSchema.getName());
    assertEquals(Option.of("com.example"), recordSchema.getNamespace());
    assertEquals(Option.of("com.example.TestRecord"), recordSchema.getFullName());
    assertEquals(Option.of("Test record schema"), recordSchema.getDoc());
  }

  @Test
  public void testCreateRecordWithInvalidParameters() {
    HoodieSchemaField field = HoodieSchemaField.of("test", HoodieSchema.create(HoodieSchemaType.STRING));
    List<HoodieSchemaField> fields = Collections.singletonList(field);

    // Test null record name
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchemaFactory.createRecord(null, fields);
    }, "Should throw exception for null record name");

    // Test empty record name
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchemaFactory.createRecord("", fields);
    }, "Should throw exception for empty record name");

    // Test null fields
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchemaFactory.createRecord("TestRecord", null);
    }, "Should throw exception for null fields");

    // Test empty fields
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchemaFactory.createRecord("TestRecord", Collections.emptyList());
    }, "Should throw exception for empty fields list");
  }

  @Test
  public void testCreateEnum() {
    List<String> symbols = Arrays.asList("RED", "GREEN", "BLUE");
    HoodieSchema enumSchema = HoodieSchemaFactory.createEnum("Color", symbols);

    assertNotNull(enumSchema);
    assertEquals(HoodieSchemaType.ENUM, enumSchema.getType());
    assertEquals(Option.of("Color"), enumSchema.getName());
    assertEquals(symbols, enumSchema.getEnumSymbols());
  }

  @Test
  public void testCreateEnumWithInvalidParameters() {
    List<String> symbols = Arrays.asList("A", "B", "C");

    // Test null enum name
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchemaFactory.createEnum(null, symbols);
    }, "Should throw exception for null enum name");

    // Test empty enum name
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchemaFactory.createEnum("", symbols);
    }, "Should throw exception for empty enum name");

    // Test null symbols
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchemaFactory.createEnum("TestEnum", null);
    }, "Should throw exception for null symbols");

    // Test empty symbols
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchemaFactory.createEnum("TestEnum", Collections.emptyList());
    }, "Should throw exception for empty symbols list");
  }

  @Test
  public void testCreateFixed() {
    HoodieSchema fixedSchema = HoodieSchemaFactory.createFixed("MD5Hash", 16);

    assertNotNull(fixedSchema);
    assertEquals(HoodieSchemaType.FIXED, fixedSchema.getType());
    assertEquals(Option.of("MD5Hash"), fixedSchema.getName());
    assertEquals(16, fixedSchema.getFixedSize());
  }

  @Test
  public void testCreateFixedWithInvalidParameters() {
    // Test null fixed name
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchemaFactory.createFixed(null, 16);
    }, "Should throw exception for null fixed name");

    // Test empty fixed name
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchemaFactory.createFixed("", 16);
    }, "Should throw exception for empty fixed name");

    // Test invalid size
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchemaFactory.createFixed("Test", 0);
    }, "Should throw exception for zero size");

    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchemaFactory.createFixed("Test", -1);
    }, "Should throw exception for negative size");
  }

  @Test
  public void testCreateUnion() {
    HoodieSchema stringSchema = HoodieSchema.create(HoodieSchemaType.STRING);
    HoodieSchema intSchema = HoodieSchema.create(HoodieSchemaType.INT);

    List<HoodieSchema> types = Arrays.asList(stringSchema, intSchema);
    HoodieSchema unionSchema = HoodieSchemaFactory.createUnion(types);

    assertNotNull(unionSchema);
    assertEquals(HoodieSchemaType.UNION, unionSchema.getType());

    List<HoodieSchema> unionTypes = unionSchema.getTypes();
    assertEquals(2, unionTypes.size());
    assertEquals(HoodieSchemaType.STRING, unionTypes.get(0).getType());
    assertEquals(HoodieSchemaType.INT, unionTypes.get(1).getType());
  }

  @Test
  public void testCreateUnionWithVarargs() {
    HoodieSchema stringSchema = HoodieSchema.create(HoodieSchemaType.STRING);
    HoodieSchema intSchema = HoodieSchema.create(HoodieSchemaType.INT);

    HoodieSchema unionSchema = HoodieSchemaFactory.createUnion(stringSchema, intSchema);

    assertNotNull(unionSchema);
    assertEquals(HoodieSchemaType.UNION, unionSchema.getType());
    assertEquals(2, unionSchema.getTypes().size());
  }

  @Test
  public void testCreateUnionWithInvalidParameters() {
    // Test null types list
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchemaFactory.createUnion((List<HoodieSchema>) null);
    }, "Should throw exception for null types list");

    // Test empty types list
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchemaFactory.createUnion(Collections.emptyList());
    }, "Should throw exception for empty types list");

    // Test null varargs array
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchemaFactory.createUnion((HoodieSchema[]) null);
    }, "Should throw exception for null varargs array");

    // Test empty varargs array
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchemaFactory.createUnion();
    }, "Should throw exception for empty varargs array");
  }

  @Test
  public void testCreateNullableUnion() {
    HoodieSchema stringSchema = HoodieSchema.create(HoodieSchemaType.STRING);
    HoodieSchema nullableSchema = HoodieSchemaFactory.createNullableUnion(stringSchema);

    assertNotNull(nullableSchema);
    assertEquals(HoodieSchemaType.UNION, nullableSchema.getType());
    assertTrue(nullableSchema.isNullable());

    List<HoodieSchema> types = nullableSchema.getTypes();
    assertEquals(2, types.size());

    // Should contain null and string types
    boolean hasNull = types.stream().anyMatch(s -> s.getType() == HoodieSchemaType.NULL);
    boolean hasString = types.stream().anyMatch(s -> s.getType() == HoodieSchemaType.STRING);
    assertTrue(hasNull && hasString);
  }

  @Test
  public void testCreateNullableUnionWithInvalidParameters() {
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchemaFactory.createNullableUnion(null);
    }, "Should throw exception for null type");
  }
}
