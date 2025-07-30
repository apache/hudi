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

package org.apache.hudi.table;

import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieIndexMetadata;
import org.apache.hudi.exception.SchemaCompatibilityException;
import org.apache.hudi.metadata.MetadataPartitionType;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link HoodieTable#validateSecondaryIndexSchemaEvolution}.
 */
public class TestHoodieTableSchemaEvolution {

  private static final String TABLE_SCHEMA = "{"
      + "\"type\": \"record\","
      + "\"name\": \"test\","
      + "\"fields\": ["
      + "  {\"name\": \"id\", \"type\": \"int\"},"
      + "  {\"name\": \"name\", \"type\": \"string\"},"
      + "  {\"name\": \"age\", \"type\": \"int\"},"
      + "  {\"name\": \"salary\", \"type\": \"double\"},"
      + "  {\"name\": \"nullable_field\", \"type\": [\"null\", \"string\"], \"default\": null}"
      + "]}";

  @Test
  public void testNoSecondaryIndexes() {
    // When there are no secondary indexes, any schema evolution should be allowed
    Schema tableSchema = new Schema.Parser().parse(TABLE_SCHEMA);
    Schema writerSchema = new Schema.Parser().parse(TABLE_SCHEMA.replace("\"age\", \"type\": \"int\"", "\"age\", \"type\": \"long\""));
    
    HoodieIndexMetadata indexMetadata = new HoodieIndexMetadata(new HashMap<>());
    
    assertDoesNotThrow(() -> 
        HoodieTable.validateSecondaryIndexSchemaEvolution(tableSchema, writerSchema, indexMetadata));
  }

  @Test
  public void testNoSchemaChange() {
    // When schema doesn't change, validation should pass even with secondary indexes
    Schema tableSchema = new Schema.Parser().parse(TABLE_SCHEMA);
    Schema writerSchema = new Schema.Parser().parse(TABLE_SCHEMA);
    
    HoodieIndexDefinition indexDef = createSecondaryIndexDefinition("secondary_index_age", "age");
    HoodieIndexMetadata indexMetadata = createIndexMetadata(indexDef);
    
    assertDoesNotThrow(() -> 
        HoodieTable.validateSecondaryIndexSchemaEvolution(tableSchema, writerSchema, indexMetadata));
  }

  @Test
  public void testNonIndexedColumnEvolution() {
    // Evolution of non-indexed columns should be allowed
    Schema tableSchema = new Schema.Parser().parse(TABLE_SCHEMA);
    Schema writerSchema = new Schema.Parser().parse(TABLE_SCHEMA.replace("\"name\", \"type\": \"string\"", "\"name\", \"type\": [\"null\", \"string\"]"));
    
    HoodieIndexDefinition indexDef = createSecondaryIndexDefinition("secondary_index_age", "age");
    HoodieIndexMetadata indexMetadata = createIndexMetadata(indexDef);
    
    assertDoesNotThrow(() -> 
        HoodieTable.validateSecondaryIndexSchemaEvolution(tableSchema, writerSchema, indexMetadata));
  }

  @ParameterizedTest
  @MethodSource("provideInvalidSchemaEvolutions")
  public void testIndexedColumnTypeEvolution(String fieldName, String originalType, String evolvedType) {
    // Type evolution of indexed columns should fail
    String originalSchema = TABLE_SCHEMA;
    String evolvedSchema = TABLE_SCHEMA.replace("\"" + fieldName + "\", \"type\": \"" + originalType + "\"", 
                                                 "\"" + fieldName + "\", \"type\": \"" + evolvedType + "\"");
    
    Schema tableSchema = new Schema.Parser().parse(originalSchema);
    Schema writerSchema = new Schema.Parser().parse(evolvedSchema);
    
    HoodieIndexDefinition indexDef = createSecondaryIndexDefinition("secondary_index_" + fieldName, fieldName);
    HoodieIndexMetadata indexMetadata = createIndexMetadata(indexDef);
    
    SchemaCompatibilityException exception = assertThrows(SchemaCompatibilityException.class, () -> 
        HoodieTable.validateSecondaryIndexSchemaEvolution(tableSchema, writerSchema, indexMetadata));
    
    assertTrue(exception.getMessage().contains("secondary index"));
    assertTrue(exception.getMessage().contains(fieldName));
    assertTrue(exception.getMessage().contains("secondary_index_" + fieldName));
  }

  private static Stream<Arguments> provideInvalidSchemaEvolutions() {
    return Stream.of(
        Arguments.of("age", "int", "long"),       // int to long
        Arguments.of("age", "int", "double"),     // int to double
        Arguments.of("salary", "double", "float") // double to float
    );
  }

  @Test
  public void testMultipleIndexesOnSameColumn() {
    // When a column has multiple indexes, error should mention at least one
    Schema tableSchema = new Schema.Parser().parse(TABLE_SCHEMA);
    Schema writerSchema = new Schema.Parser().parse(TABLE_SCHEMA.replace("\"age\", \"type\": \"int\"", "\"age\", \"type\": \"long\""));
    
    HoodieIndexDefinition indexDef1 = createSecondaryIndexDefinition("secondary_index_age_1", "age");
    HoodieIndexDefinition indexDef2 = createSecondaryIndexDefinition("secondary_index_age_2", "age");
    
    Map<String, HoodieIndexDefinition> indexDefs = new HashMap<>();
    indexDefs.put("secondary_index_age_1", indexDef1);
    indexDefs.put("secondary_index_age_2", indexDef2);
    HoodieIndexMetadata indexMetadata = new HoodieIndexMetadata(indexDefs);
    
    SchemaCompatibilityException exception = assertThrows(SchemaCompatibilityException.class, () -> 
        HoodieTable.validateSecondaryIndexSchemaEvolution(tableSchema, writerSchema, indexMetadata));
    
    assertTrue(exception.getMessage().contains("age"));
    // Should contain at least one of the index names
    assertTrue(exception.getMessage().contains("secondary_index_age_1") || exception.getMessage().contains("secondary_index_age_2"));
  }

  @Test
  public void testCompoundIndex() {
    // Test index on multiple columns - if any column evolves, should fail
    Schema tableSchema = new Schema.Parser().parse(TABLE_SCHEMA);
    Schema writerSchema = new Schema.Parser().parse(TABLE_SCHEMA.replace("\"age\", \"type\": \"int\"", "\"age\", \"type\": \"long\""));
    
    HoodieIndexDefinition indexDef = createSecondaryIndexDefinition("secondary_index_compound", "name", "age");
    HoodieIndexMetadata indexMetadata = createIndexMetadata(indexDef);
    
    SchemaCompatibilityException exception = assertThrows(SchemaCompatibilityException.class, () -> 
        HoodieTable.validateSecondaryIndexSchemaEvolution(tableSchema, writerSchema, indexMetadata));
    
    assertTrue(exception.getMessage().contains("age"));
    assertTrue(exception.getMessage().contains("secondary_index_compound"));
  }

  @Test
  public void testFieldWithAlias() {
    // Test schema evolution with field aliases
    String tableSchemaStr = "{"
        + "\"type\": \"record\","
        + "\"name\": \"test\","
        + "\"fields\": ["
        + "  {\"name\": \"id\", \"type\": \"int\"},"
        + "  {\"name\": \"old_name\", \"type\": \"string\", \"aliases\": [\"new_name\"]}"
        + "]}";
    
    String writerSchemaStr = "{"
        + "\"type\": \"record\","
        + "\"name\": \"test\","
        + "\"fields\": ["
        + "  {\"name\": \"id\", \"type\": \"int\"},"
        + "  {\"name\": \"new_name\", \"type\": \"string\"}"  // Field renamed using alias
        + "]}";
    
    Schema tableSchema = new Schema.Parser().parse(tableSchemaStr);
    Schema writerSchema = new Schema.Parser().parse(writerSchemaStr);
    
    HoodieIndexDefinition indexDef = createSecondaryIndexDefinition("secondary_index_name", "old_name");
    HoodieIndexMetadata indexMetadata = createIndexMetadata(indexDef);
    
    // Should pass because the field is found via alias and type hasn't changed
    assertDoesNotThrow(() -> 
        HoodieTable.validateSecondaryIndexSchemaEvolution(tableSchema, writerSchema, indexMetadata));
  }

  @Test
  public void testNullableFieldEvolution() {
    // Test evolution from non-nullable to nullable
    String evolvedSchema = TABLE_SCHEMA.replace("\"name\", \"type\": \"string\"", 
                                                 "\"name\", \"type\": [\"null\", \"string\"], \"default\": null");
    
    Schema tableSchema = new Schema.Parser().parse(TABLE_SCHEMA);
    Schema writerSchema = new Schema.Parser().parse(evolvedSchema);
    
    HoodieIndexDefinition indexDef = createSecondaryIndexDefinition("secondary_index_name", "name");
    HoodieIndexMetadata indexMetadata = createIndexMetadata(indexDef);
    
    // Making a field nullable is a backward-compatible change and should be allowed
    assertDoesNotThrow(() -> 
        HoodieTable.validateSecondaryIndexSchemaEvolution(tableSchema, writerSchema, indexMetadata));
  }

  @Test
  public void testMissingIndexedColumnInTableSchema() {
    // Edge case: index references a column that doesn't exist in table schema
    Schema tableSchema = new Schema.Parser().parse(TABLE_SCHEMA);
    Schema writerSchema = new Schema.Parser().parse(TABLE_SCHEMA);
    
    HoodieIndexDefinition indexDef = createSecondaryIndexDefinition("secondary_index_nonexistent", "nonexistent_column");
    HoodieIndexMetadata indexMetadata = createIndexMetadata(indexDef);
    
    // Should handle gracefully (logs warning and continues)
    assertDoesNotThrow(() -> 
        HoodieTable.validateSecondaryIndexSchemaEvolution(tableSchema, writerSchema, indexMetadata));
  }

  @Test
  public void testNonSecondaryIndexDefinitions() {
    // Test that non-secondary index definitions are ignored
    Schema tableSchema = new Schema.Parser().parse(TABLE_SCHEMA);
    Schema writerSchema = new Schema.Parser().parse(TABLE_SCHEMA.replace("\"age\", \"type\": \"int\"", "\"age\", \"type\": \"long\""));
    
    // Create an expression index (not secondary index)
    HoodieIndexDefinition expressionIndexDef = HoodieIndexDefinition.newBuilder()
        .withIndexName(MetadataPartitionType.EXPRESSION_INDEX.getPartitionPath() + "_expr_idx")
        .withIndexType(MetadataPartitionType.EXPRESSION_INDEX.getPartitionPath())
        .withSourceFields(Collections.singletonList("age"))
        .build();
    
    HoodieIndexMetadata indexMetadata = createIndexMetadata(expressionIndexDef);
    
    // Should not throw because it's not a secondary index
    assertDoesNotThrow(() -> 
        HoodieTable.validateSecondaryIndexSchemaEvolution(tableSchema, writerSchema, indexMetadata));
  }

  @Test
  public void testFixedTypeEvolution() {
    // Test fixed type size changes
    String fixed8Schema = "{"
        + "\"type\": \"record\","
        + "\"name\": \"test\","
        + "\"fields\": ["
        + "  {\"name\": \"id\", \"type\": \"int\"},"
        + "  {\"name\": \"fixed_field\", \"type\": {\"type\": \"fixed\", \"name\": \"FixedField\", \"size\": 8}}"
        + "]}";
    
    String fixed16Schema = "{"
        + "\"type\": \"record\","
        + "\"name\": \"test\","
        + "\"fields\": ["
        + "  {\"name\": \"id\", \"type\": \"int\"},"
        + "  {\"name\": \"fixed_field\", \"type\": {\"type\": \"fixed\", \"name\": \"FixedField\", \"size\": 16}}"
        + "]}";
    
    Schema tableSchema = new Schema.Parser().parse(fixed8Schema);
    Schema writerSchema = new Schema.Parser().parse(fixed16Schema);
    
    HoodieIndexDefinition indexDef = createSecondaryIndexDefinition("secondary_index_fixed", "fixed_field");
    HoodieIndexMetadata indexMetadata = createIndexMetadata(indexDef);
    
    final Schema tableSchemaFixed1 = tableSchema;
    final Schema writerSchemaFixed1 = writerSchema;
    final HoodieIndexMetadata indexMetadataFixed = indexMetadata;
    SchemaCompatibilityException exception = assertThrows(SchemaCompatibilityException.class, () -> 
        HoodieTable.validateSecondaryIndexSchemaEvolution(tableSchemaFixed1, writerSchemaFixed1, indexMetadataFixed));
    
    assertTrue(exception.getMessage().contains("fixed_field"));
    assertTrue(exception.getMessage().contains("secondary index"));
    
    // Fixed size decrease
    tableSchema = new Schema.Parser().parse(fixed16Schema);
    writerSchema = new Schema.Parser().parse(fixed8Schema);
    
    final Schema tableSchemaFixed2 = tableSchema;
    final Schema writerSchemaFixed2 = writerSchema;
    exception = assertThrows(SchemaCompatibilityException.class, () -> 
        HoodieTable.validateSecondaryIndexSchemaEvolution(tableSchemaFixed2, writerSchemaFixed2, indexMetadataFixed));
    
    assertTrue(exception.getMessage().contains("fixed_field"));
    assertTrue(exception.getMessage().contains("secondary index"));
  }

  private HoodieIndexDefinition createSecondaryIndexDefinition(String indexName, String... sourceFields) {
    return HoodieIndexDefinition.newBuilder()
        .withIndexName(MetadataPartitionType.SECONDARY_INDEX.getPartitionPath() + "_" + indexName)
        .withIndexType(MetadataPartitionType.SECONDARY_INDEX.getPartitionPath())
        .withSourceFields(Arrays.asList(sourceFields))
        .build();
  }

  private HoodieIndexMetadata createIndexMetadata(HoodieIndexDefinition... indexDefs) {
    Map<String, HoodieIndexDefinition> indexDefMap = new HashMap<>();
    for (HoodieIndexDefinition indexDef : indexDefs) {
      indexDefMap.put(indexDef.getIndexName(), indexDef);
    }
    return new HoodieIndexMetadata(indexDefMap);
  }
}