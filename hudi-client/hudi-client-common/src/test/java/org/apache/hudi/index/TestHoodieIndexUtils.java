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

package org.apache.hudi.index;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.exception.HoodieMetadataIndexException;
import org.apache.hudi.metadata.MetadataPartitionType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.hudi.common.config.HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP;
import static org.apache.hudi.index.HoodieIndexUtils.validateDataTypeForSecondaryIndex;
import static org.apache.hudi.index.HoodieIndexUtils.validateDataTypeForSecondaryOrExpressionIndex;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_SECONDARY_INDEX;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

/**
 * Test cases for HoodieIndexUtils.
 */
public class TestHoodieIndexUtils {

  @Mock
  private HoodieTableMetaClient mockMetaClient;

  @Mock
  private HoodieTableConfig mockTableConfig;

  @BeforeEach
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    when(mockMetaClient.getTableConfig()).thenReturn(mockTableConfig);
  }

  /**
   * Test eligibility check for secondary index with supported data types.
   * 
   * Given: A schema with supported data types (String/CHAR, Int, Long, Float, Double) and record index enabled
   * When: Checking eligibility for secondary index creation
   * Then: Should not throw exception as all data types are supported and record index requirement is met
   */
  @Test
  public void testIsEligibleForSecondaryIndexWithSupportedDataTypes() {
    // Given: A schema with supported data types for secondary index (String/CHAR, Int, Long, Float, Double)
    // Note: CHAR is represented as STRING in Avro schema
    HoodieSchema schema = HoodieSchema.createRecord("TestRecord", null, null, Arrays.asList(
        HoodieSchemaField.of("stringField", HoodieSchema.create(HoodieSchemaType.STRING)),
        HoodieSchemaField.of("charField", HoodieSchema.create(HoodieSchemaType.STRING)),
        HoodieSchemaField.of("intField", HoodieSchema.createNullable(HoodieSchemaType.INT)),
        HoodieSchemaField.of("longField", HoodieSchema.create(HoodieSchemaType.LONG)),
        HoodieSchemaField.of("doubleField", HoodieSchema.create(HoodieSchemaType.DOUBLE))
    ));

    // Mock the schema resolver
    try (MockedConstruction<TableSchemaResolver> mockedResolver = Mockito.mockConstruction(TableSchemaResolver.class,
        (mock, context) -> when(mock.getTableSchema()).thenReturn(schema))) {

      // Test case 1: Secondary index with record index already present
      // Given: Record index partition already exists
      Set<String> partitions = new HashSet<>();
      partitions.add(MetadataPartitionType.RECORD_INDEX.getPartitionPath());
      when(mockTableConfig.getMetadataPartitions()).thenReturn(partitions);

      Map<String, Map<String, String>> columns = new HashMap<>();
      columns.put("stringField", Collections.emptyMap());
      Map<String, String> options = new HashMap<>();

      // When: Checking eligibility for secondary index
      // Then: Should not throw exception because data type is supported and record index exists
      assertDoesNotThrow(() -> HoodieIndexUtils.validateEligibilityForSecondaryOrExpressionIndex(
          mockMetaClient, PARTITION_NAME_SECONDARY_INDEX, options, columns, "test_index"));

      // Test case 2: Secondary index with record index enabled in options
      // Given: No record index partition but enabled in options
      when(mockTableConfig.getMetadataPartitions()).thenReturn(Collections.emptySet());
      options.put(GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key(), "true");

      // When: Checking eligibility for secondary index
      // Then: Should not throw exception because data type is supported and record index is enabled
      assertDoesNotThrow(() -> HoodieIndexUtils.validateEligibilityForSecondaryOrExpressionIndex(
          mockMetaClient, PARTITION_NAME_SECONDARY_INDEX, options, columns, "test_index"));

      // Test case 3: Secondary index with multiple supported fields (including float and double)
      // Given: Multiple columns with supported data types
      columns.clear();
      columns.put("stringField", Collections.emptyMap());
      columns.put("intField", Collections.emptyMap());
      columns.put("longField", Collections.emptyMap());
      columns.put("doubleField", Collections.emptyMap());

      // When: Checking eligibility for secondary index with multiple columns
      // Then: Should not throw exception because all data types are supported
      assertDoesNotThrow(() -> HoodieIndexUtils.validateEligibilityForSecondaryOrExpressionIndex(
          mockMetaClient, PARTITION_NAME_SECONDARY_INDEX, options, columns, "test_index"));
      // Test case 5: Secondary index with double field alone
      // Given: Column with double data type
      columns.clear();
      columns.put("doubleField", Collections.emptyMap());
      
      // When: Checking eligibility for secondary index with double field
      // Then: Should not throw exception because double is now supported
      assertDoesNotThrow(() -> HoodieIndexUtils.validateEligibilityForSecondaryOrExpressionIndex(
          mockMetaClient, PARTITION_NAME_SECONDARY_INDEX, options, columns, "test_index"));
    }
  }

  @Test
  public void testValidateDataTypeForSecondaryOrExpressionIndex() {
    // Create a dummy schema with both complex and primitive types
    HoodieSchema schema = HoodieSchema.createRecord("TestRecord", null, null, Arrays.asList(
        HoodieSchemaField.of("stringField", HoodieSchema.create(HoodieSchemaType.STRING)),
        HoodieSchemaField.of("intField", HoodieSchema.createNullable(HoodieSchemaType.INT)),
        HoodieSchemaField.of("arrayField", HoodieSchema.createArray(HoodieSchema.create(HoodieSchemaType.STRING))),
        HoodieSchemaField.of("mapField", HoodieSchema.createMap(HoodieSchema.create(HoodieSchemaType.INT))),
        HoodieSchemaField.of("structField", HoodieSchema.createRecord("NestedRecord", null, null, Arrays.asList(
            HoodieSchemaField.of("nestedString", HoodieSchema.create(HoodieSchemaType.STRING))
        )))
    ));
    // Test for primitive fields
    assertTrue(validateDataTypeForSecondaryOrExpressionIndex(Arrays.asList("stringField", "intField"), schema));

    // Test for complex fields
    assertFalse(validateDataTypeForSecondaryOrExpressionIndex(Arrays.asList("arrayField", "mapField", "structField"), schema));
  }

  /**
   * Test validation of data types for secondary index.
   *
   * Given: A schema with various data types including supported (String/CHAR, Int, Long) and unsupported (Double, Boolean, Array, Map, Struct) types
   * When: Validating each data type for secondary index compatibility
   * Then: Should return true for supported types and false for unsupported types
   */
  @Test
  public void testValidateDataTypeForSecondaryIndex() {
    // Create a schema with various data types
    HoodieSchema schema = HoodieSchema.createRecord("TestRecord", null, null, Arrays.asList(
        HoodieSchemaField.of("stringField", HoodieSchema.create(HoodieSchemaType.STRING)),
        HoodieSchemaField.of("charField", HoodieSchema.create(HoodieSchemaType.STRING)), // CHAR is represented as STRING
        HoodieSchemaField.of("intField", HoodieSchema.createNullable(HoodieSchemaType.INT)),
        HoodieSchemaField.of("longField", HoodieSchema.create(HoodieSchemaType.LONG)),
        HoodieSchemaField.of("timestampField", HoodieSchema.create(HoodieSchemaType.LONG), null, 0L), // timestamp as long
        HoodieSchemaField.of("booleanField", HoodieSchema.create(HoodieSchemaType.BOOLEAN)),
        HoodieSchemaField.of("floatField", HoodieSchema.create(HoodieSchemaType.FLOAT)),
        HoodieSchemaField.of("doubleField", HoodieSchema.create(HoodieSchemaType.DOUBLE)),
        HoodieSchemaField.of("arrayField", HoodieSchema.createArray(HoodieSchema.create(HoodieSchemaType.STRING))),
        HoodieSchemaField.of("mapField", HoodieSchema.createMap(HoodieSchema.create(HoodieSchemaType.INT))),
        HoodieSchemaField.of("structField", HoodieSchema.createRecord("NestedRecord", null, null, Arrays.asList(
            HoodieSchemaField.of("nestedString", HoodieSchema.create(HoodieSchemaType.STRING))
        )))
    ));

    // Test supported types for secondary index
    assertTrue(validateDataTypeForSecondaryIndex(Collections.singletonList("stringField"), schema));
    assertTrue(validateDataTypeForSecondaryIndex(Collections.singletonList("charField"), schema)); // CHAR as STRING
    assertTrue(validateDataTypeForSecondaryIndex(Collections.singletonList("intField"), schema));
    assertTrue(validateDataTypeForSecondaryIndex(Collections.singletonList("longField"), schema));
    assertTrue(validateDataTypeForSecondaryIndex(Collections.singletonList("timestampField"), schema));
    assertTrue(validateDataTypeForSecondaryIndex(Collections.singletonList("doubleField"), schema));

    // Test multiple supported fields
    assertTrue(validateDataTypeForSecondaryIndex(Arrays.asList("stringField", "intField", "longField"), schema));

    // Test unsupported types for secondary index
    assertFalse(validateDataTypeForSecondaryIndex(Collections.singletonList("booleanField"), schema));
    assertFalse(validateDataTypeForSecondaryIndex(Collections.singletonList("arrayField"), schema));
    assertFalse(validateDataTypeForSecondaryIndex(Collections.singletonList("mapField"), schema));
    assertFalse(validateDataTypeForSecondaryIndex(Collections.singletonList("structField"), schema));
    assertFalse(validateDataTypeForSecondaryIndex(Collections.singletonList("floatField"), schema));

    // Test mix of supported and unsupported types (should fail)
    assertFalse(validateDataTypeForSecondaryIndex(Arrays.asList("stringField", "booleanField"), schema));
  }

  /**
   * Test validation of logical types for secondary index.
   *
   * Given: A schema with all Avro logical types including supported (timestampMillis, timestampMicros, date, timeMillis, timeMicros) 
   *        and unsupported (decimal, uuid, duration, localTimestampMillis, localTimestampMicros)
   * When: Validating each logical type for secondary index compatibility
   * Then: Should return true only for timestamp and date/time logical types, false for others
   */
  @Test
  public void testValidateDataTypeForSecondaryIndexWithLogicalTypes() {
    // Supported logical types
    HoodieSchema timestampMillis = HoodieSchema.createTimestampMillis();
    HoodieSchema timestampMicros = HoodieSchema.createTimestampMicros();
    HoodieSchema date = HoodieSchema.createDate();
    HoodieSchema timeMillis = HoodieSchema.createTimeMillis();
    HoodieSchema timeMicros = HoodieSchema.createTimeMicros();

    // Unsupported logical types
    HoodieSchema decimal = HoodieSchema.createDecimal(10, 2);
    HoodieSchema uuid = HoodieSchema.createUUID();
    HoodieSchema localTimestampMillis = HoodieSchema.createLocalTimestampMillis();
    HoodieSchema localTimestampMicros = HoodieSchema.createLocalTimestampMicros();

    HoodieSchema schemaWithLogicalTypes = HoodieSchema.createRecord("TestRecord", null, null, Arrays.asList(
        // Supported logical types
        HoodieSchemaField.of("timestampMillisField", timestampMillis),
        HoodieSchemaField.of("timestampMicrosField", timestampMicros),
        HoodieSchemaField.of("dateField", date),
        HoodieSchemaField.of("timeMillisField", timeMillis),
        HoodieSchemaField.of("timeMicrosField", timeMicros),
        // Unsupported logical types
        HoodieSchemaField.of("decimalField", decimal),
        HoodieSchemaField.of("uuidField", uuid),
        HoodieSchemaField.of("localTimestampMillisField", localTimestampMillis),
        HoodieSchemaField.of("localTimestampMicrosField", localTimestampMicros)
    ));

    // Test supported timestamp and date/time logical types
    assertTrue(validateDataTypeForSecondaryIndex(Collections.singletonList("timestampMillisField"), schemaWithLogicalTypes));
    assertTrue(validateDataTypeForSecondaryIndex(Collections.singletonList("timestampMicrosField"), schemaWithLogicalTypes));
    assertTrue(validateDataTypeForSecondaryIndex(Collections.singletonList("dateField"), schemaWithLogicalTypes));
    assertTrue(validateDataTypeForSecondaryIndex(Collections.singletonList("timeMillisField"), schemaWithLogicalTypes));
    assertTrue(validateDataTypeForSecondaryIndex(Collections.singletonList("timeMicrosField"), schemaWithLogicalTypes));
    
    // Test unsupported logical types
    assertFalse(validateDataTypeForSecondaryIndex(Collections.singletonList("decimalField"), schemaWithLogicalTypes));
    assertFalse(validateDataTypeForSecondaryIndex(Collections.singletonList("uuidField"), schemaWithLogicalTypes));
    assertFalse(validateDataTypeForSecondaryIndex(Collections.singletonList("localTimestampMillisField"), schemaWithLogicalTypes));
    assertFalse(validateDataTypeForSecondaryIndex(Collections.singletonList("localTimestampMicrosField"), schemaWithLogicalTypes));
    
    // Test mix of supported and unsupported logical types
    assertFalse(validateDataTypeForSecondaryIndex(Arrays.asList("timestampMillisField", "decimalField"), schemaWithLogicalTypes));
  }

  /**
   * Test eligibility check for secondary index with unsupported data types.
   * 
   * Given: A schema with unsupported data types (Boolean, Decimal) and record index enabled
   * When: Checking eligibility for secondary index creation
   * Then: Should throw HoodieMetadataIndexException as these data types are not supported for secondary index
   */
  @Test
  public void testIsEligibleForSecondaryIndexWithUnsupportedDataTypes() {
    // Given: A schema with unsupported data types for secondary index (Boolean, Decimal)
    // Note: Float and Double are now supported
    HoodieSchema decimalType = HoodieSchema.createDecimal(10, 2);
    HoodieSchema schema = HoodieSchema.createRecord("TestRecord", null, null, Arrays.asList(
        HoodieSchemaField.of("stringField", HoodieSchema.create(HoodieSchemaType.STRING)),
        HoodieSchemaField.of("floatField", HoodieSchema.create(HoodieSchemaType.FLOAT)),
        HoodieSchemaField.of("doubleField", HoodieSchema.create(HoodieSchemaType.DOUBLE)),
        HoodieSchemaField.of("booleanField", HoodieSchema.create(HoodieSchemaType.BOOLEAN)),
        HoodieSchemaField.of("decimalField", decimalType)
    ));

    // Mock the schema resolver
    try (MockedConstruction<TableSchemaResolver> mockedResolver = Mockito.mockConstruction(TableSchemaResolver.class,
        (mock, context) -> when(mock.getTableSchema()).thenReturn(schema))) {

      // Given: Record index partition exists
      Set<String> partitions = new HashSet<>();
      partitions.add(MetadataPartitionType.RECORD_INDEX.getPartitionPath());
      when(mockTableConfig.getMetadataPartitions()).thenReturn(partitions);

      Map<String, Map<String, String>> columns = new HashMap<>();
      Map<String, String> options = new HashMap<>();

      // Test case 1: Supported float field (now supported)
      // Given: Column with float data type
      columns.put("floatField", Collections.emptyMap());
      
      // When: Checking eligibility for secondary index
      // Then: Should not throw exception because float is now supported for secondary index
      assertThrows(HoodieMetadataIndexException.class,
          () -> HoodieIndexUtils.validateEligibilityForSecondaryOrExpressionIndex(
          mockMetaClient, PARTITION_NAME_SECONDARY_INDEX, options, columns, "test_index"));
      
      // Test case 2: Supported double field (now supported)
      // Given: Column with double data type
      columns.clear();
      columns.put("doubleField", Collections.emptyMap());
      
      // When: Checking eligibility for secondary index
      // Then: Should not throw exception because double is now supported for secondary index
      assertDoesNotThrow(() -> HoodieIndexUtils.validateEligibilityForSecondaryOrExpressionIndex(
          mockMetaClient, PARTITION_NAME_SECONDARY_INDEX, options, columns, "test_index"));
      
      // Test case 3: Unsupported boolean field
      // Given: Column with boolean data type
      columns.clear();
      columns.put("booleanField", Collections.emptyMap());
      
      // When: Checking eligibility for secondary index
      // Then: Should throw exception because boolean is not supported for secondary index
      HoodieMetadataIndexException ex3 = assertThrows(HoodieMetadataIndexException.class,
          () -> HoodieIndexUtils.validateEligibilityForSecondaryOrExpressionIndex(
              mockMetaClient, PARTITION_NAME_SECONDARY_INDEX, options, columns, "test_index"));
      assertTrue(ex3.getMessage().contains("unsupported data type"));
      assertTrue(ex3.getMessage().contains("BOOLEAN"));
      assertTrue(ex3.getMessage().contains("Secondary indexes only support"));
      
      // Test case 4: Unsupported decimal field
      // Given: Column with decimal data type
      columns.clear();
      columns.put("decimalField", Collections.emptyMap());
      
      // When: Checking eligibility for secondary index
      // Then: Should throw exception because decimal is not supported for secondary index
      HoodieMetadataIndexException ex4 = assertThrows(HoodieMetadataIndexException.class,
          () -> HoodieIndexUtils.validateEligibilityForSecondaryOrExpressionIndex(
              mockMetaClient, PARTITION_NAME_SECONDARY_INDEX, options, columns, "test_index"));
      assertTrue(ex4.getMessage().contains("unsupported data type"));
      assertTrue(ex4.getMessage().contains("DECIMAL"));
      assertTrue(ex4.getMessage().contains("Secondary indexes only support"));

      // Test case 5: Mix of supported fields (now including double)
      // Given: Columns with supported types (string and double)
      columns.clear();
      columns.put("stringField", Collections.emptyMap());
      columns.put("doubleField", Collections.emptyMap());
      
      // When: Checking eligibility for secondary index
      // Then: Should not throw exception because both field types are now supported
      assertDoesNotThrow(() -> HoodieIndexUtils.validateEligibilityForSecondaryOrExpressionIndex(
          mockMetaClient, PARTITION_NAME_SECONDARY_INDEX, options, columns, "test_index"));
    }
  }

  /**
   * Test eligibility check for secondary index with logical types.
   * 
   * Given: A schema with timestamp and date logical types and record index enabled
   * When: Checking eligibility for secondary index creation
   * Then: Should not throw exception as timestamp and date logical types are supported
   */
  @Test
  public void testIsEligibleForSecondaryIndexWithLogicalTypes() {
    // Given: A schema with timestamp and date logical types
    HoodieSchema timestampMillis = HoodieSchema.createTimestampMillis();
    HoodieSchema date = HoodieSchema.createDate();

    HoodieSchema schema = HoodieSchema.createRecord("TestRecord", null, null, Arrays.asList(
          HoodieSchemaField.of("timestampField", timestampMillis),
          HoodieSchemaField.of("dateField", date)
        ));
    // Mock the schema resolver
    try (MockedConstruction<TableSchemaResolver> mockedResolver = Mockito.mockConstruction(TableSchemaResolver.class,
        (mock, context) -> when(mock.getTableSchema()).thenReturn(schema))) {

      // Given: Record index partition exists
      Set<String> partitions = new HashSet<>();
      partitions.add(MetadataPartitionType.RECORD_INDEX.getPartitionPath());
      when(mockTableConfig.getMetadataPartitions()).thenReturn(partitions);

      Map<String, Map<String, String>> columns = new HashMap<>();
      columns.put("timestampField", Collections.emptyMap());
      columns.put("dateField", Collections.emptyMap());
      Map<String, String> options = new HashMap<>();

      // When: Checking eligibility for secondary index with logical types
      // Then: Should not throw exception because timestamp and date logical types are supported
      assertDoesNotThrow(() -> HoodieIndexUtils.validateEligibilityForSecondaryOrExpressionIndex(
          mockMetaClient, PARTITION_NAME_SECONDARY_INDEX, options, columns, "test_index"));
    }
  }

  /**
   * Test eligibility check for secondary index without record index.
   * 
   * Given: A schema with supported data types but no record index enabled
   * When: Checking eligibility for secondary index creation
   * Then: Should throw HoodieMetadataIndexException as record index is a prerequisite for secondary index
   */
  @Test
  public void testIsEligibleForSecondaryIndexWithoutRecordIndex() {
    // Given: A schema with supported data types
    HoodieSchema schema = HoodieSchema.createRecord("TestRecord", null, null, Collections.singletonList(
        HoodieSchemaField.of("stringField", HoodieSchema.create(HoodieSchemaType.STRING))
    ));

    // Mock the schema resolver
    try (MockedConstruction<TableSchemaResolver> mockedResolver = Mockito.mockConstruction(TableSchemaResolver.class,
        (mock, context) -> when(mock.getTableSchema()).thenReturn(schema))) {

      // Test case 1: No record index partition and not enabled in options
      // Given: No record index partition exists and not enabled in options
      when(mockTableConfig.getMetadataPartitions()).thenReturn(Collections.emptySet());

      Map<String, Map<String, String>> columns = new HashMap<>();
      columns.put("stringField", Collections.emptyMap());
      Map<String, String> options = new HashMap<>();

      // When: Checking eligibility for secondary index
      // Then: Should throw exception because record index is required for secondary index
      HoodieMetadataIndexException ex = assertThrows(HoodieMetadataIndexException.class,
          () -> HoodieIndexUtils.validateEligibilityForSecondaryOrExpressionIndex(
              mockMetaClient, PARTITION_NAME_SECONDARY_INDEX, options, columns, "test_index"));
      assertTrue(ex.getMessage().contains("Record index is required"));
      assertTrue(ex.getMessage().contains("not enabled"));

      // Test case 2: Record index explicitly disabled
      // Given: Record index is explicitly disabled in options
      options.put(GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key(), "false");
      
      // When: Checking eligibility for secondary index
      // Then: Should throw exception because record index is disabled
      HoodieMetadataIndexException ex2 = assertThrows(HoodieMetadataIndexException.class,
          () -> HoodieIndexUtils.validateEligibilityForSecondaryOrExpressionIndex(
              mockMetaClient, PARTITION_NAME_SECONDARY_INDEX, options, columns, "test_index"));
      assertTrue(ex2.getMessage().contains("Record index is required"));
      assertTrue(ex2.getMessage().contains("not enabled"));
    }
  }

  /**
   * Test eligibility check for expression index with various data types.
   * 
   * Given: A schema with primitive and complex data types
   * When: Checking eligibility for expression index creation
   * Then: Should not throw exception for primitive types and throw for complex types
   */
  @Test
  public void testIsEligibleForExpressionIndex() {
    // Given: A schema with various data types including complex types
    HoodieSchema schema = HoodieSchema.createRecord("TestRecord", null, null, Arrays.asList(
        HoodieSchemaField.of("stringField", HoodieSchema.create(HoodieSchemaType.STRING)),
        HoodieSchemaField.of("floatField", HoodieSchema.create(HoodieSchemaType.FLOAT)),
        HoodieSchemaField.of("arrayField", HoodieSchema.createArray(HoodieSchema.create(HoodieSchemaType.STRING))),
        HoodieSchemaField.of("mapField", HoodieSchema.createMap(HoodieSchema.create(HoodieSchemaType.INT)))
    ));

    // Mock the schema resolver
    try (MockedConstruction<TableSchemaResolver> mockedResolver = Mockito.mockConstruction(TableSchemaResolver.class,
        (mock, context) -> when(mock.getTableSchema()).thenReturn(schema))) {

      Map<String, Map<String, String>> columns = new HashMap<>();
      Map<String, String> options = new HashMap<>();

      // Test case 1: Expression index with primitive string type
      // Given: Column with string data type
      columns.put("stringField", Collections.emptyMap());
      
      // When: Checking eligibility for expression index
      // Then: Should not throw exception because string is a primitive type
      assertDoesNotThrow(() -> HoodieIndexUtils.validateEligibilityForSecondaryOrExpressionIndex(
          mockMetaClient, "EXPRESSION_INDEX", options, columns, "test_index"));

      // Test case 2: Expression index with float type
      // Given: Column with float data type
      columns.clear();
      columns.put("floatField", Collections.emptyMap());
      
      // When: Checking eligibility for expression index
      // Then: Should not throw exception because float is a primitive type (allowed for expression index)
      assertDoesNotThrow(() -> HoodieIndexUtils.validateEligibilityForSecondaryOrExpressionIndex(
          mockMetaClient, "EXPRESSION_INDEX", options, columns, "test_index"));

      // Test case 3: Expression index with complex array type
      // Given: Column with array data type
      columns.clear();
      columns.put("arrayField", Collections.emptyMap());
      
      // When: Checking eligibility for expression index
      // Then: Should throw exception because array is a complex type
      HoodieMetadataIndexException ex = assertThrows(HoodieMetadataIndexException.class,
          () -> HoodieIndexUtils.validateEligibilityForSecondaryOrExpressionIndex(
              mockMetaClient, "EXPRESSION_INDEX", options, columns, "test_index"));
      assertTrue(ex.getMessage().contains("Complex types"));
      assertTrue(ex.getMessage().contains("ARRAY"));

      // Test case 4: Expression index with complex map type
      // Given: Column with map data type
      columns.clear();
      columns.put("mapField", Collections.emptyMap());
      
      // When: Checking eligibility for expression index
      // Then: Should throw exception because map is a complex type
      HoodieMetadataIndexException ex2 = assertThrows(HoodieMetadataIndexException.class,
          () -> HoodieIndexUtils.validateEligibilityForSecondaryOrExpressionIndex(
              mockMetaClient, "EXPRESSION_INDEX", options, columns, "test_index"));
      assertTrue(ex2.getMessage().contains("Complex types"));
      assertTrue(ex2.getMessage().contains("MAP"));
    }
  }

  /**
   * Test eligibility check for expression index with nullable fields.
   * 
   * Given: A schema with nullable primitive fields
   * When: Checking eligibility for expression index creation
   * Then: Should not throw exception as nullable primitive types are allowed
   */
  @Test
  public void testIsEligibleForExpressionIndexWithNullableFields() {
    // An int with default 0 must have the int type defined first.
    // If null is defined first, which HoodieSchema#createNullable does, an error will be thrown
    HoodieSchema nullableIntWithDefault = HoodieSchema.createUnion(HoodieSchema.create(HoodieSchemaType.INT), HoodieSchema.create(HoodieSchemaType.NULL));
    // Given: A schema with nullable fields (union types)
    HoodieSchema schema = HoodieSchema.createRecord("TestRecord", null, null, Arrays.asList(
        HoodieSchemaField.of("nullableStringField", HoodieSchema.create(HoodieSchemaType.STRING)),
        HoodieSchemaField.of("nullableIntField", nullableIntWithDefault, null, 0)
    ));
    // Mock the schema resolver
    try (MockedConstruction<TableSchemaResolver> mockedResolver = Mockito.mockConstruction(TableSchemaResolver.class,
        (mock, context) -> when(mock.getTableSchema()).thenReturn(schema))) {

      Map<String, Map<String, String>> columns = new HashMap<>();
      columns.put("nullableStringField", Collections.emptyMap());
      columns.put("nullableIntField", Collections.emptyMap());
      Map<String, String> options = new HashMap<>();

      // When: Checking eligibility for expression index with nullable fields
      // Then: Should not throw exception because nullable primitive types are allowed
      assertDoesNotThrow(() -> HoodieIndexUtils.validateEligibilityForSecondaryOrExpressionIndex(
          mockMetaClient, "EXPRESSION_INDEX", options, columns, "test_index"));
    }
  }

  /**
   * Test eligibility check for secondary index with nullable fields.
   * 
   * Given: A schema with nullable fields of supported types and record index enabled
   * When: Checking eligibility for secondary index creation
   * Then: Should not throw exception as nullable versions of supported types are allowed
   */
  @Test
  public void testIsEligibleForSecondaryIndexWithNullableFields() {
    HoodieSchema nullableIntWithDefault = HoodieSchema.createUnion(HoodieSchema.create(HoodieSchemaType.INT), HoodieSchema.create(HoodieSchemaType.NULL));
    HoodieSchema nullableLongWithDefault = HoodieSchema.createUnion(HoodieSchema.create(HoodieSchemaType.LONG), HoodieSchema.create(HoodieSchemaType.NULL));
    // Given: A schema with nullable fields that are supported for secondary index
    HoodieSchema schema = HoodieSchema.createRecord("TestRecord", null, null, Arrays.asList(
          HoodieSchemaField.of("nullableStringField", HoodieSchema.create(HoodieSchemaType.STRING)),
          HoodieSchemaField.of("nullableIntField", nullableIntWithDefault, null, 0),
          HoodieSchemaField.of("nullableLongField", nullableLongWithDefault, null, 0L)
        ));

    // Mock the schema resolver
    try (MockedConstruction<TableSchemaResolver> mockedResolver = Mockito.mockConstruction(TableSchemaResolver.class,
        (mock, context) -> when(mock.getTableSchema()).thenReturn(schema))) {

      // Given: Record index partition exists
      Set<String> partitions = new HashSet<>();
      partitions.add(MetadataPartitionType.RECORD_INDEX.getPartitionPath());
      when(mockTableConfig.getMetadataPartitions()).thenReturn(partitions);

      Map<String, Map<String, String>> columns = new HashMap<>();
      columns.put("nullableStringField", Collections.emptyMap());
      columns.put("nullableIntField", Collections.emptyMap());
      columns.put("nullableLongField", Collections.emptyMap());
      Map<String, String> options = new HashMap<>();

      // When: Checking eligibility for secondary index with nullable fields
      // Then: Should not throw exception because nullable versions of supported types are allowed
      assertDoesNotThrow(() -> HoodieIndexUtils.validateEligibilityForSecondaryOrExpressionIndex(
          mockMetaClient, PARTITION_NAME_SECONDARY_INDEX, options, columns, "test_index"));
    }
  }

  /**
   * Test eligibility check for secondary index with all supported logical types.
   * 
   * Given: A schema with all timestamp-related logical types and record index enabled
   * When: Checking eligibility for secondary index creation
   * Then: Should not throw exception as all timestamp-related logical types are supported
   */
  @Test
  public void testIsEligibleForSecondaryIndexWithAllLogicalTypes() {
    // Given: A schema with all supported timestamp logical types
    HoodieSchema timestampMillis = HoodieSchema.createTimestampMillis();
    HoodieSchema timestampMicros = HoodieSchema.createTimestampMicros();
    HoodieSchema date = HoodieSchema.createDate();
    HoodieSchema timeMillis = HoodieSchema.createTimeMillis();
    HoodieSchema timeMicros = HoodieSchema.createTimeMicros();

    HoodieSchema schema = HoodieSchema.createRecord("TestRecord", null, null, Arrays.asList(
        HoodieSchemaField.of("timestampMillisField", timestampMillis),
        HoodieSchemaField.of("timestampMicrosField", timestampMicros),
        HoodieSchemaField.of("dateField", date),
        HoodieSchemaField.of("timeMillisField", timeMillis),
        HoodieSchemaField.of("timeMicrosField", timeMicros)
        ));

    // Mock the schema resolver
    try (MockedConstruction<TableSchemaResolver> mockedResolver = Mockito.mockConstruction(TableSchemaResolver.class,
        (mock, context) -> when(mock.getTableSchema()).thenReturn(schema))) {

      // Given: Record index is enabled
      Set<String> partitions = new HashSet<>();
      partitions.add(MetadataPartitionType.RECORD_INDEX.getPartitionPath());
      when(mockTableConfig.getMetadataPartitions()).thenReturn(partitions);

      Map<String, Map<String, String>> columns = new HashMap<>();
      columns.put("timestampMillisField", Collections.emptyMap());
      columns.put("timestampMicrosField", Collections.emptyMap());
      columns.put("dateField", Collections.emptyMap());
      columns.put("timeMillisField", Collections.emptyMap());
      columns.put("timeMicrosField", Collections.emptyMap());
      Map<String, String> options = new HashMap<>();

      // When: Checking eligibility for secondary index with all logical types
      // Then: Should not throw exception because all timestamp-related logical types are supported
      assertDoesNotThrow(() -> HoodieIndexUtils.validateEligibilityForSecondaryOrExpressionIndex(
          mockMetaClient, PARTITION_NAME_SECONDARY_INDEX, options, columns, "test_index"));
    }
  }

  /**
   * Test eligibility check for secondary index with column not in schema.
   * 
   * Given: A schema without the requested column
   * When: Checking eligibility for secondary index creation
   * Then: Should throw HoodieMetadataIndexException with helpful error message
   */
  @Test
  public void testIsEligibleForSecondaryIndexWithColumnNotInSchema() {
    // Given: A schema without the requested column
    HoodieSchema schema = HoodieSchema.createRecord("TestRecord", null, null, Arrays.asList(
        HoodieSchemaField.of("existingField", HoodieSchema.create(HoodieSchemaType.STRING))
    ));

    // Mock the schema resolver
    try (MockedConstruction<TableSchemaResolver> mockedResolver = Mockito.mockConstruction(TableSchemaResolver.class,
        (mock, context) -> when(mock.getTableSchema()).thenReturn(schema))) {

      // Given: Record index is enabled
      Set<String> partitions = new HashSet<>();
      partitions.add(MetadataPartitionType.RECORD_INDEX.getPartitionPath());
      when(mockTableConfig.getMetadataPartitions()).thenReturn(partitions);

      Map<String, Map<String, String>> columns = new HashMap<>();
      columns.put("nonExistentField", Collections.emptyMap());
      Map<String, String> options = new HashMap<>();

      // When: Checking eligibility for secondary index with non-existent column
      // Then: Should throw exception with helpful error message
      HoodieMetadataIndexException ex = assertThrows(HoodieMetadataIndexException.class,
          () -> HoodieIndexUtils.validateEligibilityForSecondaryOrExpressionIndex(
              mockMetaClient, PARTITION_NAME_SECONDARY_INDEX, options, columns, "test_index"));
      assertTrue(ex.getMessage().contains("Column 'nonExistentField' does not exist"));
      assertTrue(ex.getMessage().contains("verify the column name"));
    }
  }

  /**
   * Test eligibility check for secondary index with String fields having logical types.
   * 
   * Given: A schema with String fields that have logical types (UUID)
   * When: Checking eligibility for secondary index creation
   * Then: Should throw HoodieMetadataIndexException as String with logical types is not supported
   */
  @Test
  public void testIsEligibleForSecondaryIndexWithStringLogicalTypes() {
    // Given: A schema with UUID logical type on string field
    HoodieSchema uuidSchema = HoodieSchema.createUUID();

    HoodieSchema schema = HoodieSchema.createRecord("TestRecord", null, null, Arrays.asList(
          HoodieSchemaField.of("uuidField", uuidSchema),
          HoodieSchemaField.of("regularStringField", HoodieSchema.create(HoodieSchemaType.STRING))
    ));

    // Mock the schema resolver
    try (MockedConstruction<TableSchemaResolver> mockedResolver = Mockito.mockConstruction(TableSchemaResolver.class,
        (mock, context) -> when(mock.getTableSchema()).thenReturn(schema))) {

      // Given: Record index is enabled
      Set<String> partitions = new HashSet<>();
      partitions.add(MetadataPartitionType.RECORD_INDEX.getPartitionPath());
      when(mockTableConfig.getMetadataPartitions()).thenReturn(partitions);

      Map<String, Map<String, String>> columns = new HashMap<>();
      Map<String, String> options = new HashMap<>();

      // Test case 1: UUID field should fail for secondary index
      columns.put("uuidField", Collections.emptyMap());
      
      // When: Checking eligibility for secondary index with UUID field
      // Then: Should throw exception because STRING with logical type UUID is not supported
      HoodieMetadataIndexException ex = assertThrows(HoodieMetadataIndexException.class,
          () -> HoodieIndexUtils.validateEligibilityForSecondaryOrExpressionIndex(
              mockMetaClient, PARTITION_NAME_SECONDARY_INDEX, options, columns, "test_index"));
      assertTrue(ex.getMessage().contains("unsupported data type"), 
          "Expected message to contain 'unsupported data type', but was: " + ex.getMessage());
      assertTrue(ex.getMessage().contains("STRING") && ex.getMessage().contains("uuid"), 
          "Expected message to mention STRING and uuid, but was: " + ex.getMessage());
      
      // Test case 2: Regular string field should succeed
      columns.clear();
      columns.put("regularStringField", Collections.emptyMap());
      
      // When: Checking eligibility for secondary index with regular string field
      // Then: Should not throw exception because regular string is supported
      assertDoesNotThrow(() -> HoodieIndexUtils.validateEligibilityForSecondaryOrExpressionIndex(
          mockMetaClient, PARTITION_NAME_SECONDARY_INDEX, options, columns, "test_index"));
    }
  }

  /**
   * Test eligibility check for expression index with column not in schema.
   * 
   * Given: A schema without the requested column
   * When: Checking eligibility for expression index creation
   * Then: Should throw HoodieMetadataIndexException with a helpful error message
   */
  @Test
  public void testIsEligibleForExpressionIndexWithColumnNotInSchema() {
    // Given: A schema without the requested column
    HoodieSchema schema = HoodieSchema.createRecord("TestRecord", null, null, Arrays.asList(
        HoodieSchemaField.of("existingField", HoodieSchema.create(HoodieSchemaType.STRING))
    ));

    // Mock the schema resolver
    try (MockedConstruction<TableSchemaResolver> mockedResolver = Mockito.mockConstruction(TableSchemaResolver.class,
        (mock, context) -> when(mock.getTableSchema()).thenReturn(schema))) {

      Map<String, Map<String, String>> columns = new HashMap<>();
      columns.put("nonExistentField", Collections.emptyMap());
      Map<String, String> options = new HashMap<>();

      // When: Checking eligibility for expression index with non-existent column
      // Then: Should throw exception with helpful error message
      HoodieMetadataIndexException ex = assertThrows(HoodieMetadataIndexException.class,
          () -> HoodieIndexUtils.validateEligibilityForSecondaryOrExpressionIndex(
              mockMetaClient, "EXPRESSION_INDEX", options, columns, "test_index"));
      assertTrue(ex.getMessage().contains("Column 'nonExistentField' does not exist"));
      assertTrue(ex.getMessage().contains("verify the column name"));
    }
  }
}