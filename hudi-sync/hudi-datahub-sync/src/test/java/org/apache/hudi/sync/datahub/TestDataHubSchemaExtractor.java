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

package org.apache.hudi.sync.datahub;

import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldDataType;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.apache.hudi.sync.datahub.DataHubSyncClient.toSchemaFieldDataType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TestDataHubSchemaExtractor {

  private SchemaField createSchemaField(
      String fieldPath, SchemaFieldDataType dataType, String nativeType) {
    return createSchemaField(fieldPath, dataType, nativeType, false);
  }

  private SchemaField createSchemaField(
      String fieldPath, SchemaFieldDataType dataType, String nativeType, boolean isNullable) {
    return new SchemaField()
        .setFieldPath(fieldPath)
        .setType(dataType)
        .setNativeDataType(nativeType)
        .setNullable(isNullable);
  }

  @Test
  void testPrimitiveTypes() throws IOException {
    List<SchemaField> schemaFields = DataHubSyncClient.createSchemaFields(readAvroSchema("schema/primitives.avsc"));
    List<SchemaField> expectedSchemaFields = Arrays.asList(
        createSchemaField("[version=2.0].intField", toSchemaFieldDataType(Schema.Type.INT), "int"),
        createSchemaField("[version=2.0].intFieldV2", toSchemaFieldDataType(Schema.Type.INT), "int"),
        createSchemaField("[version=2.0].nullField", toSchemaFieldDataType(Schema.Type.NULL), "null"),
        createSchemaField("[version=2.0].nullFieldV2", toSchemaFieldDataType(Schema.Type.NULL), "null"),
        createSchemaField("[version=2.0].longField", toSchemaFieldDataType(Schema.Type.LONG), "long"),
        createSchemaField("[version=2.0].floatField", toSchemaFieldDataType(Schema.Type.FLOAT), "float"),
        createSchemaField("[version=2.0].doubleField", toSchemaFieldDataType(Schema.Type.DOUBLE), "double"),
        createSchemaField("[version=2.0].stringField", toSchemaFieldDataType(Schema.Type.STRING), "string"),
        createSchemaField("[version=2.0].booleanField", toSchemaFieldDataType(Schema.Type.BOOLEAN), "boolean"),
        createSchemaField("[version=2.0].nullableIntField", toSchemaFieldDataType(Schema.Type.INT), "int", true),
        createSchemaField("[version=2.0].nullableLongField", toSchemaFieldDataType(Schema.Type.LONG), "long", true),
        createSchemaField("[version=2.0].nullableStringField", toSchemaFieldDataType(Schema.Type.STRING), "string", true),
        createSchemaField("[version=2.0].status", toSchemaFieldDataType(Schema.Type.ENUM), "enum")
    );

    assertEquals(expectedSchemaFields, schemaFields);
  }

  @Test
  void testFailureOnDuplicateNullType() {
    AvroRuntimeException exception = assertThrows(AvroRuntimeException.class, () -> readAvroSchema("schema/invalid_union.avsc"));
    assertEquals("Duplicate in union:null", exception.getMessage());
  }

  @Test
  void testLogicalTypes() throws IOException {
    List<SchemaField> schemaFields = DataHubSyncClient.createSchemaFields(readAvroSchema("schema/logical_types.avsc"));
    List<SchemaField> expectedSchemaFields = Arrays.asList(
        createSchemaField("[version=2.0].decimalField", toSchemaFieldDataType(Schema.Type.BYTES), "decimal(9,2)"),
        createSchemaField("[version=2.0].decimalFieldWithoutScale", toSchemaFieldDataType(Schema.Type.BYTES), "decimal(9,0)"),
        createSchemaField("[version=2.0].decimalFieldWithoutPrecisionAndScale", toSchemaFieldDataType(Schema.Type.BYTES), "bytes"),
        createSchemaField("[version=2.0].timestampMillisField", toSchemaFieldDataType(Schema.Type.LONG), "timestamp-millis"),
        createSchemaField("[version=2.0].timestampMicrosField", toSchemaFieldDataType(Schema.Type.LONG), "timestamp-micros"),
        createSchemaField("[version=2.0].dateField", toSchemaFieldDataType(Schema.Type.INT), "date"),
        createSchemaField("[version=2.0].timeMillisField", toSchemaFieldDataType(Schema.Type.INT), "time-millis"),
        createSchemaField("[version=2.0].timeMicrosField", toSchemaFieldDataType(Schema.Type.LONG), "time-micros"),
        createSchemaField("[version=2.0].uuidField", toSchemaFieldDataType(Schema.Type.STRING), "uuid")
    );

    assertEquals(expectedSchemaFields, schemaFields);
  }

  @Test
  void testComplexTypes_Map() throws IOException {
    List<SchemaField> schemaFields = DataHubSyncClient.createSchemaFields(readAvroSchema("schema/complex_map.avsc"));
    List<SchemaField> expectedSchemaFields = Arrays.asList(
        createSchemaField("[version=2.0].mapOfString", toSchemaFieldDataType(Schema.Type.MAP), "map"),
        createSchemaField("[version=2.0].mapOfString.key", toSchemaFieldDataType(Schema.Type.STRING), "string"),
        createSchemaField("[version=2.0].mapOfString.value", toSchemaFieldDataType(Schema.Type.STRING), "string"),
        createSchemaField("[version=2.0].mapOfComplexType", toSchemaFieldDataType(Schema.Type.MAP), "map"),
        createSchemaField("[version=2.0].mapOfComplexType.key", toSchemaFieldDataType(Schema.Type.STRING), "string"),
        createSchemaField("[version=2.0].mapOfComplexType.value", toSchemaFieldDataType(Schema.Type.RECORD), "record"),
        createSchemaField("[version=2.0].mapOfComplexType.value.field1", toSchemaFieldDataType(Schema.Type.STRING), "string"),
        createSchemaField("[version=2.0].mapOfComplexType.value.field2", toSchemaFieldDataType(Schema.Type.INT), "int"),
        createSchemaField("[version=2.0].mapOfNullableString", toSchemaFieldDataType(Schema.Type.MAP), "map"),
        createSchemaField("[version=2.0].mapOfNullableString.key", toSchemaFieldDataType(Schema.Type.STRING), "string"),
        createSchemaField("[version=2.0].mapOfNullableString.value", toSchemaFieldDataType(Schema.Type.STRING), "string", true),
        createSchemaField("[version=2.0].mapOfNullableComplexType", toSchemaFieldDataType(Schema.Type.MAP), "map"),
        createSchemaField("[version=2.0].mapOfNullableComplexType.key", toSchemaFieldDataType(Schema.Type.STRING), "string"),
        createSchemaField("[version=2.0].mapOfNullableComplexType.value", toSchemaFieldDataType(Schema.Type.RECORD), "record", true),
        createSchemaField("[version=2.0].mapOfNullableComplexType.value.field1", toSchemaFieldDataType(Schema.Type.STRING), "string"),
        createSchemaField("[version=2.0].mapOfNullableComplexType.value.field2", toSchemaFieldDataType(Schema.Type.INT), "int"),
        createSchemaField("[version=2.0].mapOfArray", toSchemaFieldDataType(Schema.Type.MAP), "map"),
        createSchemaField("[version=2.0].mapOfArray.key", toSchemaFieldDataType(Schema.Type.STRING), "string"),
        createSchemaField("[version=2.0].mapOfArray.value", toSchemaFieldDataType(Schema.Type.ARRAY), "array"),
        createSchemaField("[version=2.0].mapOfArray.value.element", toSchemaFieldDataType(Schema.Type.STRING), "string"),
        createSchemaField("[version=2.0].mapOfMap", toSchemaFieldDataType(Schema.Type.MAP), "map"),
        createSchemaField("[version=2.0].mapOfMap.key", toSchemaFieldDataType(Schema.Type.STRING), "string"),
        createSchemaField("[version=2.0].mapOfMap.value", toSchemaFieldDataType(Schema.Type.MAP), "map"),
        createSchemaField("[version=2.0].mapOfMap.value.key", toSchemaFieldDataType(Schema.Type.STRING), "string"),
        createSchemaField("[version=2.0].mapOfMap.value.value", toSchemaFieldDataType(Schema.Type.INT), "int"),
        createSchemaField("[version=2.0].mapOfUnion", toSchemaFieldDataType(Schema.Type.MAP), "map"),
        createSchemaField("[version=2.0].mapOfUnion.key", toSchemaFieldDataType(Schema.Type.STRING), "string"),
        createSchemaField("[version=2.0].mapOfUnion.value", toSchemaFieldDataType(Schema.Type.UNION), "union", true),
        createSchemaField("[version=2.0].mapOfUnion.value.string", toSchemaFieldDataType(Schema.Type.STRING), "string"),
        createSchemaField("[version=2.0].mapOfUnion.value.int", toSchemaFieldDataType(Schema.Type.INT), "int")
    );

    assertEquals(expectedSchemaFields, schemaFields);
  }

  @Test
  void testComplexTypes_Array() throws IOException {
    List<SchemaField> schemaFields = DataHubSyncClient.createSchemaFields(readAvroSchema("schema/complex_array.avsc"));
    List<SchemaField> expectedSchemaFields = Arrays.asList(
        createSchemaField("[version=2.0].arrayOfString", toSchemaFieldDataType(Schema.Type.ARRAY), "array"),
        createSchemaField("[version=2.0].arrayOfString.element", toSchemaFieldDataType(Schema.Type.STRING), "string"),
        createSchemaField("[version=2.0].arrayOfMap", toSchemaFieldDataType(Schema.Type.ARRAY), "array"),
        createSchemaField("[version=2.0].arrayOfMap.element", toSchemaFieldDataType(Schema.Type.MAP), "map"),
        createSchemaField("[version=2.0].arrayOfMap.element.key", toSchemaFieldDataType(Schema.Type.STRING), "string"),
        createSchemaField("[version=2.0].arrayOfMap.element.value", toSchemaFieldDataType(Schema.Type.STRING), "string"),
        createSchemaField("[version=2.0].arrayOfRecord", toSchemaFieldDataType(Schema.Type.ARRAY), "array"),
        createSchemaField("[version=2.0].arrayOfRecord.element", toSchemaFieldDataType(Schema.Type.RECORD), "record"),
        createSchemaField("[version=2.0].arrayOfRecord.element.field1", toSchemaFieldDataType(Schema.Type.STRING), "string"),
        createSchemaField("[version=2.0].arrayOfRecord.element.field2", toSchemaFieldDataType(Schema.Type.INT), "int"),
        createSchemaField("[version=2.0].arrayOfArray", toSchemaFieldDataType(Schema.Type.ARRAY), "array"),
        createSchemaField("[version=2.0].arrayOfArray.element", toSchemaFieldDataType(Schema.Type.ARRAY), "array"),
        createSchemaField("[version=2.0].arrayOfArray.element.element", toSchemaFieldDataType(Schema.Type.STRING), "string"),
        createSchemaField("[version=2.0].arrayOfUnion", toSchemaFieldDataType(Schema.Type.ARRAY), "array"),
        createSchemaField("[version=2.0].arrayOfUnion.element", toSchemaFieldDataType(Schema.Type.UNION), "union"),
        createSchemaField("[version=2.0].arrayOfUnion.element.string", toSchemaFieldDataType(Schema.Type.STRING), "string"),
        createSchemaField("[version=2.0].arrayOfUnion.element.int", toSchemaFieldDataType(Schema.Type.INT), "int"),
        createSchemaField("[version=2.0].arrayOfUnion.element.boolean", toSchemaFieldDataType(Schema.Type.BOOLEAN), "boolean"),
        createSchemaField("[version=2.0].arrayOfNullableString", toSchemaFieldDataType(Schema.Type.ARRAY), "array"),
        createSchemaField("[version=2.0].arrayOfNullableString.element", toSchemaFieldDataType(Schema.Type.STRING), "string", true),
        createSchemaField("[version=2.0].arrayOfNullableRecord", toSchemaFieldDataType(Schema.Type.ARRAY), "array"),
        createSchemaField("[version=2.0].arrayOfNullableRecord.element", toSchemaFieldDataType(Schema.Type.RECORD), "record", true),
        createSchemaField("[version=2.0].arrayOfNullableRecord.element.field1", toSchemaFieldDataType(Schema.Type.STRING), "string"),
        createSchemaField("[version=2.0].arrayOfNullableRecord.element.field2", toSchemaFieldDataType(Schema.Type.INT), "int")
    );

    assertEquals(expectedSchemaFields, schemaFields);
  }

  @Test
  void testComplexTypes_Struct() throws IOException {
    List<SchemaField> schemaFields = DataHubSyncClient.createSchemaFields(readAvroSchema("schema/complex_struct.avsc"));
    List<SchemaField> expectedSchemaFields = Arrays.asList(
        createSchemaField("[version=2.0].structField", toSchemaFieldDataType(Schema.Type.RECORD), "record"),
        createSchemaField("[version=2.0].structField.fieldString", toSchemaFieldDataType(Schema.Type.STRING), "string"),
        createSchemaField("[version=2.0].structField.fieldInt", toSchemaFieldDataType(Schema.Type.INT), "int"),
        createSchemaField("[version=2.0].structField.fieldBoolean", toSchemaFieldDataType(Schema.Type.BOOLEAN), "boolean"),
        createSchemaField("[version=2.0].structField.fieldMap", toSchemaFieldDataType(Schema.Type.MAP), "map"),
        createSchemaField("[version=2.0].structField.fieldMap.key", toSchemaFieldDataType(Schema.Type.STRING), "string"),
        createSchemaField("[version=2.0].structField.fieldMap.value", toSchemaFieldDataType(Schema.Type.STRING), "string"),
        createSchemaField("[version=2.0].structField.fieldRecord", toSchemaFieldDataType(Schema.Type.RECORD), "record"),
        createSchemaField("[version=2.0].structField.fieldRecord.nestedField1", toSchemaFieldDataType(Schema.Type.STRING), "string"),
        createSchemaField("[version=2.0].structField.fieldRecord.nestedField2", toSchemaFieldDataType(Schema.Type.INT), "int"),
        createSchemaField("[version=2.0].structField.fieldArray", toSchemaFieldDataType(Schema.Type.ARRAY), "array"),
        createSchemaField("[version=2.0].structField.fieldArray.element", toSchemaFieldDataType(Schema.Type.STRING), "string"),
        createSchemaField("[version=2.0].structField.fieldUnion", toSchemaFieldDataType(Schema.Type.UNION), "union", true),
        createSchemaField("[version=2.0].structField.fieldUnion.string", toSchemaFieldDataType(Schema.Type.STRING), "string"),
        createSchemaField("[version=2.0].structField.fieldUnion.int", toSchemaFieldDataType(Schema.Type.INT), "int"),
        createSchemaField("[version=2.0].structField.fieldNullableMap", toSchemaFieldDataType(Schema.Type.MAP), "map", true),
        createSchemaField("[version=2.0].structField.fieldNullableMap.key", toSchemaFieldDataType(Schema.Type.STRING), "string"),
        createSchemaField("[version=2.0].structField.fieldNullableMap.value", toSchemaFieldDataType(Schema.Type.STRING), "string")
    );

    assertEquals(expectedSchemaFields, schemaFields);
  }

  @Test
  void testComplexTypes_Union() throws IOException {
    List<SchemaField> schemaFields = DataHubSyncClient.createSchemaFields(readAvroSchema("schema/complex_union.avsc"));
    List<SchemaField> expectedSchemaFields = Arrays.asList(
        createSchemaField("[version=2.0].fieldUnionNullablePrimitives", toSchemaFieldDataType(Schema.Type.UNION), "union", true),
        createSchemaField("[version=2.0].fieldUnionNullablePrimitives.string", toSchemaFieldDataType(Schema.Type.STRING), "string"),
        createSchemaField("[version=2.0].fieldUnionNullablePrimitives.int", toSchemaFieldDataType(Schema.Type.INT), "int"),
        createSchemaField("[version=2.0].fieldUnionNullablePrimitives.boolean", toSchemaFieldDataType(Schema.Type.BOOLEAN), "boolean"),
        createSchemaField("[version=2.0].fieldUnionComplexTypes", toSchemaFieldDataType(Schema.Type.UNION), "union", true),
        createSchemaField("[version=2.0].fieldUnionComplexTypes.NestedRecord", toSchemaFieldDataType(Schema.Type.RECORD), "record"),
        createSchemaField("[version=2.0].fieldUnionComplexTypes.NestedRecord.nestedField1", toSchemaFieldDataType(Schema.Type.STRING), "string"),
        createSchemaField("[version=2.0].fieldUnionComplexTypes.NestedRecord.nestedField2", toSchemaFieldDataType(Schema.Type.INT), "int"),
        createSchemaField("[version=2.0].fieldUnionComplexTypes.map", toSchemaFieldDataType(Schema.Type.MAP), "map"),
        createSchemaField("[version=2.0].fieldUnionComplexTypes.map.key", toSchemaFieldDataType(Schema.Type.STRING), "string"),
        createSchemaField("[version=2.0].fieldUnionComplexTypes.map.value", toSchemaFieldDataType(Schema.Type.STRING), "string"),
        createSchemaField("[version=2.0].fieldUnionPrimitiveAndComplex", toSchemaFieldDataType(Schema.Type.UNION), "union", true),
        createSchemaField("[version=2.0].fieldUnionPrimitiveAndComplex.string", toSchemaFieldDataType(Schema.Type.STRING), "string"),
        createSchemaField("[version=2.0].fieldUnionPrimitiveAndComplex.ComplexTypeRecord", toSchemaFieldDataType(Schema.Type.RECORD), "record"),
        createSchemaField("[version=2.0].fieldUnionPrimitiveAndComplex.ComplexTypeRecord.complexField1", toSchemaFieldDataType(Schema.Type.STRING), "string"),
        createSchemaField("[version=2.0].fieldUnionPrimitiveAndComplex.ComplexTypeRecord.complexField2", toSchemaFieldDataType(Schema.Type.INT), "int")
    );

    assertEquals(expectedSchemaFields, schemaFields);
  }

  private Schema readAvroSchema(String schemaFileName) throws IOException {
    String schemaPath = getClass().getClassLoader().getResource(schemaFileName).getPath();
    File schemaFile = new File(schemaPath);
    return new Schema.Parser().parse(schemaFile);
  }
}
