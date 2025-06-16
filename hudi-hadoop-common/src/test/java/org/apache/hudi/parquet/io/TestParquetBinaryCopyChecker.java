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

package org.apache.hudi.parquet.io;

import org.apache.hudi.parquet.io.ParquetBinaryCopyChecker.ParquetFileInfo;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.hudi.avro.HoodieBloomFilterWriteSupport.HOODIE_BLOOM_FILTER_TYPE_CODE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Types.optional;
import static org.apache.parquet.schema.Types.optionalGroup;
import static org.apache.parquet.schema.Types.optionalList;
import static org.apache.parquet.schema.Types.optionalMap;
import static org.apache.parquet.schema.Types.repeated;
import static org.apache.parquet.schema.Types.required;
import static org.apache.parquet.schema.Types.requiredGroup;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestParquetBinaryCopyChecker {

  private Configuration conf = new Configuration();
  private final int numRecord = 0;

  private static final PrimitiveType BINARY_TYPE = optional(BINARY).named("Binary");
  private static final PrimitiveType INT32_TYPE = optional(INT32).named("Int32");
  private static final PrimitiveType INT64_TYPE = optional(INT64).named("Int64");
  private static final PrimitiveType INT96_TYPE = optional(INT96).named("Int96");
  private static final PrimitiveType FLOAT_TYPE = optional(FLOAT).named("Float");
  private static final PrimitiveType DOUBLE_TYPE = optional(DOUBLE).named("DOUBLE");
  private static final PrimitiveType BOOLEAN_TYPE = optional(BOOLEAN).named("BOOLEAN");
  private static final PrimitiveType FIXED_LEN_BYTE_ARRAY_TYPE = optional(FIXED_LEN_BYTE_ARRAY)
      .length(20)
      .named("FIXED_LEN_BYTE_ARRAY_TYPE");

  private static final GroupType LEGACY_TWO_LEVEL_ARRAY_TYPE = optionalGroup()
      .as(OriginalType.LIST)
      .addField(repeated(BINARY).named("array"))
      .named("LEGACY_TWO_LEVEL_ARRAY_TYPE");
  private static final GroupType LEGACY_TREE_LEVEL_ARRAY_TYPE = optionalGroup()
      .as(OriginalType.LIST)
      .optionalGroup()
      .addField(optional(BINARY).named("array"))
      .named("bag")
      .named("LEGACY_TREE_LEVEL_ARRAY_TYPE");
  private static final GroupType STANDARD_THREE_LEVEL_ARRAY_TYPE = optionalList()
      .element(BINARY, OPTIONAL)
      .named("STANDARD_THREE_LEVEL_ARRAY_TYPE");

  private static final GroupType LEGACY_MAP_TYPE = optionalGroup()
      .as(OriginalType.MAP)
      .optionalGroup()
      .addField(required(BINARY).named("key"))
      .addField(optional(BINARY).named("value"))
      .named("map")
      .named("LEGACY_MAP_TYPE");
  private static final GroupType STANDARD_MAP_TYPE = optionalMap()
      .key(BINARY)
      .value(BINARY, OPTIONAL)
      .named("STANDARD_MAP_TYPE");

  private static final PrimitiveType INT32_DECIMAL_TYPE = optional(INT32)
      .as(LogicalTypeAnnotation.decimalType(2, 5))
      .named("INT32_DECIMAL_TYPE");
  private static final PrimitiveType INT64_DECIMAL_TYPE = optional(INT64)
      .as(LogicalTypeAnnotation.decimalType(2, 15))
      .named("INT64_DECIMAL_TYPE");
  private static final PrimitiveType FIXED_LEN_BYTE_ARRAY_DECIMAL_TYPE = optional(FIXED_LEN_BYTE_ARRAY)
      .length(25)
      .as(LogicalTypeAnnotation.decimalType(2, 25))
      .named("FIXED_LEN_BYTE_ARRAY_DECIMAL_TYPE");

  @Test
  public void testVerifyFilesBySupport() {
    ParquetFileInfo file = makeSchemaNotSupportFileInfo();
    // file schema not support, should return false
    List<ParquetFileInfo> files = makeFileInfos(file);
    assertFalse(ParquetBinaryCopyChecker.verifyFiles(files));
  }

  @Test
  public void testVerifyFilesByFilterCode() {
    List<ParquetFileInfo> files;
    String schema = Types.buildMessage()
        .addField(required(BINARY).named("field1"))
        .addField(optional(BINARY).named("field2"))
        .addField(repeated(BINARY).named("field3"))
        .named("schema")
        .toString();
    ParquetFileInfo file1 = makeFileInfo(true, null, null);
    ParquetFileInfo file2 = makeFileInfo(true, "simple", schema);
    ParquetFileInfo file3 = makeFileInfo(true, "dynamic", schema);

    // file1 schema contains none bloom filter, file2 contains simple bloom filter, should return false
    files = makeFileInfos(file1, file2);
    assertFalse(ParquetBinaryCopyChecker.verifyFiles(files));

    // files contains multip bloom filter, should return false
    files = makeFileInfos(file2, file3);
    assertFalse(ParquetBinaryCopyChecker.verifyFiles(files));
  }

  @Test
  public void testVerifyFilesByRepetition() {
    List<ParquetFileInfo> files;
    String schema = Types.buildMessage()
        .addField(required(BINARY).named("field1"))
        .addField(optionalList().requiredElement(BINARY).named("list-simple"))
        .addField(optionalList().optionalGroupElement().addField(optional(INT32).named("ele")).named("list-nested"))
        .addField(optionalMap()
            .key(required(BINARY).named("key"))
            .value(optional(BINARY).named("value"))
            .named("map-simple"))
        .addField(optionalMap()
            .key(required(BINARY).named("key"))
            .value(optionalGroup().addField(optional(BINARY).named("map-ele")).named("value"))
            .named("map-nested"))
        .addField(optionalGroup().addField(optional(BINARY).named("struct-field1")).named("struct-simple"))
        .addField(optionalGroup().addField(
                optionalGroup().addField(optional(INT64).named("struct-field2-int")).named("struct-field2"))
            .named("struct-nested"))
        .named("schema")
        .toString();
    ParquetFileInfo file1 = makeFileInfo(true, "dynamic", schema);

    schema = Types.buildMessage()
        .addField(optional(BINARY).named("field1"))
        .addField(optionalList().requiredElement(BINARY).named("list-simple"))
        .addField(optionalList().optionalGroupElement().addField(optional(INT32).named("ele")).named("list-nested"))
        .addField(optionalMap()
            .key(required(BINARY).named("key"))
            .value(optional(BINARY).named("value"))
            .named("map-simple"))
        .addField(optionalMap()
            .key(required(BINARY).named("key"))
            .value(optionalGroup().addField(optional(BINARY).named("map-ele")).named("value"))
            .named("map-nested"))
        .addField(optionalGroup().addField(optional(BINARY).named("struct-field1")).named("struct-simple"))
        .addField(optionalGroup().addField(
                optionalGroup().addField(optional(INT64).named("struct-field2-int")).named("struct-field2"))
            .named("struct-nested"))
        .named("schema")
        .toString();
    ParquetFileInfo file2 = makeFileInfo(true, "dynamic", schema);

    schema = Types.buildMessage()
        .addField(required(BINARY).named("field1"))
        .addField(optionalList().requiredElement(BINARY).named("list-simple"))
        .addField(optionalList().requiredGroupElement().addField(optional(INT32).named("ele")).named("list-nested"))
        .addField(optionalMap()
            .key(required(BINARY).named("key"))
            .value(optional(BINARY).named("value"))
            .named("map-simple"))
        .addField(optionalMap()
            .key(required(BINARY).named("key"))
            .value(optionalGroup().addField(optional(BINARY).named("map-ele")).named("value"))
            .named("map-nested"))
        .addField(optionalGroup().addField(optional(BINARY).named("struct-field1")).named("struct-simple"))
        .addField(optionalGroup().addField(
                optionalGroup().addField(optional(INT64).named("struct-field2-int")).named("struct-field2"))
            .named("struct-nested"))
        .named("schema")
        .toString();
    ParquetFileInfo file3 = makeFileInfo(true, "dynamic", schema);

    schema = Types.buildMessage()
        .addField(required(BINARY).named("field1"))
        .addField(optionalList().requiredElement(BINARY).named("list-simple"))
        .addField(optionalList().optionalGroupElement().addField(optional(INT32).named("ele")).named("list-nested"))
        .addField(optionalMap()
            .key(required(BINARY).named("key"))
            .value(optional(BINARY).named("value"))
            .named("map-simple"))
        .addField(optionalMap()
            .key(required(BINARY).named("key"))
            .value(requiredGroup().addField(optional(BINARY).named("map-ele")).named("value"))
            .named("map-nested"))
        .addField(optionalGroup().addField(optional(BINARY).named("struct-field1")).named("struct-simple"))
        .addField(optionalGroup().addField(
                optionalGroup().addField(optional(INT64).named("struct-field2-int")).named("struct-field2"))
            .named("struct-nested"))
        .named("schema")
        .toString();
    ParquetFileInfo file4 = makeFileInfo(true, "dynamic", schema);

    schema = Types.buildMessage()
        .addField(required(BINARY).named("field1"))
        .addField(optionalList().requiredElement(BINARY).named("list-simple"))
        .addField(optionalList().optionalGroupElement().addField(optional(INT32).named("ele")).named("list-nested"))
        .addField(optionalMap()
            .key(required(BINARY).named("key"))
            .value(optional(BINARY).named("value"))
            .named("map-simple"))
        .addField(optionalMap()
            .key(required(BINARY).named("key"))
            .value(optionalGroup().addField(optional(BINARY).named("map-ele")).named("value"))
            .named("map-nested"))
        .addField(optionalGroup().addField(optional(BINARY).named("struct-field1")).named("struct-simple"))
        .addField(optionalGroup().addField(
                optionalGroup().addField(required(INT64).named("struct-field2-int")).named("struct-field2"))
            .named("struct-nested"))
        .named("schema")
        .toString();
    ParquetFileInfo file5 = makeFileInfo(true, "dynamic", schema);

    schema = Types.buildMessage()
        .addField(required(BINARY).named("field1"))
        .addField(optionalList().requiredElement(BINARY).named("list-simple"))
        .addField(optionalList().optionalGroupElement().addField(optional(INT32).named("ele")).named("list-nested"))
        .addField(optionalMap()
            .key(required(BINARY).named("key"))
            .value(optional(BINARY).named("value"))
            .named("map-simple"))
        .addField(optionalMap()
            .key(required(BINARY).named("key"))
            .value(optionalGroup().addField(optional(BINARY).named("map-ele")).named("value"))
            .named("map-nested"))
        .addField(optionalGroup().addField(optional(BINARY).named("struct-field1")).named("struct-simple"))
        .addField(optionalGroup().addField(
                optionalGroup().addField(optional(INT64).named("struct-field2-int")).named("struct-field2"))
            .named("struct-nested"))
        .addField(optional(BINARY).named("field-added"))
        .named("schema")
        .toString();
    ParquetFileInfo file6 = makeFileInfo(true, "dynamic", schema);
    ParquetFileInfo file7 = makeFileInfo(true, "dynamic", schema);

    // files contains multip repetition for binary column, should return false
    files = makeFileInfos(file1, file2);
    assertFalse(ParquetBinaryCopyChecker.verifyFiles(files));

    // files contains multip repetition for nested list column, should return false
    files = makeFileInfos(file1, file3);
    assertFalse(ParquetBinaryCopyChecker.verifyFiles(files));

    // files contains multip repetition for nested map column, should return false
    files = makeFileInfos(file1, file4);
    assertFalse(ParquetBinaryCopyChecker.verifyFiles(files));

    // files contains multip repetition for nested struct column, should return false
    files = makeFileInfos(file1, file5);
    assertFalse(ParquetBinaryCopyChecker.verifyFiles(files));

    // files contains schema evolution, should return true
    files = makeFileInfos(file1, file6);
    assertTrue(ParquetBinaryCopyChecker.verifyFiles(files));

    // all same, should return true
    files = makeFileInfos(file6, file7);
    assertTrue(ParquetBinaryCopyChecker.verifyFiles(files));
  }

  @Test
  public void testVerifyFile() throws IOException {
    MessageType schema = new MessageType("schema",
        BINARY_TYPE,
        INT32_TYPE,
        INT64_TYPE,
        INT96_TYPE,
        FLOAT_TYPE,
        DOUBLE_TYPE,
        BOOLEAN_TYPE,
        FIXED_LEN_BYTE_ARRAY_TYPE);
    String testFile = makeTestFile(schema, "simple");
    ParquetFileInfo info = ParquetBinaryCopyChecker.collectFileInfo(conf, testFile);
    Assertions.assertNotNull(info);
    Assertions.assertTrue(info.canBinaryCopy());
    String schemaString = schema.toString();
    assertFileInfo(
        info,
        true,
        "simple", schemaString);
    TestHoodieParquetFileBinaryCopier.TestFileBuilder.deleteTempFile(testFile);

    schema = new MessageType("schema", LEGACY_TWO_LEVEL_ARRAY_TYPE);
    testSingleFile(schema,false);
    schema = new MessageType("schema", LEGACY_TREE_LEVEL_ARRAY_TYPE);
    testSingleFile(schema,true);
    schema = new MessageType("schema", STANDARD_THREE_LEVEL_ARRAY_TYPE);
    testSingleFile(schema,true);

    schema = new MessageType("schema", LEGACY_MAP_TYPE);
    testSingleFile(schema,true);
    schema = new MessageType("schema", STANDARD_MAP_TYPE);
    testSingleFile(schema,true);

    schema = new MessageType("schema", INT32_DECIMAL_TYPE);
    testSingleFile(schema,false);
    schema = new MessageType("schema", INT64_DECIMAL_TYPE);
    testSingleFile(schema,false);
    schema = new MessageType("schema", FIXED_LEN_BYTE_ARRAY_DECIMAL_TYPE);
    testSingleFile(schema,true);
  }

  private List<ParquetFileInfo> makeFileInfos(ParquetFileInfo... infos) {
    return Arrays.stream(infos).collect(Collectors.toList());
  }

  private ParquetFileInfo makeSchemaNotSupportFileInfo() {
    return makeFileInfo(false, null, null);
  }

  private ParquetFileInfo makeFileInfo(boolean schemaSupport, String typeCode, String schema) {
    return new ParquetFileInfo(schemaSupport, typeCode, schema);
  }

  private String makeTestFile(MessageType schema) throws IOException {
    return makeTestFile(schema, null);
  }

  private String makeTestFile(MessageType schema, String typeCode) throws IOException {
    Map<String, String> meta = new HashMap<>();
    meta.put(HOODIE_BLOOM_FILTER_TYPE_CODE, typeCode);
    return new TestHoodieParquetFileBinaryCopier.TestFileBuilder(conf, schema)
        .withNumRecord(numRecord)
        .withExtraMeta(meta)
        .withPageSize(ParquetProperties.DEFAULT_PAGE_SIZE)
        .build()
        .getFileName();
  }

  private void testSingleFile(MessageType schema, String inputTypeCode, boolean support, String typeCode) throws IOException {
    withTempFile(makeTestFile(schema, inputTypeCode), file -> {
      assertFileInfo(ParquetBinaryCopyChecker.collectFileInfo(conf, file), support, typeCode, schema.toString());
    });
  }

  private void testSingleFile(MessageType schema, boolean support) throws IOException {
    testSingleFile(schema, null, support, null);
  }

  private void withTempFile(String file, Consumer<String> run) {
    try {
      run.accept(file);
    } finally {
      TestHoodieParquetFileBinaryCopier.TestFileBuilder.deleteTempFile(file);
    }
  }

  private void assertFileInfo(ParquetFileInfo info, boolean support, String codeType, String schema) {
    Assertions.assertNotNull(info);
    Assertions.assertEquals(support, info.canBinaryCopy());
    if (support) {
      Assertions.assertEquals(codeType, info.getBloomFilterTypeCode());
      Assertions.assertEquals(schema, info.getSchema().toString());
    }
  }
}
