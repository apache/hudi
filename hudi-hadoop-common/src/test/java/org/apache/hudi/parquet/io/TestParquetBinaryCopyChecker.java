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
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestParquetBinaryCopyChecker {

  private Configuration conf = new Configuration();
  private final int numRecord = 0;

  private static final PrimitiveType BINARY_TYPE = Types.optional(BINARY).named("Binary");
  private static final PrimitiveType INT32_TYPE = Types.optional(INT32).named("Int32");
  private static final PrimitiveType INT64_TYPE = Types.optional(INT64).named("Int64");
  private static final PrimitiveType INT96_TYPE = Types.optional(INT96).named("Int96");
  private static final PrimitiveType FLOAT_TYPE = Types.optional(FLOAT).named("Float");
  private static final PrimitiveType DOUBLE_TYPE = Types.optional(DOUBLE).named("DOUBLE");
  private static final PrimitiveType BOOLEAN_TYPE = Types.optional(BOOLEAN).named("BOOLEAN");
  private static final PrimitiveType FIXED_LEN_BYTE_ARRAY_TYPE = Types.optional(FIXED_LEN_BYTE_ARRAY)
      .length(20)
      .named("FIXED_LEN_BYTE_ARRAY_TYPE");

  private static final GroupType LEGACY_TWO_LEVEL_ARRAY_TYPE = Types.optionalGroup()
      .as(OriginalType.LIST)
      .addField(Types.repeated(BINARY).named("array"))
      .named("LEGACY_TWO_LEVEL_ARRAY_TYPE");
  private static final GroupType LEGACY_TREE_LEVEL_ARRAY_TYPE = Types.optionalGroup()
      .as(OriginalType.LIST)
      .optionalGroup()
      .addField(Types.optional(BINARY).named("array"))
      .named("bag")
      .named("LEGACY_TREE_LEVEL_ARRAY_TYPE");
  private static final GroupType STANDARD_THREE_LEVEL_ARRAY_TYPE = Types.optionalList()
      .element(BINARY, OPTIONAL)
      .named("STANDARD_THREE_LEVEL_ARRAY_TYPE");

  private static final GroupType LEGACY_MAP_TYPE = Types.optionalGroup()
      .as(OriginalType.MAP)
      .optionalGroup()
      .addField(Types.required(BINARY).named("key"))
      .addField(Types.optional(BINARY).named("value"))
      .named("map")
      .named("LEGACY_MAP_TYPE");
  private static final GroupType STANDARD_MAP_TYPE = Types.optionalMap()
      .key(BINARY)
      .value(BINARY, OPTIONAL)
      .named("STANDARD_MAP_TYPE");


  private static final PrimitiveType INT32_DECIMAL_TYPE = Types.optional(INT32)
      .as(LogicalTypeAnnotation.decimalType(2, 5))
      .named("INT32_DECIMAL_TYPE");
  private static final PrimitiveType INT64_DECIMAL_TYPE = Types.optional(INT64)
      .as(LogicalTypeAnnotation.decimalType(2, 15))
      .named("INT64_DECIMAL_TYPE");
  private static final PrimitiveType FIXED_LEN_BYTE_ARRAY_DECIMAL_TYPE = Types.optional(FIXED_LEN_BYTE_ARRAY)
      .length(25)
      .as(LogicalTypeAnnotation.decimalType(2, 25))
      .named("FIXED_LEN_BYTE_ARRAY_DECIMAL_TYPE");

  @Test
  public void testVerifyFiles() {
    List<ParquetFileInfo> files;

    ParquetFileInfo file0 = makeSchemaNotSupportFileInfo();
    ParquetFileInfo file1 = makeFileInfo(true, null);
    ParquetFileInfo file2 = makeFileInfo(
        true,
        "simple",
        "field1", "required",
        "field2", "optional",
        "field2", "repeated"
    );
    ParquetFileInfo file3 = makeFileInfo(
        true,
        "dynamic",
        "field1", "required",
        "field2", "optional",
        "field3", "repeated"
    );
    ParquetFileInfo file4 = makeFileInfo(
        true,
        "dynamic",
        "field1", "optional",
        "field2", "optional",
        "field3", "optional"
    );
    ParquetFileInfo file5 = makeFileInfo(
        true,
        "dynamic",
        "field1", "optional",
        "field2", "optional",
        "field3", "optional",
        "field4", "optional"
    );
    ParquetFileInfo file6 = makeFileInfo(
        true,
        "dynamic",
        "field1", "optional",
        "field2", "optional",
        "field3", "optional",
        "field4", "optional"
    );

    // file0 schema not support, should return false
    files = makeFileInfos(file0);
    Assertions.assertFalse(ParquetBinaryCopyChecker.verifyFiles(files));

    // file1 schema contains none bloom filter, file2 contains simple bloom filter, should return false
    files = makeFileInfos(file1, file2);
    Assertions.assertFalse(ParquetBinaryCopyChecker.verifyFiles(files));

    // files contains multip bloom filter, should return false
    files = makeFileInfos(file2, file3);
    Assertions.assertFalse(ParquetBinaryCopyChecker.verifyFiles(files));

    // files contains multip repetition for some column, should return false
    files = makeFileInfos(file3, file4);
    Assertions.assertFalse(ParquetBinaryCopyChecker.verifyFiles(files));

    // files contains schema evolution, should return true
    files = makeFileInfos(file4, file5);
    assertTrue(ParquetBinaryCopyChecker.verifyFiles(files));

    // all same, should return true
    files = makeFileInfos(file5, file6);
    assertTrue(ParquetBinaryCopyChecker.verifyFiles(files));
  }

  @Test
  public void testVerifyFile() throws IOException {
    MessageType schema = new MessageType("schema",
        BINARY_TYPE,
        INT32_TYPE,
        INT96_TYPE,
        FLOAT_TYPE,
        DOUBLE_TYPE,
        BOOLEAN_TYPE,
        FIXED_LEN_BYTE_ARRAY_TYPE);
    String testFile = makeTestFile(schema, "simple");
    ParquetFileInfo info = ParquetBinaryCopyChecker.verifyFile(conf, testFile);
    Assertions.assertNotNull(info);
    Assertions.assertTrue(info.isSchemaSupport());
    assertFileInfo(
        info,
        true,
        "simple",
        "Float", "OPTIONAL",
        "Int96", "OPTIONAL",
        "Int32", "OPTIONAL",
        "FIXED_LEN_BYTE_ARRAY_TYPE", "OPTIONAL",
        "DOUBLE", "OPTIONAL",
        "Binary", "OPTIONAL",
        "BOOLEAN", "OPTIONAL");
    TestHoodieParquetFileBinaryCopier.TestFileBuilder.deleteTempFile(testFile);

    schema = new MessageType("schema", LEGACY_TWO_LEVEL_ARRAY_TYPE);
    withTempFile(makeTestFile(schema), file -> {
      assertFileInfo(ParquetBinaryCopyChecker.verifyFile(conf, file), false);
    });
    schema = new MessageType("schema", LEGACY_TREE_LEVEL_ARRAY_TYPE);
    withTempFile(makeTestFile(schema), file -> {
      assertFileInfo(ParquetBinaryCopyChecker.verifyFile(conf, file), true, null, "LEGACY_TREE_LEVEL_ARRAY_TYPE", "OPTIONAL");
    });
    schema = new MessageType("schema", STANDARD_THREE_LEVEL_ARRAY_TYPE);
    withTempFile(makeTestFile(schema), file -> {
      assertFileInfo(ParquetBinaryCopyChecker.verifyFile(conf, file), true, null, "STANDARD_THREE_LEVEL_ARRAY_TYPE", "OPTIONAL");
    });

    schema = new MessageType("schema", LEGACY_MAP_TYPE);
    withTempFile(makeTestFile(schema), file -> {
      assertFileInfo(ParquetBinaryCopyChecker.verifyFile(conf, file), true, null, "LEGACY_MAP_TYPE", "OPTIONAL");
    });
    schema = new MessageType("schema", STANDARD_MAP_TYPE);
    withTempFile(makeTestFile(schema), file -> {
      assertFileInfo(ParquetBinaryCopyChecker.verifyFile(conf, file), true, null, "STANDARD_MAP_TYPE", "OPTIONAL");
    });

    schema = new MessageType("schema", INT32_DECIMAL_TYPE);
    withTempFile(makeTestFile(schema), file -> {
      assertFileInfo(ParquetBinaryCopyChecker.verifyFile(conf, file), false);
    });
    schema = new MessageType("schema", INT64_DECIMAL_TYPE);
    withTempFile(makeTestFile(schema), file -> {
      assertFileInfo(ParquetBinaryCopyChecker.verifyFile(conf, file), false);
    });
    schema = new MessageType("schema", FIXED_LEN_BYTE_ARRAY_DECIMAL_TYPE);
    withTempFile(makeTestFile(schema), file -> {
      assertFileInfo(ParquetBinaryCopyChecker.verifyFile(conf, file), true, null, "FIXED_LEN_BYTE_ARRAY_DECIMAL_TYPE", "OPTIONAL");
    });
  }

  private List<ParquetFileInfo> makeFileInfos(ParquetFileInfo... infos) {
    return Arrays.stream(infos).collect(Collectors.toList());
  }

  private ParquetFileInfo makeSchemaNotSupportFileInfo() {
    return makeFileInfo(false, null);
  }

  private ParquetFileInfo makeFileInfo(boolean schemaSupport, String typeCode, String... repetitions) {
    assertTrue(repetitions.length % 2 == 0);
    Map<String, String> repetition = new HashMap<>();
    for (int i = 0; i < repetitions.length; i += 2) {
      repetition.put(repetitions[i], repetitions[i + 1]);
    }
    return new ParquetFileInfo(schemaSupport, typeCode, repetition);
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

  private void withTempFile(String file, Consumer<String> run) {
    try {
      run.accept(file);
    } finally {
      TestHoodieParquetFileBinaryCopier.TestFileBuilder.deleteTempFile(file);
    }
  }

  private void assertFileInfo(ParquetFileInfo info, boolean support) {
    assertFileInfo(info, support, null);
  }

  private void assertFileInfo(ParquetFileInfo info, boolean support, String codeType, String... repetitions) {
    Assertions.assertNotNull(info);
    Assertions.assertEquals(support, info.isSchemaSupport());
    Assertions.assertEquals(codeType, info.getBloomFilterTypeCode());
    assertTrue(repetitions.length % 2 == 0);
    Map<String, String> repetition = new HashMap<>();
    for (int i = 0; i < repetitions.length; i += 2) {
      repetition.put(repetitions[i], repetitions[i + 1]);
    }
    Assertions.assertEquals(repetition, info.getRepetitions());
  }
}
