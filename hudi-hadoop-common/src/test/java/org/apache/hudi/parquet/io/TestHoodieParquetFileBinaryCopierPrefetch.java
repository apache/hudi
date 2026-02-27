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

import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.util.HoodieFileMetadataMerger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieParquetFileBinaryCopierPrefetch {

  private Configuration conf;
  private MessageType schema;
  private List<TestHoodieParquetFileBinaryCopier.TestFile> inputFiles;
  private String outputFile;

  @BeforeEach
  public void setUp() {
    conf = new Configuration();
    schema = Types.buildMessage()
        .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, PrimitiveType.Repetition.REQUIRED).named("field1"))
        .named("TestRecord");
    inputFiles = new ArrayList<>();
    outputFile = TestHoodieParquetFileBinaryCopier.TestFileBuilder.createTempFile("test_out");
  }

  @AfterEach
  public void tearDown() {
    if (outputFile != null) {
      TestHoodieParquetFileBinaryCopier.TestFileBuilder.deleteTempFile(outputFile);
    }
    if (inputFiles != null) {
      inputFiles.stream().map(TestHoodieParquetFileBinaryCopier.TestFile::getFileName)
          .forEach(TestHoodieParquetFileBinaryCopier.TestFileBuilder::deleteTempFile);
    }
  }

  @Test
  public void testPrefetchingMultipleFiles() throws IOException {
    // Create multiple small files
    int numFiles = 5;
    for (int i = 0; i < numFiles; i++) {
      inputFiles.add(new TestHoodieParquetFileBinaryCopier.TestFileBuilder(conf, schema)
          .withNumRecord(100)
          .build());
    }

    List<StoragePath> inputPaths = inputFiles.stream()
        .map(f -> new StoragePath(f.getFileName()))
        .collect(Collectors.toList());

    HoodieFileMetadataMerger merger = new HoodieFileMetadataMerger();
    try (HoodieParquetFileBinaryCopier copier = new HoodieParquetFileBinaryCopier(conf, CompressionCodecName.UNCOMPRESSED, merger)) {
      assertDoesNotThrow(() -> copier.binaryCopy(inputPaths, Collections.singletonList(new StoragePath(outputFile)), schema, true));
    }

    assertTrue(Files.exists(Paths.get(outputFile)));
    assertTrue(Files.size(Paths.get(outputFile)) > 0);

    // Verify row count in output equals sum of rows in input files
    int expectedTotalRows = inputFiles.stream().mapToInt(f -> f.getFileContent().length).sum();
    int rowsRead = 0;
    Path outputPath = new Path(outputFile);
    try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), outputPath).withConf(conf).build()) {
      while (reader.read() != null) {
        rowsRead++;
      }
    }

    assertEquals(expectedTotalRows, rowsRead, "Output row count should match sum of input rows");
  }

  @Test
  public void testPrefetchingSingleFile() throws IOException {
    inputFiles.add(new TestHoodieParquetFileBinaryCopier.TestFileBuilder(conf, schema)
        .withNumRecord(100)
        .build());

    List<StoragePath> inputPaths = inputFiles.stream()
        .map(f -> new StoragePath(f.getFileName()))
        .collect(Collectors.toList());

    HoodieFileMetadataMerger merger = new HoodieFileMetadataMerger();
    try (HoodieParquetFileBinaryCopier copier = new HoodieParquetFileBinaryCopier(conf, CompressionCodecName.UNCOMPRESSED, merger)) {
      assertDoesNotThrow(() -> copier.binaryCopy(inputPaths, Collections.singletonList(new StoragePath(outputFile)), schema, true));
    }

    assertTrue(Files.exists(Paths.get(outputFile)));

    // Verify row count in output equals rows in single input file
    int expectedTotalRows = inputFiles.get(0).getFileContent().length;
    int rowsRead = 0;
    Path outputPath = new Path(outputFile);
    try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), outputPath).withConf(conf).build()) {
      Group group;
      while ((group = reader.read()) != null) {
        rowsRead++;
      }
    }

    assertEquals(expectedTotalRows, rowsRead, "Output row count should match input rows");
  }
}
