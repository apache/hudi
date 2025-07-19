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

package org.apache.hudi.parquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link HoodieParquetStrictMerge}.
 */
public class HoodieParquetStrictMergeTest {

  private Configuration conf;
  private FileSystem fs;
  
  @TempDir
  java.nio.file.Path tempDir;

  @BeforeEach
  public void setUp() throws IOException {
    conf = new Configuration();
    fs = FileSystem.get(conf);
  }

  /**
   * Helper method to create a Parquet file with test data.
   *
   * @param filePath Path where the Parquet file will be created
   * @param schema Avro schema for the records
   * @param records List of records to write to the file
   * @param config Hadoop configuration (optional, uses default if null)
   * @param compressionCodec Compression codec (optional, uses SNAPPY if null)
   * @throws IOException if file writing fails
   */
  private void createParquetFile(Path filePath, Schema schema, List<GenericRecord> records,
                                 Configuration config, CompressionCodecName compressionCodec) throws IOException {
    Configuration confToUse = config != null ? config : this.conf;
    CompressionCodecName codecToUse = compressionCodec != null ? compressionCodec : CompressionCodecName.SNAPPY;
    
    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(filePath)
        .withSchema(schema)
        .withConf(confToUse)
        .withCompressionCodec(codecToUse)
        .build()) {
      
      for (GenericRecord record : records) {
        writer.write(record);
      }
    }
  }

  /**
   * Helper method to create a Parquet file with test data using default configuration.
   *
   * @param filePath Path where the Parquet file will be created
   * @param schema Avro schema for the records
   * @param records List of records to write to the file
   * @throws IOException if file writing fails
   */
  private void createParquetFile(Path filePath, Schema schema, List<GenericRecord> records) throws IOException {
    createParquetFile(filePath, schema, records, null, null);
  }

  @Test
  public void testMergeFilesWithIdenticalSchemas() throws IOException {
    // Create test schema
    Schema schema = Schema.createRecord("TestRecord", null, null, false);
    List<Schema.Field> fields = new ArrayList<>();
    fields.add(new Schema.Field("id", Schema.create(Schema.Type.INT), null, null));
    fields.add(new Schema.Field("name", Schema.create(Schema.Type.STRING), null, null));
    fields.add(new Schema.Field("age", Schema.create(Schema.Type.INT), null, null));
    schema.setFields(fields);

    // Create test input files
    List<Path> inputFiles = new ArrayList<>();
    int numFiles = 3;
    int recordsPerFile = 10;
    
    for (int fileIdx = 0; fileIdx < numFiles; fileIdx++) {
      Path inputFile = new Path(tempDir.toString(), "input" + fileIdx + ".parquet");
      inputFiles.add(inputFile);
      
      List<GenericRecord> records = new ArrayList<>();
      for (int recordIdx = 0; recordIdx < recordsPerFile; recordIdx++) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("id", fileIdx * recordsPerFile + recordIdx);
        record.put("name", "Person" + (fileIdx * recordsPerFile + recordIdx));
        record.put("age", 20 + recordIdx);
        records.add(record);
      }
      createParquetFile(inputFile, schema, records);
    }

    // Merge files
    Path outputFile = new Path(tempDir.toString(), "output.parquet");
    HoodieParquetStrictMerge merger = new HoodieParquetStrictMerge(conf);
    merger.mergeFiles(inputFiles, outputFile);

    // Verify output file exists
    assertTrue(fs.exists(outputFile), "Output file should exist");

    // Verify merged file contains all records
    long totalRecords = 0;
    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(outputFile, conf))) {
      totalRecords = reader.getRecordCount();
    }
    assertEquals(numFiles * recordsPerFile, totalRecords, "Merged file should contain all records");
  }

  @Test
  public void testMergeFilesWithSchemaMismatch() throws IOException {
    // Create two different schemas
    Schema schema1 = Schema.createRecord("TestRecord1", null, null, false);
    List<Schema.Field> fields1 = new ArrayList<>();
    fields1.add(new Schema.Field("id", Schema.create(Schema.Type.INT), null, null));
    fields1.add(new Schema.Field("name", Schema.create(Schema.Type.STRING), null, null));
    schema1.setFields(fields1);

    Schema schema2 = Schema.createRecord("TestRecord2", null, null, false);
    List<Schema.Field> fields2 = new ArrayList<>();
    fields2.add(new Schema.Field("id", Schema.create(Schema.Type.INT), null, null));
    fields2.add(new Schema.Field("name", Schema.create(Schema.Type.STRING), null, null));
    fields2.add(new Schema.Field("email", Schema.create(Schema.Type.STRING), null, null));
    schema2.setFields(fields2);

    // Create test files with different schemas
    Path inputFile1 = new Path(tempDir.toString(), "input1.parquet");
    Path inputFile2 = new Path(tempDir.toString(), "input2.parquet");
    
    // Write file with schema1
    List<GenericRecord> records1 = new ArrayList<>();
    GenericRecord record1 = new GenericData.Record(schema1);
    record1.put("id", 1);
    record1.put("name", "Person1");
    records1.add(record1);
    createParquetFile(inputFile1, schema1, records1);
    
    // Write file with schema2
    List<GenericRecord> records2 = new ArrayList<>();
    GenericRecord record2 = new GenericData.Record(schema2);
    record2.put("id", 2);
    record2.put("name", "Person2");
    record2.put("email", "person2@example.com");
    records2.add(record2);
    createParquetFile(inputFile2, schema2, records2);

    // Attempt to merge files with different schemas
    List<Path> inputFiles = Arrays.asList(inputFile1, inputFile2);
    Path outputFile = new Path(tempDir.toString(), "output.parquet");
    HoodieParquetStrictMerge merger = new HoodieParquetStrictMerge(conf);
    
    // Should throw IllegalArgumentException due to schema mismatch
    assertThrows(IllegalArgumentException.class, () -> {
      merger.mergeFiles(inputFiles, outputFile);
    }, "Should throw exception for schema mismatch");
  }

  @Test
  public void testMergeWithEmptyInputFiles() {
    List<Path> inputFiles = new ArrayList<>();
    Path outputFile = new Path(tempDir.toString(), "output.parquet");
    HoodieParquetStrictMerge merger = new HoodieParquetStrictMerge(conf);
    
    // Should throw IllegalArgumentException for empty input files
    assertThrows(IllegalArgumentException.class, () -> {
      merger.mergeFiles(inputFiles, outputFile);
    }, "Should throw exception for empty input files list");
  }

  @Test
  public void testMergeWithNullInputFiles() {
    Path outputFile = new Path(tempDir.toString(), "output.parquet");
    HoodieParquetStrictMerge merger = new HoodieParquetStrictMerge(conf);
    
    // Should throw IllegalArgumentException for null input files
    assertThrows(IllegalArgumentException.class, () -> {
      merger.mergeFiles(null, outputFile);
    }, "Should throw exception for null input files");
  }

  @Test
  public void testDefaultConstructor() throws IOException {
    // Test default constructor
    HoodieParquetStrictMerge merger = new HoodieParquetStrictMerge();
    assertNotNull(merger, "Merger instance should not be null");
    
    // Create a simple test file to verify it works
    Schema schema = Schema.createRecord("TestRecord", null, null, false);
    List<Schema.Field> fields = new ArrayList<>();
    fields.add(new Schema.Field("id", Schema.create(Schema.Type.INT), null, null));
    schema.setFields(fields);
    
    Path inputFile = new Path(tempDir.toString(), "input.parquet");
    List<GenericRecord> records = new ArrayList<>();
    GenericRecord record = new GenericData.Record(schema);
    record.put("id", 1);
    records.add(record);
    createParquetFile(inputFile, schema, records, new Configuration(), null);
    
    Path outputFile = new Path(tempDir.toString(), "output.parquet");
    merger.mergeFiles(Arrays.asList(inputFile), outputFile);
    
    // Verify file was created
    assertTrue(FileSystem.get(new Configuration()).exists(outputFile), 
        "Output file should exist when using default constructor");
  }

  @Test
  public void testMergeWithCustomConfiguration() throws IOException {
    // Create custom configuration
    Configuration customConf = new Configuration();
    customConf.set("custom.property", "customValue");
    
    HoodieParquetStrictMerge merger = new HoodieParquetStrictMerge(customConf);
    
    // Create test schema
    Schema schema = Schema.createRecord("TestRecord", null, null, false);
    List<Schema.Field> fields = new ArrayList<>();
    fields.add(new Schema.Field("id", Schema.create(Schema.Type.INT), null, null));
    schema.setFields(fields);
    
    // Create test file
    Path inputFile = new Path(tempDir.toString(), "input.parquet");
    List<GenericRecord> records = new ArrayList<>();
    GenericRecord record = new GenericData.Record(schema);
    record.put("id", 1);
    records.add(record);
    createParquetFile(inputFile, schema, records, customConf, null);
    
    Path outputFile = new Path(tempDir.toString(), "output.parquet");
    merger.mergeFiles(Arrays.asList(inputFile), outputFile);
    
    // Verify output file exists
    assertTrue(fs.exists(outputFile), "Output file should exist with custom configuration");
  }

  @Test
  public void testMergeLargeNumberOfFiles() throws IOException {
    // Create test schema
    Schema schema = Schema.createRecord("TestRecord", null, null, false);
    List<Schema.Field> fields = new ArrayList<>();
    fields.add(new Schema.Field("id", Schema.create(Schema.Type.INT), null, null));
    fields.add(new Schema.Field("value", Schema.create(Schema.Type.DOUBLE), null, null));
    schema.setFields(fields);

    // Create many small files
    List<Path> inputFiles = new ArrayList<>();
    int numFiles = 20;
    int recordsPerFile = 5;
    
    for (int fileIdx = 0; fileIdx < numFiles; fileIdx++) {
      Path inputFile = new Path(tempDir.toString(), "input" + fileIdx + ".parquet");
      inputFiles.add(inputFile);
      
      List<GenericRecord> records = new ArrayList<>();
      for (int recordIdx = 0; recordIdx < recordsPerFile; recordIdx++) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("id", fileIdx * recordsPerFile + recordIdx);
        record.put("value", Math.random() * 100);
        records.add(record);
      }
      createParquetFile(inputFile, schema, records);
    }

    // Merge all files
    Path outputFile = new Path(tempDir.toString(), "output_large.parquet");
    HoodieParquetStrictMerge merger = new HoodieParquetStrictMerge(conf);
    merger.mergeFiles(inputFiles, outputFile);

    // Verify output
    assertTrue(fs.exists(outputFile), "Output file should exist");
    
    // Verify record count
    long totalRecords = 0;
    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(outputFile, conf))) {
      totalRecords = reader.getRecordCount();
    }
    assertEquals(numFiles * recordsPerFile, totalRecords, 
        "Merged file should contain all records from all input files");
  }
}