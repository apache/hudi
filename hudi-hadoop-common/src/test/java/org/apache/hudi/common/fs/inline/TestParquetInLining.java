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

package org.apache.hudi.common.fs.inline;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.testutils.FileSystemTestUtils;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.hadoop.fs.inline.InLineFileSystem;
import org.apache.hudi.hadoop.fs.inline.InMemoryFileSystem;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.apache.hudi.common.testutils.FileSystemTestUtils.FILE_SCHEME;
import static org.apache.hudi.common.testutils.FileSystemTestUtils.getPhantomFile;
import static org.apache.hudi.common.testutils.FileSystemTestUtils.getRandomOuterInMemPath;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

/**
 * Tests for {@link InLineFileSystem} with Parquet writer and reader.
 */
public class TestParquetInLining {

  private final Configuration inMemoryConf;
  private final Configuration inlineConf;
  private Path generatedPath;

  public TestParquetInLining() {
    inMemoryConf = new Configuration();
    inMemoryConf.set("fs." + InMemoryFileSystem.SCHEME + ".impl", InMemoryFileSystem.class.getName());
    inlineConf = new Configuration();
    inlineConf.set("fs." + InLineFileSystem.SCHEME + ".impl", InLineFileSystem.class.getName());
  }

  @AfterEach
  public void teardown() throws IOException {
    if (generatedPath != null) {
      File filePath = new File(generatedPath.toString().substring(generatedPath.toString().indexOf(':') + 1));
      if (filePath.exists()) {
        FileSystemTestUtils.deleteFile(filePath);
      }
    }
  }

  @Test
  public void testSimpleInlineFileSystem() throws IOException {
    Path outerInMemFSPath = getRandomOuterInMemPath();
    Path outerPath = new Path(FILE_SCHEME + outerInMemFSPath.toString().substring(outerInMemFSPath.toString().indexOf(':')));
    generatedPath = outerPath;
    ParquetWriter inlineWriter = new AvroParquetWriter(outerInMemFSPath, HoodieTestDataGenerator.AVRO_SCHEMA,
        CompressionCodecName.GZIP, 100 * 1024 * 1024, 1024 * 1024, true, inMemoryConf);
    // write few records
    List<GenericRecord> recordsToWrite = getParquetHoodieRecords();
    for (GenericRecord rec : recordsToWrite) {
      inlineWriter.write(rec);
    }
    inlineWriter.close();
    byte[] inlineBytes = getBytesToInline(outerInMemFSPath);
    long startOffset = generateOuterFile(outerPath, inlineBytes);

    long inlineLength = inlineBytes.length;

    // Generate phantom inline file
    Path inlinePath = new Path(getPhantomFile(new StoragePath(outerPath.toUri()), startOffset, inlineLength).toUri());

    // instantiate Parquet reader
    ParquetReader inLineReader = AvroParquetReader.builder(inlinePath).withConf(inlineConf).build();
    List<GenericRecord> records = readParquetGenericRecords(inLineReader);
    assertArrayEquals(recordsToWrite.toArray(), records.toArray());
    inLineReader.close();
  }

  private long generateOuterFile(Path outerPath, byte[] inlineBytes) throws IOException {
    FSDataOutputStream wrappedOut = outerPath.getFileSystem(inMemoryConf).create(outerPath, true);
    // write random bytes
    writeRandomBytes(wrappedOut, 10);

    // save position for start offset
    long startOffset = wrappedOut.getPos();
    // embed inline file
    wrappedOut.write(inlineBytes);

    // write random bytes
    writeRandomBytes(wrappedOut, 5);
    wrappedOut.hsync();
    wrappedOut.close();
    return startOffset;
  }

  private byte[] getBytesToInline(Path outerInMemFSPath) throws IOException {
    InMemoryFileSystem inMemoryFileSystem = (InMemoryFileSystem) outerInMemFSPath.getFileSystem(inMemoryConf);
    return inMemoryFileSystem.getFileAsBytes();
  }

  static List<GenericRecord> readParquetGenericRecords(ParquetReader reader) throws IOException {
    List<GenericRecord> toReturn = new ArrayList<>();
    Object obj = reader.read();
    while (obj instanceof GenericRecord) {
      toReturn.add((GenericRecord) obj);
      obj = reader.read();
    }
    return toReturn;
  }

  private void writeRandomBytes(FSDataOutputStream writer, int count) throws IOException {
    for (int i = 0; i < count; i++) {
      writer.writeUTF(UUID.randomUUID().toString());
    }
  }

  static List<GenericRecord> getParquetHoodieRecords() {
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    String commitTime = "001";
    List<HoodieRecord> hoodieRecords = dataGenerator.generateInserts(commitTime, 10);
    List<GenericRecord> toReturn = new ArrayList<>();
    for (HoodieRecord record : hoodieRecords) {
      toReturn.add((GenericRecord) record.getData());
    }
    return toReturn;
  }
}
