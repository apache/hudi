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

package org.apache.hudi.utilities.inline.fs;

import org.apache.hudi.common.HoodieTestDataGenerator;
import org.apache.hudi.common.model.HoodieRecord;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class TestHoodieEmbeddedParquetLogFile {

  private Configuration conf;

  public TestHoodieEmbeddedParquetLogFile() {
    conf = new Configuration();
    conf.set("fs." + InlineFileSystem.SCHEME + ".impl", InlineFileSystem.class.getName());
  }

  @Test
  public void simpleReadWriteTest() throws IOException {
    String randomFileName = UUID.randomUUID().toString();
    HoodieEmbeddedParquetLogFile logFile =
        new HoodieEmbeddedParquetLogFile(Collections.EMPTY_MAP, conf, new Path("file:///tmp/" + randomFileName),
            HoodieTestDataGenerator.avroSchema);

    // Fetch ParquetWriter
    ParquetWriter writer = logFile.getInlineParquetWriter();
    List<GenericRecord> writeList = getParquetHoodieRecords();
    for (GenericRecord genericRecord : writeList) {
      writer.write(genericRecord);
    }
    // close the parquet writer
    logFile.closeInlineParquetWriter();
    // serialize the whole file
    logFile.write();
    // Fetch ParquetReader
    ParquetReader reader = logFile.getInlineParquetReader();
    List<GenericRecord> readRecords = readParquetGenericRecords(reader);
    logFile.closeInLineParquetReader();
    Assert.assertArrayEquals(writeList.toArray(), readRecords.toArray());
  }

  @Test
  public void simpleEmptyReadWriteTest() throws IOException {
    String randomFileName = UUID.randomUUID().toString();
    HoodieEmbeddedParquetLogFile logFile =
        new HoodieEmbeddedParquetLogFile(Collections.EMPTY_MAP, conf, new Path("file:///tmp/" + randomFileName),
            HoodieTestDataGenerator.avroSchema);

    // Fetch ParquetWriter
    ParquetWriter writer = logFile.getInlineParquetWriter();
    // close the parquet writer
    logFile.closeInlineParquetWriter();
    // serialize the whole file
    logFile.write();
    // Fetch ParquetReader
    ParquetReader reader = logFile.getInlineParquetReader();
    List<GenericRecord> readRecords = readParquetGenericRecords(reader);
    logFile.closeInLineParquetReader();
    Assert.assertTrue(readRecords.isEmpty());
  }

  private List<GenericRecord> getParquetHoodieRecords() throws IOException {
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    String commitTime = "001";
    List<HoodieRecord> hoodieRecords = dataGenerator.generateInsertsWithHoodieAvroPayload(commitTime, 10);
    List<GenericRecord> toReturn = new ArrayList<>();
    for (HoodieRecord record : hoodieRecords) {
      toReturn.add((GenericRecord) record.getData().getInsertValue(HoodieTestDataGenerator.avroSchema).get());
    }
    return toReturn;
  }

  private List<GenericRecord> readParquetGenericRecords(ParquetReader reader) throws IOException {
    List<GenericRecord> toReturn = new ArrayList<>();
    Object obj = reader.read();
    while (obj != null && obj instanceof GenericRecord) {
      toReturn.add((GenericRecord) obj);
      obj = reader.read();
    }
    return toReturn;
  }
}
