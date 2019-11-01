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

package org.apache.hudi.bench;

import org.apache.hudi.bench.generator.GenericRecordFullPayloadGenerator;
import org.apache.hudi.bench.reader.SparkBasedReader;
import org.apache.hudi.bench.writer.AvroDeltaInputWriter;
import org.apache.hudi.bench.writer.FileDeltaInputWriter;
import org.apache.hudi.bench.writer.WriteStats;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.UtilitiesTestBase;
import org.apache.hudi.utilities.schema.FilebasedSchemaProvider;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestFileDeltaInputWriter extends UtilitiesTestBase {

  private FilebasedSchemaProvider schemaProvider;

  @BeforeClass
  public static void initClass() throws Exception {
    UtilitiesTestBase.initClass();
  }

  @AfterClass
  public static void cleanupClass() throws Exception {
    UtilitiesTestBase.cleanupClass();
  }

  @Before
  public void setup() throws Exception {
    super.setup();
    schemaProvider = new FilebasedSchemaProvider(Helpers.setupSchemaOnDFS("hudi-bench-config/complex-source.avsc"),
        jsc);
  }

  @After
  public void teardown() throws Exception {
    super.teardown();
  }

  @Test
  public void testAvroFileSinkWriter() throws IOException {
    // 1. Create a Avro File Sink Writer
    FileDeltaInputWriter<GenericRecord> fileSinkWriter =
        new AvroDeltaInputWriter(jsc.hadoopConfiguration(), dfsBasePath + "/input", schemaProvider.getSourceSchema()
            .toString(), 1024 * 1024L);
    GenericRecordFullPayloadGenerator payloadGenerator =
        new GenericRecordFullPayloadGenerator(schemaProvider.getSourceSchema());
    fileSinkWriter.open();
    // 2. Generate 100 avro payloads and write them to an avro file
    IntStream.range(0, 100).forEach(a -> {
      try {
        fileSinkWriter.writeData(payloadGenerator.getNewPayload());
      } catch (IOException io) {
        throw new UncheckedIOException(io);
      }
    });
    fileSinkWriter.close();
    WriteStats writeStats = fileSinkWriter.getWriteStats();
    FileSystem fs = FSUtils.getFs(dfsBasePath, jsc.hadoopConfiguration());
    FileStatus[] fileStatuses = fs.listStatus(new Path(writeStats.getFilePath()));
    // Atleast 1 file was written
    assertEquals(1, fileStatuses.length);
    // File length should be greater than 0
    assertTrue(fileStatuses[0].getLen() > 0);
    // File length should be the same as the number of bytes written
    assertTrue(writeStats.getBytesWritten() > 0);
    List<String> paths = Arrays.asList(fs.globStatus(new Path(dfsBasePath + "/*/*.avro")))
        .stream().map(f -> f.getPath().toString()).collect(Collectors.toList());
    JavaRDD<GenericRecord> writtenRecords =
        SparkBasedReader.readAvro(sparkSession, schemaProvider.getSourceSchema().toString(), paths, Option.empty(),
            Option.empty());
    // Number of records written should be 100
    assertEquals(writtenRecords.count(), 100);
    // Number of records in file should match with the stats
    assertEquals(writtenRecords.count(), writeStats.getRecordsWritten());
  }

  @Test
  public void testAvroFileSinkCreateNewWriter() throws IOException {
    // 1. Create a Avro File Sink Writer
    FileDeltaInputWriter<GenericRecord> fileSinkWriter =
        new AvroDeltaInputWriter(jsc.hadoopConfiguration(), dfsBasePath, schemaProvider.getSourceSchema().toString(),
            1024 * 1024L);
    GenericRecordFullPayloadGenerator payloadGenerator =
        new GenericRecordFullPayloadGenerator(schemaProvider.getSourceSchema());
    fileSinkWriter.open();
    // 2. Generate 100 avro payloads and write them to an avro file
    IntStream.range(0, 100).forEach(a -> {
      try {
        fileSinkWriter.writeData(payloadGenerator.getNewPayload());
      } catch (IOException io) {
        throw new UncheckedIOException(io);
      }
    });
    fileSinkWriter.close();
    String oldFilePath = fileSinkWriter.getWriteStats().getFilePath();
    assertFalse(oldFilePath == null);
    FileDeltaInputWriter<GenericRecord> newFileSinkWriter = fileSinkWriter.getNewWriter();
    newFileSinkWriter.close();
    WriteStats newStats = newFileSinkWriter.getWriteStats();
    assertEquals(newStats.getBytesWritten(), 3674);
    assertEquals(newStats.getRecordsWritten(), 0);
    assertTrue(newStats.getFilePath() != null);
  }

}
