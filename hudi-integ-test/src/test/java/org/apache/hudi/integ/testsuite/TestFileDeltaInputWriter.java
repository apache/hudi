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

package org.apache.hudi.integ.testsuite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.integ.testsuite.reader.SparkBasedReader;
import org.apache.hudi.integ.testsuite.writer.AvroFileDeltaInputWriter;
import org.apache.hudi.integ.testsuite.writer.DeltaInputWriter;
import org.apache.hudi.integ.testsuite.writer.DeltaWriteStats;
import org.apache.hudi.integ.testsuite.generator.GenericRecordFullPayloadGenerator;
import org.apache.hudi.integ.testsuite.reader.SparkBasedReader;
import org.apache.hudi.integ.testsuite.writer.AvroFileDeltaInputWriter;
import org.apache.hudi.integ.testsuite.writer.DeltaInputWriter;
import org.apache.hudi.integ.testsuite.writer.DeltaWriteStats;
import org.apache.hudi.utilities.schema.FilebasedSchemaProvider;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestFileDeltaInputWriter extends UtilitiesTestBase {

  private FilebasedSchemaProvider schemaProvider;
  private static final String COMPLEX_SOURCE_SCHEMA_DOCKER_DEMO_RELATIVE_PATH = "/docker/demo/config/test-suite/";

  @BeforeAll
  public static void initClass() throws Exception {
    UtilitiesTestBase.initClass();
  }

  @AfterAll
  public static void cleanupClass() {
    UtilitiesTestBase.cleanupClass();
  }

  @BeforeEach
  public void setup() throws Exception {
    super.setup();
    schemaProvider = new FilebasedSchemaProvider(Helpers.setupSchemaOnDFSWithAbsoluteScope(System.getProperty("user.dir") + "/.."
        + COMPLEX_SOURCE_SCHEMA_DOCKER_DEMO_RELATIVE_PATH, "complex-source.avsc"), jsc);
  }

  @AfterEach
  public void teardown() throws Exception {
    super.teardown();
  }

  @Test
  public void testAvroFileSinkWriter() throws IOException {
    // 1. Create a Avro File Sink Writer
    DeltaInputWriter<GenericRecord> fileSinkWriter =
        new AvroFileDeltaInputWriter(jsc.hadoopConfiguration(), dfsBasePath + "/input", schemaProvider.getSourceSchema()
            .toString(), 1024 * 1024L);
    GenericRecordFullPayloadGenerator payloadGenerator =
        new GenericRecordFullPayloadGenerator(schemaProvider.getSourceSchema());
    // 2. Generate 100 avro payloads and write them to an avro file
    IntStream.range(0, 100).forEach(a -> {
      try {
        fileSinkWriter.writeData(payloadGenerator.getNewPayload());
      } catch (IOException io) {
        throw new UncheckedIOException(io);
      }
    });
    fileSinkWriter.close();
    DeltaWriteStats deltaWriteStats = fileSinkWriter.getDeltaWriteStats();
    FileSystem fs = FSUtils.getFs(dfsBasePath, jsc.hadoopConfiguration());
    FileStatus[] fileStatuses = fs.listStatus(new Path(deltaWriteStats.getFilePath()));
    // Atleast 1 file was written
    assertEquals(1, fileStatuses.length);
    // File length should be greater than 0
    assertTrue(fileStatuses[0].getLen() > 0);
    // File length should be the same as the number of bytes written
    assertTrue(deltaWriteStats.getBytesWritten() > 0);
    List<String> paths = Arrays.asList(fs.globStatus(new Path(dfsBasePath + "/*/*.avro")))
        .stream().map(f -> f.getPath().toString()).collect(Collectors.toList());
    JavaRDD<GenericRecord> writtenRecords =
        SparkBasedReader.readAvro(sparkSession, schemaProvider.getSourceSchema().toString(), paths, Option.empty(),
            Option.empty());
    // Number of records written should be 100
    assertEquals(writtenRecords.count(), 100);
    // Number of records in file should match with the stats
    assertEquals(writtenRecords.count(), deltaWriteStats.getRecordsWritten());
  }

  @Test
  public void testAvroFileSinkCreateNewWriter() throws IOException {
    // 1. Create a Avro File Sink Writer
    DeltaInputWriter<GenericRecord> fileSinkWriter =
        new AvroFileDeltaInputWriter(jsc.hadoopConfiguration(), dfsBasePath,
            schemaProvider.getSourceSchema().toString(),
            1024 * 1024L);
    GenericRecordFullPayloadGenerator payloadGenerator =
        new GenericRecordFullPayloadGenerator(schemaProvider.getSourceSchema());
    // 2. Generate 100 avro payloads and write them to an avro file
    IntStream.range(0, 100).forEach(a -> {
      try {
        fileSinkWriter.writeData(payloadGenerator.getNewPayload());
      } catch (IOException io) {
        throw new UncheckedIOException(io);
      }
    });
    fileSinkWriter.close();
    String oldFilePath = fileSinkWriter.getDeltaWriteStats().getFilePath();
    assertFalse(oldFilePath == null);
    DeltaInputWriter<GenericRecord> newFileSinkWriter = fileSinkWriter.getNewWriter();
    newFileSinkWriter.close();
    DeltaWriteStats newStats = newFileSinkWriter.getDeltaWriteStats();
    assertEquals(newStats.getBytesWritten(), 3674);
    assertEquals(newStats.getRecordsWritten(), 0);
    assertTrue(newStats.getFilePath() != null);
  }

}
