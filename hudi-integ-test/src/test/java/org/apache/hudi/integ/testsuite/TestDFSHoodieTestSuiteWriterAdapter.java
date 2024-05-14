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

import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.integ.testsuite.configuration.DFSDeltaConfig;
import org.apache.hudi.integ.testsuite.configuration.DeltaConfig;
import org.apache.hudi.integ.testsuite.generator.FlexibleSchemaRecordGenerationIterator;
import org.apache.hudi.integ.testsuite.reader.DeltaInputType;
import org.apache.hudi.integ.testsuite.utils.TestUtils;
import org.apache.hudi.integ.testsuite.writer.AvroFileDeltaInputWriter;
import org.apache.hudi.integ.testsuite.writer.DFSDeltaWriterAdapter;
import org.apache.hudi.integ.testsuite.writer.DeltaInputWriter;
import org.apache.hudi.integ.testsuite.writer.DeltaOutputMode;
import org.apache.hudi.integ.testsuite.writer.DeltaWriteStats;
import org.apache.hudi.integ.testsuite.writer.DeltaWriterAdapter;
import org.apache.hudi.integ.testsuite.writer.DeltaWriterFactory;
import org.apache.hudi.utilities.schema.FilebasedSchemaProvider;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

/**
 * Unit test against DeltaWriterAdapter, by testing writing DFS files.
 */
public class TestDFSHoodieTestSuiteWriterAdapter extends UtilitiesTestBase {

  private FilebasedSchemaProvider schemaProvider;
  private static final String COMPLEX_SOURCE_SCHEMA_DOCKER_DEMO_RELATIVE_PATH = "/docker/demo/config/test-suite/";

  @BeforeAll
  public static void initClass() throws Exception {
    UtilitiesTestBase.initTestServices(true, false, false);
  }

  @AfterAll
  public static void cleanupClass() throws IOException {
    UtilitiesTestBase.cleanUpUtilitiesTestServices();
  }

  @BeforeEach
  public void setup() throws Exception {
    super.setup();
    schemaProvider = new FilebasedSchemaProvider(Helpers.setupSchemaOnDFSWithAbsoluteScope(
        System.getProperty("user.dir") + "/.." + COMPLEX_SOURCE_SCHEMA_DOCKER_DEMO_RELATIVE_PATH,
        "complex-source.avsc"), jsc);
  }

  @AfterEach
  public void teardown() throws Exception {
    super.teardown();
  }

  @Test
  public void testDFSOneFileWrite() throws IOException {

    DeltaInputWriter<GenericRecord> mockFileSinkWriter = Mockito.mock(AvroFileDeltaInputWriter.class);
    DeltaWriteStats mockDeltaWriteStats = Mockito.mock(DeltaWriteStats.class);
    when(mockFileSinkWriter.getNewWriter()).thenReturn(mockFileSinkWriter);
    when(mockFileSinkWriter.canWrite()).thenReturn(true);
    when(mockFileSinkWriter.getDeltaWriteStats()).thenReturn(mockDeltaWriteStats);

    DeltaWriterAdapter<GenericRecord> dfsDeltaWriterAdapter = new DFSDeltaWriterAdapter(mockFileSinkWriter);

    JavaRDD<GenericRecord> records = TestUtils.makeRDD(jsc, 10);

    dfsDeltaWriterAdapter.write(records.collect().iterator());
    Mockito.verify(mockFileSinkWriter, times(10)).canWrite();
    Mockito.verify(mockFileSinkWriter, times(1)).close();
  }

  @Test
  @Disabled
  // TODO(HUDI-3668): Fix this test
  public void testDFSTwoFilesWriteWithRollover() throws IOException {

    DeltaInputWriter<GenericRecord> mockFileSinkWriter = Mockito.mock(AvroFileDeltaInputWriter.class);
    DeltaWriteStats mockDeltaWriteStats = Mockito.mock(DeltaWriteStats.class);
    when(mockFileSinkWriter.getNewWriter()).thenReturn(mockFileSinkWriter);
    when(mockFileSinkWriter.canWrite()).thenReturn(false, true);
    when(mockFileSinkWriter.getDeltaWriteStats()).thenReturn(mockDeltaWriteStats);

    DeltaWriterAdapter<GenericRecord> dfsDeltaWriterAdapter = new DFSDeltaWriterAdapter(mockFileSinkWriter);

    Iterator<GenericRecord> mockIterator = Mockito.mock(Iterator.class);
    when(mockIterator.hasNext()).thenReturn(true, true, true, false);

    dfsDeltaWriterAdapter.write(mockIterator);
    Mockito.verify(mockFileSinkWriter, times(2)).canWrite();
    Mockito.verify(mockFileSinkWriter, times(1)).getNewWriter();
    Mockito.verify(mockFileSinkWriter, times(2)).close();
  }

  @Test
  @Disabled
  // TODO(HUDI-3668): Fix this test
  public void testDFSWorkloadSinkWithMultipleFilesFunctional() throws IOException {
    DeltaConfig dfsSinkConfig = new DFSDeltaConfig(DeltaOutputMode.DFS, DeltaInputType.AVRO,
        HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration()), basePath, basePath,
        schemaProvider.getSourceSchema().toString(), 10240L, jsc.defaultParallelism(), false, false);
    DeltaWriterAdapter<GenericRecord> dfsDeltaWriterAdapter = DeltaWriterFactory
        .getDeltaWriterAdapter(dfsSinkConfig, 1);
    FlexibleSchemaRecordGenerationIterator itr = new FlexibleSchemaRecordGenerationIterator(1000,
        schemaProvider.getSourceSchema().toString());
    dfsDeltaWriterAdapter.write(itr);
    FileSystem fs = HadoopFSUtils.getFs(basePath, jsc.hadoopConfiguration());
    FileStatus[] fileStatuses = fs.listStatus(new Path(basePath));
    // Since maxFileSize was 10240L and we produced 1K records each close to 1K size, we should produce more than
    // 1 file
    assertTrue(fileStatuses.length > 0);
  }

}
