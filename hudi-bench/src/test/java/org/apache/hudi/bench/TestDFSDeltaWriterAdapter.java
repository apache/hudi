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

import org.apache.hudi.bench.configuration.DFSDeltaConfig;
import org.apache.hudi.bench.configuration.DeltaConfig;
import org.apache.hudi.bench.generator.FlexibleSchemaRecordGenerationIterator;
import org.apache.hudi.bench.utils.TestUtils;
import org.apache.hudi.bench.writer.FileDeltaInputWriter;
import org.apache.hudi.bench.writer.WriteStats;
import org.apache.hudi.common.SerializableConfiguration;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.utilities.UtilitiesTestBase;
import org.apache.hudi.utilities.schema.FilebasedSchemaProvider;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Iterator;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

public class TestDFSDeltaWriterAdapter extends UtilitiesTestBase {

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
  public void testDFSOneFileWrite() throws IOException {

    FileDeltaInputWriter<GenericRecord> mockFileSinkWriter = Mockito.mock(FileDeltaInputWriter.class);
    WriteStats mockWriteStats = Mockito.mock(WriteStats.class);
    when(mockFileSinkWriter.getNewWriter()).thenReturn(mockFileSinkWriter);
    when(mockFileSinkWriter.canWrite()).thenReturn(true);
    when(mockFileSinkWriter.getWriteStats()).thenReturn(mockWriteStats);

    DeltaWriterAdapter<GenericRecord> dfsDeltaWriterAdapter = new DFSDeltaWriterAdapter(mockFileSinkWriter);

    JavaRDD<GenericRecord> records = TestUtils.makeRDD(jsc, 10);

    dfsDeltaWriterAdapter.write(records.collect().iterator());
    Mockito.verify(mockFileSinkWriter, times(10)).canWrite();
    Mockito.verify(mockFileSinkWriter, times(1)).close();
  }

  @Test
  public void testDFSTwoFilesWriteWithRollover() throws IOException {

    FileDeltaInputWriter<GenericRecord> mockFileSinkWriter = Mockito.mock(FileDeltaInputWriter.class);
    WriteStats mockWriteStats = Mockito.mock(WriteStats.class);
    when(mockFileSinkWriter.getNewWriter()).thenReturn(mockFileSinkWriter);
    when(mockFileSinkWriter.canWrite()).thenReturn(false, true);
    when(mockFileSinkWriter.getWriteStats()).thenReturn(mockWriteStats);

    DeltaWriterAdapter<GenericRecord> dfsDeltaWriterAdapter = new DFSDeltaWriterAdapter(mockFileSinkWriter);

    Iterator<GenericRecord> mockIterator = Mockito.mock(Iterator.class);
    when(mockIterator.hasNext()).thenReturn(true, true, true, false);

    dfsDeltaWriterAdapter.write(mockIterator);
    Mockito.verify(mockFileSinkWriter, times(2)).canWrite();
    Mockito.verify(mockFileSinkWriter, times(1)).getNewWriter();
    Mockito.verify(mockFileSinkWriter, times(2)).close();
  }

  @Test
  public void testDFSWorkloadSinkWithMultipleFilesFunctional() throws IOException {
    DeltaConfig dfsSinkConfig = new DFSDeltaConfig(DeltaOutputType.DFS, DeltaInputFormat.AVRO,
        new SerializableConfiguration(jsc.hadoopConfiguration()), dfsBasePath, dfsBasePath,
        schemaProvider.getSourceSchema().toString(), 10240L);
    DeltaWriterAdapter<GenericRecord> dfsDeltaWriterAdapter = DeltaWriterFactory
        .getDeltaWriterAdapter(dfsSinkConfig, 1);
    FlexibleSchemaRecordGenerationIterator itr = new FlexibleSchemaRecordGenerationIterator(1000,
        schemaProvider.getSourceSchema().toString());
    dfsDeltaWriterAdapter.write(itr);
    FileSystem fs = FSUtils.getFs(dfsBasePath, jsc.hadoopConfiguration());
    FileStatus[] fileStatuses = fs.listStatus(new Path(dfsBasePath));
    // Since maxFileSize was 10240L and we produced 1K records each close to 1K size, we should produce more than
    // 1 file
    Assert.assertTrue(fileStatuses.length > 0);
  }

}
