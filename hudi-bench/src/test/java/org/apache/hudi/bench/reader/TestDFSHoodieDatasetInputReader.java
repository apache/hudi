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

package org.apache.hudi.bench.reader;

import org.apache.hudi.HoodieWriteClient;
import org.apache.hudi.WriteStatus;
import org.apache.hudi.common.HoodieTestDataGenerator;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTestUtils;
import org.apache.hudi.common.util.HoodieAvroUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.utilities.UtilitiesTestBase;
import org.apache.hudi.utilities.schema.FilebasedSchemaProvider;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class TestDFSHoodieDatasetInputReader extends UtilitiesTestBase {

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
    HoodieTestUtils.init(jsc.hadoopConfiguration(), dfsBasePath);
  }

  @After
  public void teardown() throws Exception {
    super.teardown();
  }

  @Test
  public void testSimpleHoodieDatasetReader() throws Exception {

    HoodieWriteConfig config = makeHoodieClientConfig();
    HoodieWriteClient client = new HoodieWriteClient(jsc, config);
    String commitTime = client.startCommit();
    HoodieTestDataGenerator generator = new HoodieTestDataGenerator();
    // Insert 100 records across 3 partitions
    List<HoodieRecord> inserts = generator.generateInserts(commitTime, 100);
    JavaRDD<WriteStatus> writeStatuses = client.upsert(jsc.parallelize(inserts), commitTime);
    writeStatuses.count();

    DFSHoodieDatasetInputReader reader = new DFSHoodieDatasetInputReader(jsc, config.getBasePath(),
        HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(config.getSchema())).toString());
    // Try to read 100 records for the same partition path and same file ID
    JavaRDD<GenericRecord> records = reader.read(1, 1, 100L);
    assertTrue(records.count() <= 100);
    assertEquals(new HashSet<>(records.map(p -> p.get(HoodieRecord.PARTITION_PATH_METADATA_FIELD)).collect()).size(),
        1);
    assertEquals(new HashSet<>(records.map(p -> p.get(HoodieRecord.FILENAME_METADATA_FIELD)).collect()).size(),
        1);

    // Try to read 100 records for 3 partition paths and 3 different file ids
    records = reader.read(3, 3, 100L);
    assertTrue(records.count() <= 100);
    assertEquals(new HashSet<>(records.map(p -> p.get(HoodieRecord.PARTITION_PATH_METADATA_FIELD)).collect()).size(),
        3);
    assertEquals(new HashSet<>(records.map(p -> p.get(HoodieRecord.FILENAME_METADATA_FIELD)).collect()).size(),
        3);

    // Try to read 100 records for 3 partition paths and 50% records from each file
    records = reader.read(3, 3, 0.5);
    assertTrue(records.count() <= 100);
    assertEquals(new HashSet<>(records.map(p -> p.get(HoodieRecord.PARTITION_PATH_METADATA_FIELD)).collect()).size(),
        3);
    assertEquals(new HashSet<>(records.map(p -> p.get(HoodieRecord.FILENAME_METADATA_FIELD)).collect()).size(),
        3);
  }

  private HoodieWriteConfig makeHoodieClientConfig() throws Exception {
    return makeHoodieClientConfigBuilder().build();
  }

  private HoodieWriteConfig.Builder makeHoodieClientConfigBuilder() throws Exception {
    // Prepare the AvroParquetIO
    return HoodieWriteConfig.newBuilder().withPath(dfsBasePath)
        .withParallelism(2, 2)
        .withSchema(HoodieTestDataGenerator
            .TRIP_EXAMPLE_SCHEMA);
  }

}
