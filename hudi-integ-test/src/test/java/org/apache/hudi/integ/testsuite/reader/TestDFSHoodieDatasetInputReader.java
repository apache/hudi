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

package org.apache.hudi.integ.testsuite.reader;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit test for {@link DFSHoodieDatasetInputReader}.
 */
public class TestDFSHoodieDatasetInputReader extends UtilitiesTestBase {

  @BeforeAll
  public static void initClass() throws Exception {
    UtilitiesTestBase.initTestServices(false, false);
  }

  @AfterAll
  public static void cleanupClass() {
    UtilitiesTestBase.cleanupClass();
  }

  @BeforeEach
  public void setup() throws Exception {
    super.setup();
    HoodieTestUtils.init(jsc.hadoopConfiguration(), dfsBasePath);
  }

  @AfterEach
  public void teardown() throws Exception {
    super.teardown();
  }

  @Test
  @Disabled
  // TODO(HUDI-3668): Fix this test
  public void testSimpleHoodieDatasetReader() throws Exception {

    HoodieWriteConfig config = makeHoodieClientConfig();
    SparkRDDWriteClient client = new SparkRDDWriteClient(new HoodieSparkEngineContext(jsc), config);
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
        .withDeleteParallelism(2)
        .withSchema(HoodieTestDataGenerator
            .TRIP_EXAMPLE_SCHEMA);
  }

}
