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

package org.apache.hudi.utilities.sources;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.HoodieClientTestHarness;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.IncrSourceHelper;

import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestHoodieIncrSource extends HoodieClientTestHarness {

  @BeforeEach
  public void setUp() throws IOException {
    initResources();
  }

  @AfterEach
  public void tearDown() throws IOException {
    cleanupResources();
  }

  @Test
  public void testHoodieIncrSource() throws IOException {
    HoodieWriteConfig writeConfig = getConfigBuilder(basePath).build();

    SparkRDDWriteClient writeClient = new SparkRDDWriteClient(context, writeConfig);
    Pair<String, List<HoodieRecord>> inserts = writeRecords(writeClient, true, null);
    Pair<String, List<HoodieRecord>> inserts2 = writeRecords(writeClient, true, null);
    Pair<String, List<HoodieRecord>> inserts3 = writeRecords(writeClient, true, null);

    // read everything upto latest
    readAndAssert(IncrSourceHelper.MissingCheckpointStrategy.READ_UPTO_LATEST_COMMIT, 300, inserts3.getKey());

    // read just the latest
    readAndAssert(IncrSourceHelper.MissingCheckpointStrategy.READ_LATEST, 100, inserts3.getKey());
  }

  private void readAndAssert(IncrSourceHelper.MissingCheckpointStrategy missingCheckpointStrategy, int expectedCount, String expectedCheckpoint) {

    Properties properties = new Properties();
    properties.setProperty("hoodie.deltastreamer.source.hoodieincr.path", basePath);
    properties.setProperty("hoodie.deltastreamer.source.hoodieincr.missing.checkpoint.strategy", missingCheckpointStrategy.name());
    TypedProperties typedProperties = new TypedProperties(properties);
    HoodieIncrSource incrSource = new HoodieIncrSource(typedProperties, jsc, sparkSession, new TestSchemaProvider(HoodieTestDataGenerator.AVRO_SCHEMA));

    // read everything until latest
    Pair<Option<Dataset<Row>>, String> batchCheckPoint = incrSource.fetchNextBatch(Option.empty(), 500);
    Assertions.assertNotNull(batchCheckPoint.getValue());
    assertEquals(batchCheckPoint.getKey().get().count(), expectedCount);
    Assertions.assertEquals(batchCheckPoint.getRight(), expectedCheckpoint);
  }

  public Pair<String, List<HoodieRecord>> writeRecords(SparkRDDWriteClient writeClient, boolean insert, List<HoodieRecord> insertRecords) throws IOException {
    String commit = writeClient.startCommit();
    List<HoodieRecord> records = insert ? dataGen.generateInserts(commit, 100) : dataGen.generateUpdates(commit, insertRecords);
    JavaRDD<WriteStatus> result = writeClient.upsert(jsc.parallelize(records, 1), commit);
    List<WriteStatus> statuses = result.collect();
    assertNoWriteErrors(statuses);
    return Pair.of(commit, records);
  }

  public HoodieWriteConfig.Builder getConfigBuilder(String basePath) {
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2).withBulkInsertParallelism(2).withFinalizeWriteParallelism(2).withDeleteParallelism(2)
        .withTimelineLayoutVersion(TimelineLayoutVersion.CURR_VERSION)
        .forTable("test-hoodie-incr-source");
  }

  class TestSchemaProvider extends SchemaProvider {

    private final Schema schema;

    public TestSchemaProvider(Schema schema) {
      super(new TypedProperties());
      this.schema = schema;
    }

    @Override
    public Schema getSourceSchema() {
      return schema;
    }
  }
}
