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
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.IncrSourceHelper;

import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class TestHoodieIncrSource extends SparkClientFunctionalTestHarness {

  private HoodieTestDataGenerator dataGen;
  private HoodieTableMetaClient metaClient;

  @BeforeEach
  public void setUp() throws IOException {
    dataGen = new HoodieTestDataGenerator();
    metaClient = getHoodieMetaClient(hadoopConf(), basePath());
  }

  @Test
  public void testHoodieIncrSource() throws IOException {
    HoodieWriteConfig writeConfig = getConfigBuilder(basePath(), metaClient)
        .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(2, 3).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder().retainCommits(1).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .build();

    SparkRDDWriteClient writeClient = getHoodieWriteClient(writeConfig);
    Pair<String, List<HoodieRecord>> inserts = writeRecords(writeClient, true, null, "100");
    Pair<String, List<HoodieRecord>> inserts2 = writeRecords(writeClient, true, null, "200");
    Pair<String, List<HoodieRecord>> inserts3 = writeRecords(writeClient, true, null, "300");
    Pair<String, List<HoodieRecord>> inserts4 = writeRecords(writeClient, true, null, "400");
    Pair<String, List<HoodieRecord>> inserts5 = writeRecords(writeClient, true, null, "500");

    // read everything upto latest
    readAndAssert(IncrSourceHelper.MissingCheckpointStrategy.READ_UPTO_LATEST_COMMIT, Option.empty(), 500, inserts5.getKey());

    // even if the begin timestamp is archived (100), full table scan should kick in, but should filter for records having commit time > 100
    readAndAssert(IncrSourceHelper.MissingCheckpointStrategy.READ_UPTO_LATEST_COMMIT, Option.of("100"), 400, inserts5.getKey());

    // even if the read upto latest is set, if begin timestamp is in active timeline, only incremental should kick in.
    readAndAssert(IncrSourceHelper.MissingCheckpointStrategy.READ_UPTO_LATEST_COMMIT, Option.of("400"), 100, inserts5.getKey());

    // read just the latest
    readAndAssert(IncrSourceHelper.MissingCheckpointStrategy.READ_LATEST, Option.empty(), 100, inserts5.getKey());

    // ensure checkpoint does not move
    readAndAssert(IncrSourceHelper.MissingCheckpointStrategy.READ_LATEST, Option.of(inserts5.getKey()), 0, inserts5.getKey());

    Pair<String, List<HoodieRecord>> inserts6 = writeRecords(writeClient, true, null, "600");

    // insert new batch and ensure the checkpoint moves
    readAndAssert(IncrSourceHelper.MissingCheckpointStrategy.READ_LATEST, Option.of(inserts5.getKey()), 100, inserts6.getKey());
    writeClient.close();
  }

  private void readAndAssert(IncrSourceHelper.MissingCheckpointStrategy missingCheckpointStrategy, Option<String> checkpointToPull, int expectedCount, String expectedCheckpoint) {

    Properties properties = new Properties();
    properties.setProperty("hoodie.deltastreamer.source.hoodieincr.path", basePath());
    properties.setProperty("hoodie.deltastreamer.source.hoodieincr.missing.checkpoint.strategy", missingCheckpointStrategy.name());
    TypedProperties typedProperties = new TypedProperties(properties);
    HoodieIncrSource incrSource = new HoodieIncrSource(typedProperties, jsc(), spark(), new DummySchemaProvider(HoodieTestDataGenerator.AVRO_SCHEMA));

    // read everything until latest
    Pair<Option<Dataset<Row>>, String> batchCheckPoint = incrSource.fetchNextBatch(checkpointToPull, 500);
    Assertions.assertNotNull(batchCheckPoint.getValue());
    if (expectedCount == 0) {
      assertFalse(batchCheckPoint.getKey().isPresent());
    } else {
      assertEquals(expectedCount, batchCheckPoint.getKey().get().count());
    }
    Assertions.assertEquals(expectedCheckpoint, batchCheckPoint.getRight());
  }

  private Pair<String, List<HoodieRecord>> writeRecords(SparkRDDWriteClient writeClient, boolean insert, List<HoodieRecord> insertRecords, String commit) throws IOException {
    writeClient.startCommitWithTime(commit);
    List<HoodieRecord> records = insert ? dataGen.generateInserts(commit, 100) : dataGen.generateUpdates(commit, insertRecords);
    JavaRDD<WriteStatus> result = writeClient.upsert(jsc().parallelize(records, 1), commit);
    List<WriteStatus> statuses = result.collect();
    assertNoWriteErrors(statuses);
    return Pair.of(commit, records);
  }

  private HoodieWriteConfig.Builder getConfigBuilder(String basePath, HoodieTableMetaClient metaClient) {
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2).withBulkInsertParallelism(2).withFinalizeWriteParallelism(2).withDeleteParallelism(2)
        .withTimelineLayoutVersion(TimelineLayoutVersion.CURR_VERSION)
        .forTable(metaClient.getTableConfig().getTableName());
  }

  private static class DummySchemaProvider extends SchemaProvider {

    private final Schema schema;

    public DummySchemaProvider(Schema schema) {
      super(new TypedProperties());
      this.schema = schema;
    }

    @Override
    public Schema getSourceSchema() {
      return schema;
    }
  }
}
