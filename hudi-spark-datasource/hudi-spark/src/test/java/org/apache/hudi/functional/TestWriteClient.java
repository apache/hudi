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

package org.apache.hudi.functional;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.HoodieSparkClientTestBase;

import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests write client functionality.
 */
@Tag("functional")
public class TestWriteClient extends HoodieSparkClientTestBase {

  @Test
  public void testInertsWithEmptyCommitsHavingWriterSchemaAsNull() throws Exception {
    HoodieWriteConfig.Builder cfgBuilder = getConfigBuilder().withAutoCommit(false);
    addConfigsForPopulateMetaFields(cfgBuilder, false);
    // Re-init meta client with write config props.
    metaClient = HoodieTestUtils.init(basePath, HoodieTableType.MERGE_ON_READ, cfgBuilder.build().getProps());
    SparkRDDWriteClient client = getHoodieWriteClient(cfgBuilder.build());
    try {
      String firstCommit = "001";
      int numRecords = 200;
      JavaRDD<WriteStatus> result = insertFirstBatch(cfgBuilder.build(), client, firstCommit, "000", numRecords, SparkRDDWriteClient::insert,
          false, false, numRecords);
      assertTrue(client.commit(firstCommit, result), "Commit should succeed");

      // Re-init client with null writer schema.
      cfgBuilder = getConfigBuilder((String) null).withAutoCommit(false);
      addConfigsForPopulateMetaFields(cfgBuilder, false);
      client = getHoodieWriteClient(cfgBuilder.build());
      String secondCommit = "002";
      client.startCommitWithTime(secondCommit);
      JavaRDD<HoodieRecord> emptyRdd = context.emptyRDD();
      result = client.insert(emptyRdd, secondCommit);
      assertTrue(client.commit(secondCommit, result), "Commit should succeed");
      // Schema Validations.
      HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(jsc.hadoopConfiguration()).setBasePath(basePath).build();
      HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
      HoodieCommitMetadata metadata = HoodieCommitMetadata.fromBytes(timeline.getInstantDetails(timeline.lastInstant().get()).get(), HoodieCommitMetadata.class);
      assertTrue(metadata.getExtraMetadata().get("schema").isEmpty());
      TableSchemaResolver tableSchemaResolver = new TableSchemaResolver(metaClient);
      assertEquals(Schema.parse(TRIP_EXAMPLE_SCHEMA), tableSchemaResolver.getTableAvroSchema(false));
      // Data Validations.
      Dataset<Row> df = sparkSession.read().format("hudi").load(basePath);
      assertEquals(numRecords, df.collectAsList().size());
    } finally {
      client.close();
    }
  }
}
