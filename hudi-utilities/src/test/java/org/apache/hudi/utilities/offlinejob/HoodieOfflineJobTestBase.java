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

package org.apache.hudi.utilities.offlinejob;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestUtils.createMetaClient;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class HoodieOfflineJobTestBase extends UtilitiesTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(UtilitiesTestBase.class);
  protected HoodieTestDataGenerator dataGen;
  protected SparkRDDWriteClient client;
  protected HoodieTableMetaClient metaClient;

  @BeforeAll
  public static void initClass() throws Exception {
    UtilitiesTestBase.initTestServices(false, false, false);
  }

  @BeforeEach
  public void setup() {
    dataGen = new HoodieTestDataGenerator();
  }

  @AfterEach
  public void teardown() {
    if (client != null) {
      client.close();
    }
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  protected Properties getPropertiesForKeyGen(boolean populateMetaFields) {
    Properties properties = new Properties();
    properties.put(HoodieTableConfig.POPULATE_META_FIELDS.key(), String.valueOf(populateMetaFields));
    properties.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key");
    properties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "partition_path");
    properties.put(HoodieTableConfig.RECORDKEY_FIELDS.key(), "_row_key");
    properties.put(HoodieTableConfig.PARTITION_FIELDS.key(), "partition_path");
    return properties;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  protected List<WriteStatus> writeData(boolean isUpsert, String instant, int numRecords, boolean doCommit) {
    metaClient = HoodieTableMetaClient.reload(metaClient);
    JavaRDD records = jsc.parallelize(dataGen.generateInserts(instant, numRecords), 2);
    metaClient = HoodieTableMetaClient.reload(metaClient);
    WriteClientTestUtils.startCommitWithTime(client, instant);
    List<WriteStatus> writeStatuses;
    if (isUpsert) {
      writeStatuses = client.upsert(records, instant).collect();
    } else {
      writeStatuses = client.insert(records, instant).collect();
    }
    org.apache.hudi.testutils.Assertions.assertNoWriteErrors(writeStatuses);
    if (doCommit) {
      List<HoodieWriteStat> writeStats = writeStatuses.stream().map(WriteStatus::getStat).collect(Collectors.toList());
      boolean committed = client.commitStats(instant, writeStats, Option.empty(), metaClient.getCommitActionType());
      Assertions.assertTrue(committed);
    }
    metaClient = HoodieTableMetaClient.reload(metaClient);
    return writeStatuses;
  }

  // -------------------------------------------------------------------------
  //  Inner Class
  // -------------------------------------------------------------------------
  static class TestHelpers {
    static void assertNCompletedCommits(int expected, String tablePath) {
      HoodieTableMetaClient meta = createMetaClient(storage, tablePath);
      HoodieTimeline timeline = meta.getActiveTimeline().getWriteTimeline().filterCompletedInstants();
      LOG.info("Timeline Instants=" + meta.getActiveTimeline().getInstants());
      int numCommits = timeline.countInstants();
      assertEquals(expected, numCommits, "Got=" + numCommits + ", exp =" + expected);
    }

    static void assertNCleanCommits(int expected, String tablePath) {
      HoodieTableMetaClient meta = createMetaClient(storage, tablePath);
      HoodieTimeline timeline = meta.getActiveTimeline().getCleanerTimeline().filterCompletedInstants();
      LOG.info("Timeline Instants=" + meta.getActiveTimeline().getInstants());
      int numCleanCommits = timeline.countInstants();
      assertEquals(expected, numCleanCommits, "Got=" + numCleanCommits + ", exp =" + expected);
    }

    static void assertNClusteringCommits(int expected, String tablePath) {
      HoodieTableMetaClient meta = createMetaClient(storage, tablePath);
      HoodieTimeline timeline = meta.getActiveTimeline().getCompletedReplaceTimeline();
      LOG.info("Timeline Instants=" + meta.getActiveTimeline().getInstants());
      int numCommits = timeline.countInstants();
      assertEquals(expected, numCommits, "Got=" + numCommits + ", exp =" + expected);
    }
  }
}
