/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.utilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.uber.hoodie.common.model.HoodieCommitMetadata;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.util.DFSPropertiesConfiguration;
import com.uber.hoodie.common.util.TypedProperties;
import com.uber.hoodie.exception.DatasetNotFoundException;
import com.uber.hoodie.utilities.deltastreamer.HoodieDeltaStreamer;
import com.uber.hoodie.utilities.deltastreamer.HoodieDeltaStreamer.Operation;
import com.uber.hoodie.utilities.sources.TestDataSource;
import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SQLContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Basic tests against {@link com.uber.hoodie.utilities.deltastreamer.HoodieDeltaStreamer}, by issuing bulk_inserts,
 * upserts, inserts. Check counts at the end.
 */
public class TestHoodieDeltaStreamer extends UtilitiesTestBase {

  private static volatile Logger log = LogManager.getLogger(TestHoodieDeltaStreamer.class);

  @BeforeClass
  public static void initClass() throws Exception {
    UtilitiesTestBase.initClass();

    // prepare the configs.
    UtilitiesTestBase.Helpers.copyToDFS("delta-streamer-config/base.properties", dfs, dfsBasePath + "/base.properties");
    UtilitiesTestBase.Helpers.copyToDFS("delta-streamer-config/source.avsc", dfs, dfsBasePath + "/source.avsc");
    TypedProperties props = new TypedProperties();
    props.setProperty("include", "base.properties");
    props.setProperty("hoodie.datasource.write.recordkey.field", "_row_key");
    props.setProperty("hoodie.datasource.write.partitionpath.field", "not_there");
    props.setProperty("hoodie.deltastreamer.schemaprovider.source.schema.file", dfsBasePath + "/source.avsc");
    UtilitiesTestBase.Helpers.savePropsToDFS(props, dfs, dfsBasePath + "/test-source.properties");
  }

  @AfterClass
  public static void cleanupClass() throws Exception {
    UtilitiesTestBase.cleanupClass();
  }

  @Before
  public void setup() throws Exception {
    super.setup();
    TestDataSource.initDataGen();
  }

  @After
  public void teardown() throws Exception {
    super.teardown();
    TestDataSource.resetDataGen();
  }

  static class TestHelpers {

    static HoodieDeltaStreamer.Config makeConfig(String basePath, Operation op) {
      HoodieDeltaStreamer.Config cfg = new HoodieDeltaStreamer.Config();
      cfg.targetBasePath = basePath;
      cfg.targetTableName = "hoodie_trips";
      cfg.storageType = "COPY_ON_WRITE";
      cfg.sourceClassName = TestDataSource.class.getName();
      cfg.operation = op;
      cfg.sourceOrderingField = "timestamp";
      cfg.propsFilePath = dfsBasePath + "/test-source.properties";
      cfg.sourceLimit = 1000;
      return cfg;
    }

    static void assertRecordCount(long expected, String datasetPath, SQLContext sqlContext) {
      long recordCount = sqlContext.read().format("com.uber.hoodie").load(datasetPath).count();
      assertEquals(expected, recordCount);
    }

    static void assertCommitMetadata(String expected, String datasetPath, FileSystem fs, int totalCommits)
        throws IOException {
      HoodieTableMetaClient meta = new HoodieTableMetaClient(fs.getConf(), datasetPath);
      HoodieTimeline timeline = meta.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
      HoodieInstant lastCommit = timeline.lastInstant().get();
      HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(
          timeline.getInstantDetails(lastCommit).get(), HoodieCommitMetadata.class);
      assertEquals(totalCommits, timeline.countInstants());
      assertEquals(expected, commitMetadata.getMetadata(HoodieDeltaStreamer.CHECKPOINT_KEY));
    }
  }

  @Test
  public void testProps() throws IOException {
    TypedProperties props = new DFSPropertiesConfiguration(dfs, new Path(dfsBasePath + "/test-source.properties"))
        .getConfig();
    assertEquals(2, props.getInteger("hoodie.upsert.shuffle.parallelism"));
    assertEquals("_row_key", props.getString("hoodie.datasource.write.recordkey.field"));
  }

  @Test
  public void testDatasetCreation() throws Exception {
    try {
      dfs.mkdirs(new Path(dfsBasePath + "/not_a_dataset"));
      HoodieDeltaStreamer deltaStreamer = new HoodieDeltaStreamer(
          TestHelpers.makeConfig(dfsBasePath + "/not_a_dataset", Operation.BULK_INSERT), jsc);
      deltaStreamer.sync();
      fail("Should error out when pointed out at a dir thats not a dataset");
    } catch (DatasetNotFoundException e) {
      //expected
      log.error("Expected error during dataset creation", e);
    }
  }

  @Test
  public void testBulkInsertsAndUpserts() throws Exception {
    String datasetBasePath = dfsBasePath + "/test_dataset";

    // Initial bulk insert
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(datasetBasePath, Operation.BULK_INSERT);
    new HoodieDeltaStreamer(cfg, jsc).sync();
    TestHelpers.assertRecordCount(1000, datasetBasePath + "/*/*.parquet", sqlContext);
    TestHelpers.assertCommitMetadata("00000", datasetBasePath, dfs, 1);

    // No new data => no commits.
    cfg.sourceLimit = 0;
    new HoodieDeltaStreamer(cfg, jsc).sync();
    TestHelpers.assertRecordCount(1000, datasetBasePath + "/*/*.parquet", sqlContext);
    TestHelpers.assertCommitMetadata("00000", datasetBasePath, dfs, 1);

    // upsert() #1
    cfg.sourceLimit = 2000;
    cfg.operation = Operation.UPSERT;
    new HoodieDeltaStreamer(cfg, jsc).sync();
    TestHelpers.assertRecordCount(2000, datasetBasePath + "/*/*.parquet", sqlContext);
    TestHelpers.assertCommitMetadata("00001", datasetBasePath, dfs, 2);
  }
}
