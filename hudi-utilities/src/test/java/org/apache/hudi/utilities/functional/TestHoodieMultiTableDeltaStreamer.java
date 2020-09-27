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

package org.apache.hudi.utilities.functional;

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.deltastreamer.HoodieMultiTableDeltaStreamer;
import org.apache.hudi.utilities.deltastreamer.TableExecutionContext;
import org.apache.hudi.utilities.schema.FilebasedSchemaProvider;
import org.apache.hudi.utilities.sources.JsonKafkaSource;
import org.apache.hudi.utilities.sources.TestDataSource;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieMultiTableDeltaStreamer extends TestHoodieDeltaStreamer {

  private static volatile Logger log = LogManager.getLogger(TestHoodieMultiTableDeltaStreamer.class);

  static class TestHelpers {

    static HoodieMultiTableDeltaStreamer.Config getConfig(String fileName, String configFolder, String sourceClassName, boolean enableHiveSync) {
      HoodieMultiTableDeltaStreamer.Config config = new HoodieMultiTableDeltaStreamer.Config();
      config.configFolder = configFolder;
      config.targetTableName = "dummy_table";
      config.basePathPrefix = dfsBasePath + "/multi_table_dataset";
      config.propsFilePath = dfsBasePath + "/" + fileName;
      config.tableType = "COPY_ON_WRITE";
      config.sourceClassName = sourceClassName;
      config.sourceOrderingField = "timestamp";
      config.schemaProviderClassName = FilebasedSchemaProvider.class.getName();
      config.enableHiveSync = enableHiveSync;
      return config;
    }
  }

  @Test
  public void testInvalidHiveSyncProps() throws IOException {
    HoodieMultiTableDeltaStreamer.Config cfg = TestHelpers.getConfig(PROPS_INVALID_HIVE_SYNC_TEST_SOURCE1, dfsBasePath + "/config", TestDataSource.class.getName(), true);
    Exception e = assertThrows(HoodieException.class, () -> {
      new HoodieMultiTableDeltaStreamer(cfg, jsc);
    }, "Should fail when hive sync table not provided with enableHiveSync flag");
    log.debug("Expected error when creating table execution objects", e);
    assertTrue(e.getMessage().contains("Hive sync table field not provided!"));
  }

  @Test
  public void testInvalidPropsFilePath() throws IOException {
    HoodieMultiTableDeltaStreamer.Config cfg = TestHelpers.getConfig(PROPS_INVALID_FILE, dfsBasePath + "/config", TestDataSource.class.getName(), true);
    Exception e = assertThrows(IllegalArgumentException.class, () -> {
      new HoodieMultiTableDeltaStreamer(cfg, jsc);
    }, "Should fail when invalid props file is provided");
    log.debug("Expected error when creating table execution objects", e);
    assertTrue(e.getMessage().contains("Please provide valid common config file path!"));
  }

  @Test
  public void testInvalidTableConfigFilePath() throws IOException {
    HoodieMultiTableDeltaStreamer.Config cfg = TestHelpers.getConfig(PROPS_INVALID_TABLE_CONFIG_FILE, dfsBasePath + "/config", TestDataSource.class.getName(), true);
    Exception e = assertThrows(IllegalArgumentException.class, () -> {
      new HoodieMultiTableDeltaStreamer(cfg, jsc);
    }, "Should fail when invalid table config props file path is provided");
    log.debug("Expected error when creating table execution objects", e);
    assertTrue(e.getMessage().contains("Please provide valid table config file path!"));
  }

  @Test
  public void testCustomConfigProps() throws IOException {
    HoodieMultiTableDeltaStreamer.Config cfg = TestHelpers.getConfig(PROPS_FILENAME_TEST_SOURCE1, dfsBasePath + "/config", TestDataSource.class.getName(), false);
    HoodieMultiTableDeltaStreamer streamer = new HoodieMultiTableDeltaStreamer(cfg, jsc);
    TableExecutionContext executionContext = streamer.getTableExecutionContexts().get(1);
    assertEquals(2, streamer.getTableExecutionContexts().size());
    assertEquals(dfsBasePath + "/multi_table_dataset/uber_db/dummy_table_uber", executionContext.getConfig().targetBasePath);
    assertEquals("uber_db.dummy_table_uber", executionContext.getConfig().targetTableName);
    assertEquals("topic1", executionContext.getProperties().getString(HoodieMultiTableDeltaStreamer.Constants.KAFKA_TOPIC_PROP));
    assertEquals("_row_key", executionContext.getProperties().getString(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY()));
    assertEquals(TestHoodieDeltaStreamer.TestGenerator.class.getName(), executionContext.getProperties().getString(DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY()));
    assertEquals("uber_hive_dummy_table", executionContext.getProperties().getString(HoodieMultiTableDeltaStreamer.Constants.HIVE_SYNC_TABLE_PROP));
  }

  @Test
  @Disabled
  public void testInvalidIngestionProps() {
    Exception e = assertThrows(Exception.class, () -> {
      HoodieMultiTableDeltaStreamer.Config cfg = TestHelpers.getConfig(PROPS_FILENAME_TEST_SOURCE1, dfsBasePath + "/config", TestDataSource.class.getName(), true);
      new HoodieMultiTableDeltaStreamer(cfg, jsc);
    }, "Creation of execution object should fail without kafka topic");
    log.debug("Creation of execution object failed with error: " + e.getMessage(), e);
    assertTrue(e.getMessage().contains("Please provide valid table config arguments!"));
  }

  @Test //0 corresponds to fg
  public void testMultiTableExecution() throws IOException {
    //create topics for each table
    testUtils.createTopic("topic1", 2);
    testUtils.createTopic("topic2", 2);

    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    testUtils.sendMessages("topic1", Helpers.jsonifyRecords(dataGenerator.generateInsertsAsPerSchema("000", 5, HoodieTestDataGenerator.TRIP_SCHEMA)));
    testUtils.sendMessages("topic2", Helpers.jsonifyRecords(dataGenerator.generateInsertsAsPerSchema("000", 10, HoodieTestDataGenerator.SHORT_TRIP_SCHEMA)));

    HoodieMultiTableDeltaStreamer.Config cfg = TestHelpers.getConfig(PROPS_FILENAME_TEST_SOURCE1,dfsBasePath + "/config", JsonKafkaSource.class.getName(), false);
    HoodieMultiTableDeltaStreamer streamer = new HoodieMultiTableDeltaStreamer(cfg, jsc);
    List<TableExecutionContext> executionContexts = streamer.getTableExecutionContexts();
    TypedProperties properties = executionContexts.get(1).getProperties();
    properties.setProperty("hoodie.deltastreamer.schemaprovider.source.schema.file", dfsBasePath + "/source_uber.avsc");
    properties.setProperty("hoodie.deltastreamer.schemaprovider.target.schema.file", dfsBasePath + "/target_uber.avsc");
    executionContexts.get(1).setProperties(properties);
    TypedProperties properties1 = executionContexts.get(0).getProperties();
    properties1.setProperty("hoodie.deltastreamer.schemaprovider.source.schema.file", dfsBasePath + "/source_short_trip_uber.avsc");
    properties1.setProperty("hoodie.deltastreamer.schemaprovider.target.schema.file", dfsBasePath + "/target_short_trip_uber.avsc");
    executionContexts.get(0).setProperties(properties1);
    String targetBasePath1 = executionContexts.get(1).getConfig().targetBasePath;
    String targetBasePath2 = executionContexts.get(0).getConfig().targetBasePath;
    streamer.sync();

    TestHoodieDeltaStreamer.TestHelpers.assertRecordCount(5, targetBasePath1 + "/*/*.parquet", sqlContext);
    TestHoodieDeltaStreamer.TestHelpers.assertRecordCount(10, targetBasePath2 + "/*/*.parquet", sqlContext);

    //insert updates for already existing records in kafka topics
    testUtils.sendMessages("topic1", Helpers.jsonifyRecords(dataGenerator.generateUpdatesAsPerSchema("001", 5, HoodieTestDataGenerator.TRIP_SCHEMA)));
    testUtils.sendMessages("topic2", Helpers.jsonifyRecords(dataGenerator.generateUpdatesAsPerSchema("001", 10, HoodieTestDataGenerator.SHORT_TRIP_SCHEMA)));
    streamer.sync();
    assertEquals(2, streamer.getSuccessTables().size());
    assertTrue(streamer.getFailedTables().isEmpty());

    //assert the record count matches now
    TestHoodieDeltaStreamer.TestHelpers.assertRecordCount(5, targetBasePath1 + "/*/*.parquet", sqlContext);
    TestHoodieDeltaStreamer.TestHelpers.assertRecordCount(10, targetBasePath2 + "/*/*.parquet", sqlContext);
  }
}
