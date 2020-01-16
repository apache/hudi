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

package org.apache.hudi.utilities;

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.common.HoodieTestDataGenerator;
import org.apache.hudi.common.util.TypedProperties;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.deltastreamer.HoodieMultiTableDeltaStreamer;
import org.apache.hudi.utilities.deltastreamer.TableExecutionObject;
import org.apache.hudi.utilities.schema.FilebasedSchemaProvider;
import org.apache.hudi.utilities.sources.JsonKafkaSource;
import org.apache.hudi.utilities.sources.TestDataSource;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestHoodieMultiTableDeltaStreamer extends TestHoodieDeltaStreamer {

  private static volatile Logger log = LogManager.getLogger(TestHoodieMultiTableDeltaStreamer.class);

  static class TestHelpers {

    static String[] createArgsArray(String fileName, String sourceClassName, boolean enableHiveSync) {
      List<String> argsList = new ArrayList<>();
      argsList.add("--custom-props");
      argsList.add(dfsBasePath + "/" + fileName);
      argsList.add("--target-table");
      argsList.add("dummy_table");
      argsList.add("--base-path-prefix");
      argsList.add(dfsBasePath + "/multi_table_dataset");
      argsList.add("--props");
      argsList.add(dfsBasePath + "/" + PROPS_FILENAME_TEST_SOURCE1);
      argsList.add("--storage-type");
      argsList.add("COPY_ON_WRITE");
      argsList.add("--source-class");
      argsList.add(sourceClassName);
      argsList.add("--source-ordering-field");
      argsList.add("timestamp");
      argsList.add("--schemaprovider-class");
      argsList.add(FilebasedSchemaProvider.class.getName());
      argsList.add("--target-base-path");
      argsList.add(dfsBasePath + "/multi_table_dataset");
      if (enableHiveSync) {
        argsList.add("--enable-hive-sync");
      }

      return argsList.toArray(new String[0]);
    }
  }

  @Test
  public void testInvalidHiveSyncProps() {
    String[] args = TestHelpers.createArgsArray("invalid_hive_sync_custom_config.json", TestDataSource.class.getName(), true);
    try {
      new HoodieMultiTableDeltaStreamer(args, jsc);
      fail("Should fail when hive sync table not provided with enableHiveSync flag");
    } catch (HoodieException he) {
      log.error("Expected error when creating table execution objects", he);
      assertTrue(he.getMessage().contains("Hive sync table field not provided!"));
    }
  }

  @Test
  public void testCustomConfigProps() {
    HoodieMultiTableDeltaStreamer streamer = new HoodieMultiTableDeltaStreamer(TestHelpers
        .createArgsArray("custom_config.json", TestDataSource.class.getName(), true), jsc);
    TableExecutionObject executionObject = streamer.getTableExecutionObjects().get(0);
    assertEquals(streamer.getTableExecutionObjects().size(), 2);
    assertEquals(executionObject.getConfig().targetBasePath, dfsBasePath + "/multi_table_dataset/uber_db/dummy_table_uber");
    assertEquals(executionObject.getConfig().targetTableName, "uber_db.dummy_table_uber");
    assertEquals(executionObject.getProperties().getString(HoodieMultiTableDeltaStreamer.Constants.KAFKA_TOPIC_PROP), "topic1");
    assertEquals(executionObject.getProperties().getString(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY()), "_row_key");
    assertEquals(executionObject.getProperties().getString(DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY()), TestHoodieDeltaStreamer.TestGenerator.class.getName());
    assertEquals(executionObject.getProperties().getString(HoodieMultiTableDeltaStreamer.Constants.HIVE_SYNC_TABLE_PROP), "uber_hive_dummy_table");
  }

  @Test
  public void testInvalidIngestionProps() {
    try {
      new HoodieMultiTableDeltaStreamer(TestHelpers.createArgsArray("invalid_ingestion_custom_config.json",
        TestDataSource.class.getName(), true), jsc);
      fail("Creation of execution object should fail without kafka topic");
    } catch (Exception e) {
      log.error("Creation of execution object failed with error: " + e.getMessage(), e);
      assertTrue(e.getMessage().contains("Please provide valid table config arguments!"));
    }
  }

  @Test
  public void testMultiTableExecution() {
    //create topics for each table
    testUtils.createTopic("topic1", 2);
    testUtils.createTopic("topic2", 2);

    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    testUtils.sendMessages("topic1", Helpers.jsonifyRecords(dataGenerator.generateInsertsAsPerSchema("000", 5, HoodieTestDataGenerator.TRIP_UBER_EXAMPLE_SCHEMA)));
    testUtils.sendMessages("topic2", Helpers.jsonifyRecords(dataGenerator.generateInsertsAsPerSchema("000", 10, HoodieTestDataGenerator.GROCERY_PURCHASE_SCHEMA)));

    HoodieMultiTableDeltaStreamer streamer = new HoodieMultiTableDeltaStreamer(TestHelpers.createArgsArray("custom_config.json",
        JsonKafkaSource.class.getName(), false), jsc);
    List<TableExecutionObject> executionObjects = streamer.getTableExecutionObjects();
    TypedProperties properties = executionObjects.get(0).getProperties();
    properties.setProperty("hoodie.deltastreamer.source.kafka.topic", "topic1");
    properties.setProperty("hoodie.deltastreamer.schemaprovider.source.schema.file", dfsBasePath + "/source_uber.avsc");
    properties.setProperty("hoodie.deltastreamer.schemaprovider.target.schema.file", dfsBasePath + "/target_uber.avsc");
    executionObjects.get(0).setProperties(properties);
    String targetBasePath1 = executionObjects.get(0).getConfig().targetBasePath;
    String targetBasePath2 = executionObjects.get(1).getConfig().targetBasePath;
    streamer.sync();

    TestHoodieDeltaStreamer.TestHelpers.assertRecordCount(5, targetBasePath1 + "/*/*.parquet", sqlContext);
    TestHoodieDeltaStreamer.TestHelpers.assertRecordCount(10, targetBasePath2 + "/*/*.parquet", sqlContext);

    //insert updates for already existing records in kafka topics
    testUtils.sendMessages("topic1", Helpers.jsonifyRecords(dataGenerator.generateUpdatesAsPerSchema("001", 5, HoodieTestDataGenerator.TRIP_UBER_EXAMPLE_SCHEMA)));
    testUtils.sendMessages("topic2", Helpers.jsonifyRecords(dataGenerator.generateUpdatesAsPerSchema("001", 10, HoodieTestDataGenerator.GROCERY_PURCHASE_SCHEMA)));
    streamer.sync();
    assertEquals(streamer.getSuccessTopics().size(), 2);
    assertTrue(streamer.getFailedTopics().isEmpty());

    //assert the record count matches now
    TestHoodieDeltaStreamer.TestHelpers.assertRecordCount(5, targetBasePath1 + "/*/*.parquet", sqlContext);
    TestHoodieDeltaStreamer.TestHelpers.assertRecordCount(10, targetBasePath2 + "/*/*.parquet", sqlContext);
  }
}
