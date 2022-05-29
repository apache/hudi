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
import org.apache.hudi.utilities.schema.SchemaRegistryProvider;
import org.apache.hudi.utilities.sources.JsonKafkaSource;
import org.apache.hudi.utilities.sources.ParquetDFSSource;
import org.apache.hudi.utilities.sources.TestDataSource;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Disabled("Disabled due to HDFS MiniCluster jetty conflict")
@Tag("functional")
public class TestHoodieMultiTableDeltaStreamer extends HoodieDeltaStreamerTestBase {

  private static final Logger LOG = LogManager.getLogger(TestHoodieMultiTableDeltaStreamer.class);

  static class TestHelpers {

    static HoodieMultiTableDeltaStreamer.Config getConfig(String fileName, String configFolder, String sourceClassName, boolean enableHiveSync, boolean enableMetaSync,
                                                          Class<?> clazz) {
      return getConfig(fileName, configFolder, sourceClassName, enableHiveSync, enableMetaSync, true, "multi_table_dataset", clazz);
    }

    static HoodieMultiTableDeltaStreamer.Config getConfig(String fileName, String configFolder, String sourceClassName, boolean enableHiveSync, boolean enableMetaSync,
        boolean setSchemaProvider, String basePathPrefix, Class<?> clazz) {
      HoodieMultiTableDeltaStreamer.Config config = new HoodieMultiTableDeltaStreamer.Config();
      config.configFolder = configFolder;
      config.targetTableName = "dummy_table";
      config.basePathPrefix = dfsBasePath + "/" + basePathPrefix;
      config.propsFilePath = dfsBasePath + "/" + fileName;
      config.tableType = "COPY_ON_WRITE";
      config.sourceClassName = sourceClassName;
      config.sourceOrderingField = "timestamp";
      if (setSchemaProvider) {
        config.schemaProviderClassName = clazz != null ? clazz.getName() : FilebasedSchemaProvider.class.getName();
      }
      config.enableHiveSync = enableHiveSync;
      config.enableMetaSync = enableMetaSync;
      return config;
    }
  }

  @Test
  public void testInvalidHiveSyncProps() throws IOException {
    HoodieMultiTableDeltaStreamer.Config cfg = TestHelpers.getConfig(PROPS_INVALID_HIVE_SYNC_TEST_SOURCE1, dfsBasePath + "/config", TestDataSource.class.getName(), true, true, null);
    Exception e = assertThrows(HoodieException.class, () -> {
      new HoodieMultiTableDeltaStreamer(cfg, jsc);
    }, "Should fail when hive sync table not provided with enableHiveSync flag");
    LOG.debug("Expected error when creating table execution objects", e);
    assertTrue(e.getMessage().contains("Meta sync table field not provided!"));
  }

  @Test
  public void testInvalidPropsFilePath() throws IOException {
    HoodieMultiTableDeltaStreamer.Config cfg = TestHelpers.getConfig(PROPS_INVALID_FILE, dfsBasePath + "/config", TestDataSource.class.getName(), true, true, null);
    Exception e = assertThrows(IllegalArgumentException.class, () -> {
      new HoodieMultiTableDeltaStreamer(cfg, jsc);
    }, "Should fail when invalid props file is provided");
    LOG.debug("Expected error when creating table execution objects", e);
    assertTrue(e.getMessage().contains("Please provide valid common config file path!"));
  }

  @Test
  public void testInvalidTableConfigFilePath() throws IOException {
    HoodieMultiTableDeltaStreamer.Config cfg = TestHelpers.getConfig(PROPS_INVALID_TABLE_CONFIG_FILE, dfsBasePath + "/config", TestDataSource.class.getName(), true, true, null);
    Exception e = assertThrows(IllegalArgumentException.class, () -> {
      new HoodieMultiTableDeltaStreamer(cfg, jsc);
    }, "Should fail when invalid table config props file path is provided");
    LOG.debug("Expected error when creating table execution objects", e);
    assertTrue(e.getMessage().contains("Please provide valid table config file path!"));
  }

  @Test
  public void testCustomConfigProps() throws IOException {
    HoodieMultiTableDeltaStreamer.Config cfg = TestHelpers.getConfig(PROPS_FILENAME_TEST_SOURCE1, dfsBasePath + "/config", TestDataSource.class.getName(), false, false, SchemaRegistryProvider.class);
    HoodieMultiTableDeltaStreamer streamer = new HoodieMultiTableDeltaStreamer(cfg, jsc);
    TableExecutionContext executionContext = streamer.getTableExecutionContexts().get(1);
    assertEquals(2, streamer.getTableExecutionContexts().size());
    assertEquals(dfsBasePath + "/multi_table_dataset/uber_db/dummy_table_uber", executionContext.getConfig().targetBasePath);
    assertEquals("uber_db.dummy_table_uber", executionContext.getConfig().targetTableName);
    assertEquals("topic1", executionContext.getProperties().getString(HoodieMultiTableDeltaStreamer.Constants.KAFKA_TOPIC_PROP));
    assertEquals("_row_key", executionContext.getProperties().getString(DataSourceWriteOptions.RECORDKEY_FIELD().key()));
    assertEquals(TestHoodieDeltaStreamer.TestGenerator.class.getName(), executionContext.getProperties().getString(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME().key()));
    assertEquals("uber_hive_dummy_table", executionContext.getProperties().getString(HoodieMultiTableDeltaStreamer.Constants.HIVE_SYNC_TABLE_PROP));
    assertEquals("http://localhost:8081/subjects/random-value/versions/latest", executionContext.getProperties().getString(SchemaRegistryProvider.Config.SRC_SCHEMA_REGISTRY_URL_PROP));
    assertEquals("http://localhost:8081/subjects/topic2-value/versions/latest",
            streamer.getTableExecutionContexts().get(0).getProperties().getString(SchemaRegistryProvider.Config.SRC_SCHEMA_REGISTRY_URL_PROP));
  }

  @Test
  @Disabled
  public void testInvalidIngestionProps() {
    Exception e = assertThrows(Exception.class, () -> {
      HoodieMultiTableDeltaStreamer.Config cfg = TestHelpers.getConfig(PROPS_FILENAME_TEST_SOURCE1, dfsBasePath + "/config", TestDataSource.class.getName(), true, true, null);
      new HoodieMultiTableDeltaStreamer(cfg, jsc);
    }, "Creation of execution object should fail without kafka topic");
    LOG.debug("Creation of execution object failed with error: " + e.getMessage(), e);
    assertTrue(e.getMessage().contains("Please provide valid table config arguments!"));
  }

  @Test //0 corresponds to fg
  public void testMultiTableExecutionWithKafkaSource() throws IOException {
    //create topics for each table
    String topicName1 = "topic" + testNum++;
    String topicName2 = "topic" + testNum;
    testUtils.createTopic(topicName1, 2);
    testUtils.createTopic(topicName2, 2);

    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    testUtils.sendMessages(topicName1, Helpers.jsonifyRecords(dataGenerator.generateInsertsAsPerSchema("000", 5, HoodieTestDataGenerator.TRIP_SCHEMA)));
    testUtils.sendMessages(topicName2, Helpers.jsonifyRecords(dataGenerator.generateInsertsAsPerSchema("000", 10, HoodieTestDataGenerator.SHORT_TRIP_SCHEMA)));

    HoodieMultiTableDeltaStreamer.Config cfg = TestHelpers.getConfig(PROPS_FILENAME_TEST_SOURCE1, dfsBasePath + "/config", JsonKafkaSource.class.getName(), false, false, null);
    HoodieMultiTableDeltaStreamer streamer = new HoodieMultiTableDeltaStreamer(cfg, jsc);
    List<TableExecutionContext> executionContexts = streamer.getTableExecutionContexts();
    TypedProperties properties = executionContexts.get(1).getProperties();
    properties.setProperty("hoodie.deltastreamer.schemaprovider.source.schema.file", dfsBasePath + "/source_uber.avsc");
    properties.setProperty("hoodie.deltastreamer.schemaprovider.target.schema.file", dfsBasePath + "/target_uber.avsc");
    properties.setProperty("hoodie.datasource.write.partitionpath.field", "timestamp");
    properties.setProperty("hoodie.deltastreamer.source.kafka.topic", topicName2);
    executionContexts.get(1).setProperties(properties);
    TypedProperties properties1 = executionContexts.get(0).getProperties();
    properties1.setProperty("hoodie.deltastreamer.schemaprovider.source.schema.file", dfsBasePath + "/source_short_trip_uber.avsc");
    properties1.setProperty("hoodie.deltastreamer.schemaprovider.target.schema.file", dfsBasePath + "/target_short_trip_uber.avsc");
    properties1.setProperty("hoodie.datasource.write.partitionpath.field", "timestamp");
    properties1.setProperty("hoodie.deltastreamer.source.kafka.topic", topicName1);
    executionContexts.get(0).setProperties(properties1);
    String targetBasePath1 = executionContexts.get(0).getConfig().targetBasePath;
    String targetBasePath2 = executionContexts.get(1).getConfig().targetBasePath;
    streamer.sync();

    TestHoodieDeltaStreamer.TestHelpers.assertRecordCount(5, targetBasePath1, sqlContext);
    TestHoodieDeltaStreamer.TestHelpers.assertRecordCount(10, targetBasePath2, sqlContext);

    //insert updates for already existing records in kafka topics
    testUtils.sendMessages(topicName1, Helpers.jsonifyRecords(dataGenerator.generateUpdatesAsPerSchema("001", 5, HoodieTestDataGenerator.TRIP_SCHEMA)));
    testUtils.sendMessages(topicName2, Helpers.jsonifyRecords(dataGenerator.generateUpdatesAsPerSchema("001", 10, HoodieTestDataGenerator.SHORT_TRIP_SCHEMA)));

    streamer = new HoodieMultiTableDeltaStreamer(cfg, jsc);
    streamer.getTableExecutionContexts().get(1).setProperties(properties);
    streamer.getTableExecutionContexts().get(0).setProperties(properties1);
    streamer.sync();

    assertEquals(2, streamer.getSuccessTables().size());
    assertTrue(streamer.getFailedTables().isEmpty());

    //assert the record count matches now
    TestHoodieDeltaStreamer.TestHelpers.assertRecordCount(5, targetBasePath1, sqlContext);
    TestHoodieDeltaStreamer.TestHelpers.assertRecordCount(10, targetBasePath2, sqlContext);
    testNum++;
  }

  @Test
  public void testMultiTableExecutionWithParquetSource() throws IOException {
    // ingest test data to 2 parquet source paths
    String parquetSourceRoot1 = dfsBasePath + "/parquetSrcPath1/";
    prepareParquetDFSFiles(10, parquetSourceRoot1);
    String parquetSourceRoot2 = dfsBasePath + "/parquetSrcPath2/";
    prepareParquetDFSFiles(5, parquetSourceRoot2);

    // add only common props. later we can add per table props
    String parquetPropsFile = populateCommonPropsAndWriteToFile();

    HoodieMultiTableDeltaStreamer.Config cfg = TestHelpers.getConfig(parquetPropsFile, dfsBasePath + "/config", ParquetDFSSource.class.getName(), false, false,
        false, "multi_table_parquet", null);
    HoodieMultiTableDeltaStreamer streamer = new HoodieMultiTableDeltaStreamer(cfg, jsc);

    List<TableExecutionContext> executionContexts = streamer.getTableExecutionContexts();
    // fetch per parquet source props and add per table properties
    ingestPerParquetSourceProps(executionContexts, Arrays.asList(new String[] {parquetSourceRoot1, parquetSourceRoot2}));

    String targetBasePath1 = executionContexts.get(0).getConfig().targetBasePath;
    String targetBasePath2 = executionContexts.get(1).getConfig().targetBasePath;

    // sync and verify
    syncAndVerify(streamer, targetBasePath1, targetBasePath2, 10, 5);

    int totalTable1Records = 10;
    int totalTable2Records = 5;
    // ingest multiple rounds and verify
    for (int i = 0; i < 3; i++) {
      int table1Records = 10 + RANDOM.nextInt(100);
      int table2Records = 15 + RANDOM.nextInt(100);
      prepareParquetDFSFiles(table1Records, parquetSourceRoot1, (i + 2) + ".parquet", false, null, null);
      prepareParquetDFSFiles(table2Records, parquetSourceRoot2, (i + 2) + ".parquet", false, null, null);
      totalTable1Records += table1Records;
      totalTable2Records += table2Records;
      // sync and verify
      syncAndVerify(streamer, targetBasePath1, targetBasePath2, totalTable1Records, totalTable2Records);
    }
  }

  @Test
  public void testTableLevelProperties() throws IOException {
    HoodieMultiTableDeltaStreamer.Config cfg = TestHelpers.getConfig(PROPS_FILENAME_TEST_SOURCE1, dfsBasePath + "/config", TestDataSource.class.getName(), false, false, null);
    HoodieMultiTableDeltaStreamer streamer = new HoodieMultiTableDeltaStreamer(cfg, jsc);
    List<TableExecutionContext> tableExecutionContexts = streamer.getTableExecutionContexts();
    tableExecutionContexts.forEach(tableExecutionContext -> {
      switch (tableExecutionContext.getTableName()) {
        case "dummy_table_short_trip":
          String tableLevelKeyGeneratorClass = tableExecutionContext.getProperties().getString(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME().key());
          assertEquals(TestHoodieDeltaStreamer.TestTableLevelGenerator.class.getName(), tableLevelKeyGeneratorClass);
          break;
        default:
          String defaultKeyGeneratorClass = tableExecutionContext.getProperties().getString(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME().key());
          assertEquals(TestHoodieDeltaStreamer.TestGenerator.class.getName(), defaultKeyGeneratorClass);
      }
    });
  }

  private String populateCommonPropsAndWriteToFile() throws IOException {
    TypedProperties commonProps = new TypedProperties();
    populateCommonProps(commonProps, dfsBasePath);
    UtilitiesTestBase.Helpers.savePropsToDFS(commonProps, dfs, dfsBasePath + "/" + PROPS_FILENAME_TEST_PARQUET);
    return PROPS_FILENAME_TEST_PARQUET;
  }

  private TypedProperties getParquetProps(String parquetSourceRoot) {
    TypedProperties props = new TypedProperties();
    props.setProperty("include", "base.properties");
    props.setProperty("hoodie.datasource.write.recordkey.field", "_row_key");
    props.setProperty("hoodie.datasource.write.partitionpath.field", "partition_path");
    props.setProperty("hoodie.deltastreamer.source.dfs.root", parquetSourceRoot);
    return props;
  }

  private void ingestPerParquetSourceProps(List<TableExecutionContext> executionContexts, List<String> parquetSourceRoots) {
    int counter = 0;
    for (String parquetSourceRoot : parquetSourceRoots) {
      TypedProperties properties = executionContexts.get(counter).getProperties();
      TypedProperties parquetProps = getParquetProps(parquetSourceRoot);
      parquetProps.forEach((k, v) -> {
        properties.setProperty(k.toString(), v.toString());
      });
      executionContexts.get(counter).setProperties(properties);
      counter++;
    }
  }

  private void syncAndVerify(HoodieMultiTableDeltaStreamer streamer, String targetBasePath1, String targetBasePath2, long table1ExpectedRecords, long table2ExpectedRecords) {
    streamer.sync();
    TestHoodieDeltaStreamer.TestHelpers.assertRecordCount(table1ExpectedRecords, targetBasePath1, sqlContext);
    TestHoodieDeltaStreamer.TestHelpers.assertRecordCount(table2ExpectedRecords, targetBasePath2, sqlContext);
  }
}
