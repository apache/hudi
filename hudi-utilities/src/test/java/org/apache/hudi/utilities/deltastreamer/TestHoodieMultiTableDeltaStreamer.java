/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.utilities.deltastreamer;

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.sync.common.HoodieSyncConfig;
import org.apache.hudi.utilities.config.HoodieSchemaProviderConfig;
import org.apache.hudi.utilities.config.HoodieStreamerConfig;
import org.apache.hudi.utilities.schema.FilebasedSchemaProvider;
import org.apache.hudi.utilities.schema.SchemaRegistryProvider;
import org.apache.hudi.utilities.sources.JsonKafkaSource;
import org.apache.hudi.utilities.sources.ParquetDFSSource;
import org.apache.hudi.utilities.sources.TestDataSource;
import org.apache.hudi.utilities.streamer.TableExecutionContext;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieMultiTableDeltaStreamer extends HoodieDeltaStreamerTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(TestHoodieMultiTableDeltaStreamer.class);

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
      config.basePathPrefix = basePath + "/" + basePathPrefix;
      config.propsFilePath = basePath + "/" + fileName;
      config.tableType = "COPY_ON_WRITE";
      config.sourceClassName = sourceClassName;
      config.sourceOrderingField = "timestamp";
      if (setSchemaProvider) {
        config.schemaProviderClassName = clazz != null ? clazz.getName() : FilebasedSchemaProvider.class.getName();
      }
      config.enableHiveSync = enableHiveSync;
      config.enableMetaSync = enableMetaSync;
      config.syncClientToolClassNames = "com.example.DummySyncTool1,com.example.DummySyncTool2";
      return config;
    }
  }

  @Test
  public void testEmptyTransformerProps() throws IOException {
    // HUDI-4630: If there is no transformer props passed through, don't populate the transformerClassNames
    HoodieMultiTableDeltaStreamer.Config cfg = TestHelpers.getConfig(PROPS_FILENAME_TEST_SOURCE1, basePath + "/config", TestDataSource.class.getName(), false, false, null);
    HoodieDeltaStreamer.Config dsConfig = new HoodieDeltaStreamer.Config();
    TypedProperties tblProperties = new TypedProperties();
    HoodieMultiTableDeltaStreamer streamer = new HoodieMultiTableDeltaStreamer(cfg, jsc);
    assertNull(cfg.transformerClassNames);
  }
  
  @Test
  public void testMetaSyncConfig() throws IOException {
    HoodieMultiTableDeltaStreamer.Config cfg = TestHelpers.getConfig(PROPS_FILENAME_TEST_SOURCE1, basePath + "/config", TestDataSource.class.getName(), true, true, null);
    HoodieMultiTableDeltaStreamer streamer = new HoodieMultiTableDeltaStreamer(cfg, jsc);
    TableExecutionContext executionContext = streamer.getTableExecutionContexts().get(1);
    assertEquals("com.example.DummySyncTool1,com.example.DummySyncTool2", executionContext.getConfig().syncClientToolClassNames);
  }

  @Test
  public void testInvalidHiveSyncProps() throws IOException {
    HoodieMultiTableDeltaStreamer.Config cfg = TestHelpers.getConfig(PROPS_INVALID_HIVE_SYNC_TEST_SOURCE1, basePath + "/config", TestDataSource.class.getName(), true, true, null);
    Exception e = assertThrows(HoodieException.class, () -> {
      new HoodieMultiTableDeltaStreamer(cfg, jsc);
    }, "Should fail when hive sync table not provided with enableHiveSync flag");
    LOG.debug("Expected error when creating table execution objects", e);
    assertTrue(e.getMessage().contains("Meta sync table field not provided!"));
  }

  @Test
  public void testInvalidPropsFilePath() throws IOException {
    HoodieMultiTableDeltaStreamer.Config cfg = TestHelpers.getConfig(PROPS_INVALID_FILE, basePath + "/config", TestDataSource.class.getName(), true, true, null);
    Exception e = assertThrows(IllegalArgumentException.class, () -> {
      new HoodieMultiTableDeltaStreamer(cfg, jsc);
    }, "Should fail when invalid props file is provided");
    LOG.debug("Expected error when creating table execution objects", e);
    assertTrue(e.getMessage().contains("Please provide valid common config file path!"));
  }

  @Test
  public void testInvalidTableConfigFilePath() throws IOException {
    HoodieMultiTableDeltaStreamer.Config cfg = TestHelpers.getConfig(PROPS_INVALID_TABLE_CONFIG_FILE, basePath + "/config", TestDataSource.class.getName(), true, true, null);
    Exception e = assertThrows(IllegalArgumentException.class, () -> {
      new HoodieMultiTableDeltaStreamer(cfg, jsc);
    }, "Should fail when invalid table config props file path is provided");
    LOG.debug("Expected error when creating table execution objects", e);
    assertTrue(e.getMessage().contains("Please provide valid table config file path!"));
  }

  @Test
  public void testCustomConfigProps() throws IOException {
    HoodieMultiTableDeltaStreamer.Config cfg = TestHelpers.getConfig(PROPS_FILENAME_TEST_SOURCE1, basePath + "/config", TestDataSource.class.getName(), false, false, SchemaRegistryProvider.class);
    HoodieMultiTableDeltaStreamer streamer = new HoodieMultiTableDeltaStreamer(cfg, jsc);
    TableExecutionContext executionContext = streamer.getTableExecutionContexts().get(1);
    assertEquals(2, streamer.getTableExecutionContexts().size());
    assertEquals(basePath + "/multi_table_dataset/uber_db/dummy_table_uber", executionContext.getConfig().targetBasePath);
    assertEquals("uber_db.dummy_table_uber", executionContext.getConfig().targetTableName);
    assertEquals("topic1",
        getStringWithAltKeys(executionContext.getProperties(), HoodieStreamerConfig.KAFKA_TOPIC));
    assertEquals("_row_key",
        getStringWithAltKeys(executionContext.getProperties(), DataSourceWriteOptions.RECORDKEY_FIELD()));
    assertEquals(TestHoodieDeltaStreamer.TestGenerator.class.getName(),
        getStringWithAltKeys(executionContext.getProperties(), DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME()));
    assertEquals("uber_hive_dummy_table",
        getStringWithAltKeys(executionContext.getProperties(), HoodieSyncConfig.META_SYNC_TABLE_NAME));
    assertEquals("http://localhost:8081/subjects/random-value/versions/latest",
        getStringWithAltKeys(executionContext.getProperties(), HoodieSchemaProviderConfig.SRC_SCHEMA_REGISTRY_URL));
    assertEquals("http://localhost:8081/subjects/topic2-value/versions/latest",
        getStringWithAltKeys(streamer.getTableExecutionContexts().get(0).getProperties(),
            HoodieSchemaProviderConfig.SRC_SCHEMA_REGISTRY_URL));
  }

  @Test //0 corresponds to fg
  public void testMultiTableExecutionWithKafkaSource() throws IOException {
    //create topics for each table
    String topicName1 = "topic" + testNum++;
    String topicName2 = "topic" + testNum;
    try {
      createTopic(topicName1, 2);
      createTopic(topicName2, 2);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create topics", e);
    }

    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    sendMessages(topicName1, Helpers.jsonifyRecords(dataGenerator.generateInsertsAsPerSchema("000", 5, HoodieTestDataGenerator.TRIP_SCHEMA, 0L)));
    sendMessages(topicName2, Helpers.jsonifyRecords(dataGenerator.generateInsertsAsPerSchema("000", 10, HoodieTestDataGenerator.SHORT_TRIP_SCHEMA, 0L)));

    HoodieMultiTableDeltaStreamer.Config cfg = TestHelpers.getConfig(PROPS_FILENAME_TEST_SOURCE1, basePath + "/config", JsonKafkaSource.class.getName(), false, false, null);
    HoodieMultiTableDeltaStreamer streamer = new HoodieMultiTableDeltaStreamer(cfg, jsc);
    List<TableExecutionContext> executionContexts = streamer.getTableExecutionContexts();
    TypedProperties properties = executionContexts.get(1).getProperties();
    properties.setProperty("hoodie.streamer.schemaprovider.source.schema.file", basePath + "/source_uber.avsc");
    properties.setProperty("hoodie.streamer.schemaprovider.target.schema.file", basePath + "/target_uber.avsc");
    properties.setProperty("hoodie.datasource.write.partitionpath.field", "timestamp");
    properties.setProperty("hoodie.streamer.source.kafka.topic", topicName2);
    executionContexts.get(1).setProperties(properties);
    TypedProperties properties1 = executionContexts.get(0).getProperties();
    properties1.setProperty("hoodie.streamer.schemaprovider.source.schema.file", basePath + "/source_short_trip_uber.avsc");
    properties1.setProperty("hoodie.streamer.schemaprovider.target.schema.file", basePath + "/target_short_trip_uber.avsc");
    properties1.setProperty("hoodie.datasource.write.partitionpath.field", "timestamp");
    properties1.setProperty("hoodie.streamer.source.kafka.topic", topicName1);
    executionContexts.get(0).setProperties(properties1);
    String targetBasePath1 = executionContexts.get(0).getConfig().targetBasePath;
    String targetBasePath2 = executionContexts.get(1).getConfig().targetBasePath;
    streamer.sync();

    assertRecordCount(5, targetBasePath1, sqlContext);
    assertRecordCount(10, targetBasePath2, sqlContext);

    //insert updates for already existing records in kafka topics
    sendMessages(topicName1, Helpers.jsonifyRecords(dataGenerator.generateUniqueUpdatesStream("001", 5, HoodieTestDataGenerator.TRIP_SCHEMA, 0L)
        .collect(Collectors.toList())));
    sendMessages(topicName2, Helpers.jsonifyRecords(dataGenerator.generateUniqueUpdatesStream("001", 10, HoodieTestDataGenerator.SHORT_TRIP_SCHEMA, 0L)
        .collect(Collectors.toList())));

    streamer = new HoodieMultiTableDeltaStreamer(cfg, jsc);
    streamer.getTableExecutionContexts().get(1).setProperties(properties);
    streamer.getTableExecutionContexts().get(0).setProperties(properties1);
    streamer.sync();

    assertEquals(2, streamer.getSuccessTables().size());
    assertTrue(streamer.getFailedTables().isEmpty());

    //assert the record count matches now
    assertRecordCount(5, targetBasePath1, sqlContext);
    assertRecordCount(10, targetBasePath2, sqlContext);
    testNum++;
  }

  @Test
  public void testMultiTableExecutionWithParquetSource() throws IOException {
    // ingest test data to 2 parquet source paths
    String parquetSourceRoot1 = basePath + "/parquetSrcPath1/";
    prepareParquetDFSFiles(10, parquetSourceRoot1);
    String parquetSourceRoot2 = basePath + "/parquetSrcPath2/";
    prepareParquetDFSFiles(5, parquetSourceRoot2);

    // add only common props. later we can add per table props
    String parquetPropsFile = populateCommonPropsAndWriteToFile();

    HoodieMultiTableDeltaStreamer.Config cfg = TestHelpers.getConfig(parquetPropsFile, basePath + "/config", ParquetDFSSource.class.getName(), false, false,
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
    HoodieMultiTableDeltaStreamer.Config cfg = TestHelpers.getConfig(PROPS_FILENAME_TEST_SOURCE1, basePath + "/config", TestDataSource.class.getName(), false, false, null);
    HoodieMultiTableDeltaStreamer streamer = new HoodieMultiTableDeltaStreamer(cfg, jsc);
    List<TableExecutionContext> tableExecutionContexts = streamer.getTableExecutionContexts();
    tableExecutionContexts.forEach(tableExecutionContext -> {
      switch (tableExecutionContext.getTableName()) {
        case "dummy_table_short_trip":
          String tableLevelKeyGeneratorClass = tableExecutionContext.getProperties().getString(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME().key());
          assertEquals(TestHoodieDeltaStreamer.TestGenerator.class.getName(), tableLevelKeyGeneratorClass);
          List<String> transformerClass = tableExecutionContext.getConfig().transformerClassNames;
          assertEquals(1, transformerClass.size());
          assertEquals("org.apache.hudi.utilities.deltastreamer.TestHoodieDeltaStreamer$TestIdentityTransformer", transformerClass.get(0));
          break;
        default:
          String defaultKeyGeneratorClass = tableExecutionContext.getProperties().getString(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME().key());
          assertEquals(TestHoodieDeltaStreamer.TestGenerator.class.getName(), defaultKeyGeneratorClass);
          assertNull(tableExecutionContext.getConfig().transformerClassNames);
      }
    });
  }

  private String populateCommonPropsAndWriteToFile() throws IOException {
    TypedProperties commonProps = new TypedProperties();
    populateCommonProps(commonProps, basePath);
    UtilitiesTestBase.Helpers.savePropsToDFS(
        commonProps, storage, basePath + "/" + PROPS_FILENAME_TEST_PARQUET);
    return PROPS_FILENAME_TEST_PARQUET;
  }

  private TypedProperties getParquetProps(String parquetSourceRoot) {
    TypedProperties props = new TypedProperties();
    props.setProperty("include", "base.properties");
    props.setProperty("hoodie.datasource.write.recordkey.field", "_row_key");
    props.setProperty("hoodie.datasource.write.partitionpath.field", "partition_path");
    props.setProperty("hoodie.streamer.source.dfs.root", parquetSourceRoot);
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
    assertRecordCount(table1ExpectedRecords, targetBasePath1, sqlContext);
    assertRecordCount(table2ExpectedRecords, targetBasePath2, sqlContext);
  }
}
