/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.hudi;

import org.apache.avro.Schema;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.testutils.SparkDatasetTestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Properties;

import static org.apache.hudi.common.config.HoodieReaderConfig.FILE_GROUP_READER_ENABLED;
import static org.apache.hudi.common.config.HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT;
import static org.apache.hudi.common.model.HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY;
import static org.apache.hudi.common.testutils.HoodieTestUtils.RAW_TRIPS_TEST_NAME;
import static org.apache.hudi.config.HoodieWriteConfig.RECORD_MERGER_IMPLS;
import static org.apache.hudi.config.HoodieWriteConfig.WRITE_RECORD_POSITIONS;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieFileGroupReaderForSpark extends SparkClientFunctionalTestHarness {
  private static final String SCHEMA_NAME = RAW_TRIPS_TEST_NAME;
  private static final String SCHEMA_NAME_SPACE = RAW_TRIPS_TEST_NAME + "_NS";
  private static final Schema TEST_SCHEMA = AvroConversionUtils.convertStructTypeToAvroSchema(
      SparkDatasetTestUtils.STRUCT_TYPE, SCHEMA_NAME, SCHEMA_NAME_SPACE);
  private static final String PARTITION_PATH = "2023-10-10";
  private HoodieTableMetaClient metaClient;

  @BeforeEach
  public void setUp() throws IOException {
    Properties properties = new Properties();
    properties.setProperty(
        HoodieTableConfig.BASE_FILE_FORMAT.key(),
        HoodieTableConfig.BASE_FILE_FORMAT.defaultValue().toString());
    properties.setProperty(
        PAYLOAD_ORDERING_FIELD_PROP_KEY,
        HoodieRecord.HoodieMetadataField.RECORD_KEY_METADATA_FIELD.getFieldName());
    metaClient = getHoodieMetaClient(hadoopConf(), basePath(), HoodieTableType.MERGE_ON_READ, properties);
  }

  @Test
  public void testFileGroupReaderWithKeyBasedMerge() {
    HoodieWriteConfig writeConfig = createWriteConfig(TEST_SCHEMA, false);
    HoodieRecordMerger merger = writeConfig.getRecordMerger();
    assertTrue(merger instanceof HoodieSparkRecordMerger);
    assertTrue(writeConfig.getBooleanOrDefault(FILE_GROUP_READER_ENABLED.key(), true));


  }

  public HoodieWriteConfig createWriteConfig(Schema avroSchema, boolean shouldWritePosition) {
    Properties extraProperties = new Properties();
    extraProperties.setProperty(
        RECORD_MERGER_IMPLS.key(),
        "org.apache.hudi.HoodieSparkRecordMerger");
    extraProperties.setProperty(
        LOGFILE_DATA_BLOCK_FORMAT.key(),
        "parquet");
    extraProperties.setProperty(
        PAYLOAD_ORDERING_FIELD_PROP_KEY,
        HoodieRecord.HoodieMetadataField.RECORD_KEY_METADATA_FIELD.getFieldName());
    extraProperties.setProperty(
        WRITE_RECORD_POSITIONS.key(),
        String.valueOf(shouldWritePosition));

    return getConfigBuilder(true)
        .withPath(basePath())
        .withSchema(avroSchema.toString())
        .withProperties(extraProperties)
        .build();
  }
}
