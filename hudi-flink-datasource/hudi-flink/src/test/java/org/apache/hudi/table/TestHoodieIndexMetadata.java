/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;

import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS;

public class TestHoodieIndexMetadata {
  private Configuration conf;

  @TempDir
  File tempFile;

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testColumnStatsIndexMetadata(boolean colStatsEnabled) throws Exception {
    final String path = tempFile.getAbsolutePath();
    conf = TestConfigurations.getDefaultConf(path);
    conf.setString(HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key(), colStatsEnabled + "");
    TestData.writeData(TestData.DATA_SET_INSERT, conf);

    HoodieTableMetaClient metaClient = StreamerUtil.initTableIfNotExists(conf);
    HoodieIndexDefinition indexDefinition = metaClient.getIndexMetadata().map(indexMeta -> indexMeta.getIndexDefinitions().get(PARTITION_NAME_COLUMN_STATS)).orElse(null);
    Assertions.assertTrue(!colStatsEnabled || indexDefinition != null);
    if (colStatsEnabled) {
      String[] expectedCols = new String[] {"_hoodie_commit_time", "_hoodie_partition_path", "_hoodie_record_key", "uuid", "name", "age", "ts", "partition"};
      Assertions.assertArrayEquals(expectedCols, indexDefinition.getSourceFields().toArray(new String[0]));
    }
  }

  @Test
  void testIndexMetadataWithSpecifiedColumns() throws Exception {
    final String path = tempFile.getAbsolutePath();
    conf = TestConfigurations.getDefaultConf(path);
    conf.setString(HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key(), "true");
    conf.setString(HoodieMetadataConfig.COLUMN_STATS_INDEX_FOR_COLUMNS.key(), "uuid,name,age");
    TestData.writeData(TestData.DATA_SET_INSERT, conf);

    HoodieTableMetaClient metaClient = StreamerUtil.initTableIfNotExists(conf);
    HoodieIndexDefinition indexDefinition = metaClient.getIndexMetadata().map(indexMeta -> indexMeta.getIndexDefinitions().get(PARTITION_NAME_COLUMN_STATS)).orElse(null);
    String[] expectedCols = new String[] {"_hoodie_commit_time", "_hoodie_partition_path", "_hoodie_record_key", "uuid", "name", "age"};
    Assertions.assertArrayEquals(expectedCols, indexDefinition.getSourceFields().toArray(new String[0]));
  }
}
