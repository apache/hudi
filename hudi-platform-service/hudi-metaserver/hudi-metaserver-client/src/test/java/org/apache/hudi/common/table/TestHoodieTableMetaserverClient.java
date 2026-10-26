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

package org.apache.hudi.common.table;

import org.apache.hudi.common.config.HoodieMetaserverConfig;
import org.apache.hudi.common.config.HoodieTimeGeneratorConfig;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.fs.FileSystemRetryConfig;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestHoodieTableMetaserverClient {

  @TempDir
  Path tempDir;

  @Test
  void testRequiresTheDatabaseFromTheMetaserverConfig() throws IOException {
    String basePath = tempDir.resolve("tbl").toString();
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.newTableBuilder()
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setTableName("tbl")
        .initTable(HadoopFSUtils.getStorageConf(new Configuration()), basePath);
    // A database persisted by an older writer must not stand in for the metaserver config.
    Properties legacyProps = new Properties();
    legacyProps.setProperty(HoodieTableConfig.DATABASE_NAME.key(), "legacy_db");
    HoodieTableConfig.update(metaClient.getStorage(), metaClient.getMetaPath(), legacyProps);

    HoodieMetaserverConfig config = HoodieMetaserverConfig.newBuilder().setUris("").build();
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> new HoodieTableMetaserverClient(
        metaClient.getStorage(), basePath, ConsistencyGuardConfig.newBuilder().build(), HoodieTimeGeneratorConfig.defaultConfig(basePath),
        FileSystemRetryConfig.newBuilder().build(), Option.empty(), Option.of("tbl"), config));
    assertTrue(e.getMessage().contains(HoodieMetaserverConfig.DATABASE_NAME.key()), e.getMessage());
  }
}
