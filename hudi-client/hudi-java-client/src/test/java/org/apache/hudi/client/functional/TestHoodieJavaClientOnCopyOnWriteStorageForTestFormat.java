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

package org.apache.hudi.client.functional;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.tableformat.TestTableFormat;

import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.AfterAll;

import java.io.IOException;

public class TestHoodieJavaClientOnCopyOnWriteStorageForTestFormat extends TestHoodieJavaClientOnCopyOnWriteStorage {
  @Override
  protected void initMetaClient() throws IOException {
    if (basePath == null) {
      initPath();
    }
    TypedProperties properties = new TypedProperties();
    properties.put(HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key(), "false");
    properties.put(HoodieTableConfig.TABLE_FORMAT.key(), "test-format");
    properties.put(HoodieMetadataConfig.ENABLE.key(), "false");
    metaClient = HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE, properties);
  }

  @AfterAll
  public static void tearDownAll() throws IOException {
    TestTableFormat.tearDown();
    FileSystem.closeAll();
  }
}
