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

package org.apache.hudi.sync.common;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import static org.apache.hudi.common.config.HoodieCommonConfig.META_SYNC_BASE_PATH_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;

class TestHoodieSyncTool extends HoodieCommonTestHarness {
  @Test
  void testBuildMetaClient() throws Exception {
    initMetaClient();
    TypedProperties properties = new TypedProperties();
    properties.put(META_SYNC_BASE_PATH_KEY, metaClient.getBasePath().toString());
    HoodieSyncConfig syncConfig = new HoodieSyncConfig(properties, new Configuration());
    HoodieTableMetaClient actual = HoodieSyncTool.buildMetaClient(syncConfig);
    assertEquals(actual.getBasePath(), metaClient.getBasePath());
  }
}
