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

package org.apache.hudi.aws.sync;

import org.apache.hudi.aws.testutils.GlueTestUtil;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;

import static org.apache.hudi.aws.testutils.GlueTestUtil.glueSyncProps;
import static org.apache.hudi.config.GlueCatalogSyncClientConfig.RECREATE_GLUE_TABLE_ON_ERROR;
import static org.apache.hudi.hive.HiveSyncConfig.RECREATE_HIVE_TABLE_ON_ERROR;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
class TestAwsGlueSyncTool {

  private AwsGlueCatalogSyncTool awsGlueCatalogSyncTool;

  @BeforeEach
  void setUp() throws IOException {
    GlueTestUtil.setUp();
    awsGlueCatalogSyncTool = new MockAwsGlueCatalogSyncTool(glueSyncProps, GlueTestUtil.getHadoopConf());
  }

  @AfterEach
  void clean() throws IOException {
    GlueTestUtil.clear();
  }

  @AfterAll
  public static void cleanUp() throws IOException {
    GlueTestUtil.teardown();
  }

  @Test
  void testShouldRecreateAndSyncTableOverride() {
    glueSyncProps.setProperty(RECREATE_HIVE_TABLE_ON_ERROR.key(), "false");
    glueSyncProps.setProperty(RECREATE_GLUE_TABLE_ON_ERROR.key(), "true");
    reinitGlueSyncTool();
    assertTrue(awsGlueCatalogSyncTool.shouldRecreateAndSyncTable(), "recreate_table_on_error should be true for glue");
  }

  private void reinitGlueSyncTool() {
    awsGlueCatalogSyncTool = new MockAwsGlueCatalogSyncTool(glueSyncProps, GlueTestUtil.getHadoopConf());
  }
}
