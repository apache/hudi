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
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.sync.common.HoodieSyncTool;
import org.apache.hudi.sync.common.util.SyncUtilHelpers;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.glue.GlueAsyncClient;
import software.amazon.awssdk.services.glue.GlueAsyncClientBuilder;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityRequest;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityResponse;

import java.io.IOException;

import static org.apache.hudi.aws.testutils.GlueTestUtil.getHadoopConf;
import static org.apache.hudi.aws.testutils.GlueTestUtil.glueSyncProps;
import static org.apache.hudi.config.GlueCatalogSyncClientConfig.RECREATE_GLUE_TABLE_ON_ERROR;
import static org.apache.hudi.hive.HiveSyncConfig.RECREATE_HIVE_TABLE_ON_ERROR;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TestAwsGlueSyncTool {
  private AwsGlueCatalogSyncTool awsGlueCatalogSyncTool;

  @BeforeEach
  void setUp() throws IOException {
    GlueTestUtil.setUp();
    awsGlueCatalogSyncTool = new MockAwsGlueCatalogSyncTool(glueSyncProps, getHadoopConf());
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
    awsGlueCatalogSyncTool = new MockAwsGlueCatalogSyncTool(glueSyncProps, getHadoopConf());
  }

  @Test
  void validateInitThroughSyncTool() throws Exception {
    try (MockedStatic<GlueAsyncClient> mockedStatic = mockStatic(GlueAsyncClient.class);
         MockedStatic<StsClient> mockedStsStatic = mockStatic(StsClient.class)) {
      GlueAsyncClientBuilder builder = mock(GlueAsyncClientBuilder.class);
      mockedStatic.when(GlueAsyncClient::builder).thenReturn(builder);
      when(builder.credentialsProvider(any())).thenReturn(builder);
      GlueAsyncClient mockClient = mock(GlueAsyncClient.class);
      when(builder.build()).thenReturn(mockClient);
      StsClientBuilder stsBuilder = mock(StsClientBuilder.class);
      StsClient mockSts = mock(StsClient.class);
      mockedStsStatic.when(StsClient::builder).thenReturn(stsBuilder);
      when(stsBuilder.credentialsProvider(any())).thenReturn(stsBuilder);
      when(stsBuilder.build()).thenReturn(mockSts);
      when(mockSts.getCallerIdentity(GetCallerIdentityRequest.builder().build())).thenReturn(GetCallerIdentityResponse.builder().account("").build());
      HoodieSyncTool syncTool = SyncUtilHelpers.instantiateMetaSyncTool(
          AwsGlueCatalogSyncTool.class.getName(),
          new TypedProperties(),
          getHadoopConf(),
          GlueTestUtil.fileSystem,
          GlueTestUtil.getMetaClient().getBasePath().toString(),
          "PARQUET",
          Option.empty());
      assertTrue(syncTool instanceof AwsGlueCatalogSyncTool);
      syncTool.close();
    }
  }
}
