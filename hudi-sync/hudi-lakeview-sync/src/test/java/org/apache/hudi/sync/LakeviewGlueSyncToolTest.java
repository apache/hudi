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

package org.apache.hudi.sync;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hudi.aws.testutils.GlueTestUtil;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.glue.GlueAsyncClient;
import software.amazon.awssdk.services.glue.GlueAsyncClientBuilder;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.hudi.aws.testutils.GlueTestUtil.glueSyncProps;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_PATH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LakeviewGlueSyncToolTest {

  private static final String BASE_PATH = "/tmp/test";

  private Configuration hadoopConf;

  @Mock
  private GlueAsyncClient glueAsyncClient;
  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private GlueAsyncClientBuilder glueAsyncClientBuilder;

  @BeforeEach
  public void setUp() {
    FileSystem fileSystem = HadoopFSUtils.getFs(BASE_PATH, new Configuration());
    hadoopConf = fileSystem.getConf();
  }

  @Test
  void testLakeviewGlueSyncTool() throws IOException {
    GlueTestUtil.setUp();
    try (MockedConstruction<LakeviewSyncTool> lakeviewSyncToolMockedConstruction = mockConstruction(LakeviewSyncTool.class,
        (lakeviewSyncTool, context) -> {
          List<?> arguments = context.arguments();
          assertEquals(2, arguments.size());
          assertEquals(glueSyncProps, arguments.get(0));
          assertEquals(hadoopConf, arguments.get(1));
        }); MockedStatic<GlueAsyncClient> glueAsyncClientMockedStatic = mockStatic(GlueAsyncClient.class)
    ) {
      glueAsyncClientMockedStatic.when(GlueAsyncClient::builder).thenReturn(glueAsyncClientBuilder);
      when(glueAsyncClientBuilder.credentialsProvider(any())).thenReturn(glueAsyncClientBuilder);
      when(glueAsyncClientBuilder.build()).thenReturn(glueAsyncClient);

      // change the table base path format
      String tableBasePath = glueSyncProps.getProperty(META_SYNC_BASE_PATH.key())
          .replaceFirst("file://", "file:")
          .replaceAll("/$", "");
      Table glueTable = Table.builder()
          .storageDescriptor(StorageDescriptor.builder().location(tableBasePath).build())
          .build();

      doReturn(CompletableFuture.completedFuture(GetTableResponse.builder().table(glueTable).build()))
          .when(glueAsyncClient)
          .getTable(any(GetTableRequest.class));
      performLakeviewGlueSyncToolTest(lakeviewSyncToolMockedConstruction);
    }
  }

  private void performLakeviewGlueSyncToolTest(MockedConstruction<LakeviewSyncTool> lakeviewSyncToolMockedConstruction) {
    try (LakeviewGlueSyncTool lakeviewGlueSyncTool = new LakeviewGlueSyncTool(glueSyncProps, GlueTestUtil.getHadoopConf())) {
      lakeviewGlueSyncTool.syncHoodieTable();

      List<LakeviewSyncTool> lakeviewSyncToolsConstructed = lakeviewSyncToolMockedConstruction.constructed();
      assertEquals(1, lakeviewSyncToolsConstructed.size());
      LakeviewSyncTool lakeviewSyncTool = lakeviewSyncToolsConstructed.get(0);
      verify(lakeviewSyncTool, times(1)).syncHoodieTable();
    }
  }
}