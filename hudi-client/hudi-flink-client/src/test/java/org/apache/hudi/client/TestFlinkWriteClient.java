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

package org.apache.hudi.client;

import org.apache.hudi.client.heartbeat.HoodieHeartbeatClient;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.TableServiceType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.metadata.FlinkHoodieBackedTableMetadataWriter;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieFlinkClientTestHarness;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestFlinkWriteClient extends HoodieFlinkClientTestHarness {

  @BeforeEach
  void setup() throws IOException {
    initPath();
    initFileSystem();
    initMetaClient();
  }

  @AfterEach
  void teardown() throws IOException {
    cleanupResources();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testWriteClientAndTableServiceClientWithTimelineServer(
      boolean enableEmbeddedTimelineServer) throws IOException {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(metaClient.getBasePath())
        .withEmbeddedTimelineServerEnabled(enableEmbeddedTimelineServer)
        .build();

    HoodieFlinkWriteClient writeClient = new HoodieFlinkWriteClient(context, writeConfig);
    // Only one timeline server should be instantiated, and the same timeline server
    // should be used by both the write client and the table service client.
    assertEquals(
        writeClient.getTimelineServer(),
        writeClient.getTableServiceClient().getTimelineServer());
    if (!enableEmbeddedTimelineServer) {
      assertFalse(writeClient.getTimelineServer().isPresent());
    }

    writeClient.close();
  }

  @Test
  public void testReleaseAndPostCommitResourcesForLazyFailedWrites() throws IOException {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(metaClient.getBasePath())
        .withEngineType(EngineType.FLINK)
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY)
            .build())
        .build();

    AtomicBoolean failTableCreation = new AtomicBoolean(false);
    writeClient = new HoodieFlinkWriteClient(context, writeConfig) {
      @Override
      protected HoodieTable createTable(HoodieWriteConfig config) {
        if (failTableCreation.get()) {
          throw new HoodieException("Expected table creation failure");
        }
        return super.createTable(config);
      }
    };
    String instantTime = "20260709120000000";
    writeClient.restartHeartbeat(instantTime);

    assertTrue(HoodieHeartbeatClient.heartbeatExists(
        metaClient.getStorage(), metaClient.getBasePath().toString(), instantTime));

    writeClient.releaseResources(instantTime);
    assertTrue(HoodieHeartbeatClient.heartbeatExists(
        metaClient.getStorage(), metaClient.getBasePath().toString(), instantTime));

    writeClient.postCommit(instantTime);
    assertFalse(HoodieHeartbeatClient.heartbeatExists(
        metaClient.getStorage(), metaClient.getBasePath().toString(), instantTime));

    String failedPostCommitInstantTime = "20260709120000001";
    writeClient.restartHeartbeat(failedPostCommitInstantTime);
    failTableCreation.set(true);
    assertThrows(HoodieException.class, () -> writeClient.postCommit(failedPostCommitInstantTime));
    assertFalse(HoodieHeartbeatClient.heartbeatExists(
        metaClient.getStorage(), metaClient.getBasePath().toString(), failedPostCommitInstantTime));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testRestartHeartbeatWaitsForTimedOutInitialWrite(boolean metadataHeartbeat) throws IOException {
    String instantTime = "20260709120000000";
    String heartbeatBasePath = metadataHeartbeat ? basePath + "/" + HoodieTableMetaClient.METADATA_TABLE_FOLDER_PATH : basePath;
    CountDownLatch releaseFirstWrite = new CountDownLatch(1);
    AtomicBoolean firstWrite = new AtomicBoolean(true);
    HoodieHadoopStorage slowStorage = new HoodieHadoopStorage(fs) {
      @Override
      public OutputStream create(StoragePath path, boolean overwrite) throws IOException {
        if (firstWrite.getAndSet(false)) {
          try {
            releaseFirstWrite.await();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted first heartbeat write", e);
          }
        }
        return super.create(path, overwrite);
      }
    };
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withEngineType(EngineType.FLINK)
        .withHeartbeatIntervalInMs(1000)
        .withHeartbeatTolerableMisses(10)
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withFailedWritesCleaningPolicy(metadataHeartbeat ? HoodieFailedWritesCleaningPolicy.EAGER : HoodieFailedWritesCleaningPolicy.LAZY)
            .build())
        .build();
    try (HoodieHeartbeatClient delayedHeartbeatClient = new HoodieHeartbeatClient(slowStorage, heartbeatBasePath, 1000L, 10)) {
      HoodieFlinkTable table = mock(HoodieFlinkTable.class);
      if (metadataHeartbeat) {
        HoodieWriteConfig metadataConfig = HoodieWriteConfig.newBuilder()
            .withPath(heartbeatBasePath)
            .withCleanConfig(HoodieCleanConfig.newBuilder()
                .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY).build())
            .build();
        HoodieFlinkWriteClient metadataClient = mock(HoodieFlinkWriteClient.class);
        when(metadataClient.getConfig()).thenReturn(metadataConfig);
        when(metadataClient.getHeartbeatClient()).thenReturn(delayedHeartbeatClient);
        FlinkHoodieBackedTableMetadataWriter metadataWriter = mock(FlinkHoodieBackedTableMetadataWriter.class);
        doReturn(metadataClient).when(metadataWriter).getWriteClient();
        when(table.getMetaClient()).thenReturn(metaClient);
        when(table.getMetadataWriter(instantTime, true, true)).thenReturn(Option.of(metadataWriter));
      }
      writeClient = new HoodieFlinkWriteClient(context, writeConfig, metadataHeartbeat) {
        @Override
        public HoodieHeartbeatClient getHeartbeatClient() {
          return metadataHeartbeat ? super.getHeartbeatClient() : delayedHeartbeatClient;
        }

        @Override
        public HoodieFlinkTable getHoodieTable() {
          return table;
        }
      };

      writeClient.restartHeartbeat(instantTime);
      assertFalse(firstWrite.get(), "The injected slow storage must be used by the restarted heartbeat");
      assertTrue(HoodieHeartbeatClient.heartbeatExists(slowStorage, heartbeatBasePath, instantTime),
          "Recommit must wait for a successful heartbeat after the first write times out");
      assertFalse(delayedHeartbeatClient.isHeartbeatExpired(instantTime));
    } finally {
      releaseFirstWrite.countDown();
    }
  }

  @Test
  public void testCleanResourcesCleansMetadataTableHeartbeatForStreamingMetadataWrites() throws IOException {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(metaClient.getBasePath())
        .withEngineType(EngineType.FLINK)
        .withIndexConfig(HoodieIndexConfig.newBuilder()
            .withIndexType(HoodieIndex.IndexType.GLOBAL_RECORD_LEVEL_INDEX)
            .build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(true)
            .withStreamingWriteEnabled(true)
            .withEnableGlobalRecordLevelIndex(true)
            .build())
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY)
            .build())
        .build();

    writeClient = new HoodieFlinkWriteClient(context, writeConfig, true);
    String instantTime = "20260709120000000";
    writeClient.restartHeartbeat(instantTime);

    String metadataTableBasePath = metaClient.getBasePath()
        + "/" + HoodieTableMetaClient.METADATA_TABLE_FOLDER_PATH;
    assertTrue(HoodieHeartbeatClient.heartbeatExists(
        metaClient.getStorage(), metadataTableBasePath, instantTime));

    writeClient.cleanResources(instantTime);
    assertFalse(HoodieHeartbeatClient.heartbeatExists(
        metaClient.getStorage(), metaClient.getBasePath().toString(), instantTime));
    assertFalse(HoodieHeartbeatClient.heartbeatExists(
        metaClient.getStorage(), metadataTableBasePath, instantTime));
  }

  @Test
  void testUnsupportedWriteEntryPointsAndInvalidTableServiceFailFast() {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(metaClient.getBasePath())
        .withEngineType(EngineType.FLINK)
        .withEmbeddedTimelineServerEnabled(false)
        .build();
    writeClient = new HoodieFlinkWriteClient(context, writeConfig);

    assertThrows(HoodieNotSupportedException.class, () -> writeClient.bootstrap(Option.empty()));
    assertThrows(HoodieNotSupportedException.class,
        () -> writeClient.insertPreppedRecords(Collections.emptyList(), "001"));
    assertThrows(HoodieNotSupportedException.class,
        () -> writeClient.bulkInsert(Collections.emptyList(), "001"));
    assertThrows(HoodieNotSupportedException.class,
        () -> writeClient.bulkInsert(Collections.emptyList(), "001", Option.empty()));
    assertThrows(HoodieNotSupportedException.class,
        () -> writeClient.cluster("001", false));
    assertThrows(HoodieException.class,
        () -> writeClient.delete(Collections.singletonList(new HoodieKey("id", "partition")), "001"));
    assertThrows(HoodieException.class,
        () -> writeClient.deletePrepped(Collections.emptyList(), "001"));
    assertThrows(IllegalArgumentException.class,
        () -> writeClient.completeTableService(TableServiceType.CLEAN, null, null, "001"));
    assertFalse(writeClient.loadActiveTimelineOnTableInit());
    writeClient.waitForCleaningFinish();
    writeClient.cleanHandles();
    assertNotNull(writeClient.getHoodieTable(false));
  }
}
