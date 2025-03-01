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

package org.apache.hudi.timeline.service.handlers;

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.dto.TimelineDTO;
import org.apache.hudi.common.table.timeline.versioning.v2.InstantComparatorV2;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.timeline.service.TimelineService;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestTimelineHandler extends HoodieCommonTestHarness {

  private TimelineHandler timelineHandler;

  private FileSystemViewManager fileSystemViewManager;

  private final FileSystemViewManager mockFileSystemViewManager = Mockito.mock(FileSystemViewManager.class);

  @BeforeEach
  void setUp() throws IOException {
    metaClient = HoodieTestUtils.init(tempDir.toAbsolutePath().toString());
    basePath = metaClient.getBasePath().toString();
    HoodieCommonConfig commonConfig = HoodieCommonConfig.newBuilder().build();
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().enable(true).build();
    FileSystemViewStorageConfig fileSystemViewStorageConfig = FileSystemViewStorageConfig.newBuilder()
        .withStorageType(FileSystemViewStorageType.MEMORY)
        .build();
    fileSystemViewManager = FileSystemViewManager.createViewManager(new HoodieLocalEngineContext(metaClient.getStorageConf()), metadataConfig, fileSystemViewStorageConfig, commonConfig);
    timelineHandler = new TimelineHandler(metaClient.getStorageConf(), new TimelineService.Config(), fileSystemViewManager);
  }

  @AfterEach
  void teardown() {
    fileSystemViewManager.close();
  }

  @Test
  void getTimelineHash() {
    assertEquals(metaClient.getActiveTimeline().getTimelineHash(), timelineHandler.getTimelineHash(basePath));
  }

  @Test
  void initializeTimeline() throws IOException {
    TimelineHandler timelineHandler = new TimelineHandler(metaClient.getStorageConf(), new TimelineService.Config(), mockFileSystemViewManager);
    // Init without any view.
    SyncableFileSystemView firstView = fileSystemViewManager.getFileSystemView(basePath);
    when(mockFileSystemViewManager.doesFileSystemViewExists(basePath)).thenReturn(false);
    when(mockFileSystemViewManager.getFileSystemView(basePath)).thenReturn(firstView);
    when(mockFileSystemViewManager.getFileSystemView(eq(metaClient), argThat(timeline -> timeline.getInstants().isEmpty()))).thenReturn(firstView);
    timelineHandler.initializeTimeline(basePath, TimelineDTO.fromTimeline(metaClient.getActiveTimeline()));
    verify(mockFileSystemViewManager, times(0)).clearFileSystemView(basePath);
    // Init with one instant in client.
    writeInstantToTimeline(basePath);
    when(mockFileSystemViewManager.doesFileSystemViewExists(basePath)).thenReturn(true);
    when(mockFileSystemViewManager.getFileSystemView(eq(metaClient), argThat(timeline -> timeline.getInstants().size() == 1))).thenReturn(firstView);
    timelineHandler.initializeTimeline(basePath, TimelineDTO.fromTimeline(metaClient.reloadActiveTimeline()));
    verify(mockFileSystemViewManager, times(1)).clearFileSystemView(basePath);
    verify(mockFileSystemViewManager, times(1)).getFileSystemView(basePath);
    verify(mockFileSystemViewManager, times(2)).getFileSystemView(any(), argThat(timeline -> timeline.getInstants().size() == 1));
    // Init again for no=op.
    SyncableFileSystemView secondView = fileSystemViewManager.getFileSystemView(basePath);
    Mockito.clearInvocations(mockFileSystemViewManager);
    when(mockFileSystemViewManager.getFileSystemView(basePath)).thenReturn(secondView);
    timelineHandler.initializeTimeline(basePath, TimelineDTO.fromTimeline(metaClient.reloadActiveTimeline()));
    verify(mockFileSystemViewManager, times(1)).clearFileSystemView(basePath);
  }

  @Disabled
  void initializeTimelineConcurrency() throws IOException {
    int concurrentRequests = 64;
    ExecutorService executorService = Executors.newFixedThreadPool(4);
    List<CompletableFuture<Boolean>> futureList = new ArrayList<>();
    for (int i = 0; i < concurrentRequests; i++) {
      futureList.add(CompletableFuture.supplyAsync(() -> timelineHandler.initializeTimeline(basePath, TimelineDTO.fromTimeline(metaClient.getActiveTimeline())), executorService));
    }
    CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0])).join();
    // Init with one instant in client.
    writeInstantToTimeline(basePath);
    futureList.clear();
    for (int i = 0; i < concurrentRequests; i++) {
      futureList.add(CompletableFuture.supplyAsync(() -> timelineHandler.initializeTimeline(basePath, TimelineDTO.fromTimeline(metaClient.reloadActiveTimeline())), executorService));
    }
    CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0])).join();
  }

  @Test
  void testTimelineSerDe() throws IOException {
    writeInstantToTimeline(basePath);
    TimelineDTO dto = TimelineDTO.fromTimeline(metaClient.reloadActiveTimeline());
    // The instants from metaClient and timelineDTO should be same.
    List<HoodieInstant> instantsFromMetaClient = metaClient.getActiveTimeline().getInstants();
    List<HoodieInstant> instantsFromDTO = TimelineDTO.toTimeline(dto, metaClient).getInstants();
    assertEquals(instantsFromMetaClient, instantsFromDTO);
    assertEquals(instantsFromMetaClient.get(0).getCompletionTime(), instantsFromDTO.get(0).getCompletionTime());
  }

  private void writeInstantToTimeline(String basePath) throws IOException {
    // Write data to a single partition.
    String partitionPath = "partition1";
    Paths.get(basePath, partitionPath).toFile().mkdirs();
    String fileId = UUID.randomUUID().toString();
    String newInstantTime = metaClient.createNewInstantTime(true);
    String fileName1 = FSUtils.makeBaseFileName(newInstantTime,"1-0-1", fileId, HoodieTableConfig.BASE_FILE_FORMAT.defaultValue().getFileExtension());
    Paths.get(basePath, partitionPath, fileName1).toFile().createNewFile();
    HoodieActiveTimeline commitTimeline = metaClient.getActiveTimeline();
    HoodieInstant inflight = new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, newInstantTime, InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
    HoodieInstant requested = new HoodieInstant(HoodieInstant.State.REQUESTED, inflight.getAction(), inflight.requestedTime(), InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
    commitTimeline.createNewInstant(requested);
    commitTimeline.transitionRequestedToInflight(requested, Option.empty());
    commitTimeline.saveAsComplete(inflight, Option.empty());
  }
}