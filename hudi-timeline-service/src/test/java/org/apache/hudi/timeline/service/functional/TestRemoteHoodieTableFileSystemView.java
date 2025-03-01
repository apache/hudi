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

package org.apache.hudi.timeline.service.functional;

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.dto.DTOUtils;
import org.apache.hudi.common.table.timeline.dto.FileGroupDTO;
import org.apache.hudi.common.table.timeline.versioning.v2.InstantComparatorV2;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.table.view.TestHoodieTableFileSystemView;
import org.apache.hudi.common.testutils.MockHoodieTimeline;
import org.apache.hudi.exception.HoodieRemoteException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.timeline.service.TimelineService;
import org.apache.hudi.timeline.service.TimelineServiceTestHarness;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Bring up a remote Timeline Server and run all test-cases of TestHoodieTableFileSystemView against it.
 */
public class TestRemoteHoodieTableFileSystemView extends TestHoodieTableFileSystemView {

  private static final Logger LOG = LoggerFactory.getLogger(TestRemoteHoodieTableFileSystemView.class);

  private TimelineService server = null;
  private FileSystemViewManager viewManager;
  private RemoteHoodieTableFileSystemView view;

  protected SyncableFileSystemView getFileSystemView(HoodieTimeline timeline) {
    return getFileSystemView(timeline, 0);
  }

  protected SyncableFileSystemView getFileSystemView(HoodieTimeline timeline, int numberOfSimulatedConnectionFailures) {
    FileSystemViewStorageConfig sConf =
        FileSystemViewStorageConfig.newBuilder().withStorageType(FileSystemViewStorageType.SPILLABLE_DISK).build();
    HoodieCommonConfig commonConfig = HoodieCommonConfig.newBuilder().build();
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().build();
    HoodieLocalEngineContext localEngineContext = new HoodieLocalEngineContext(metaClient.getStorageConf());

    try {
      viewManager = FileSystemViewManager.createViewManager(localEngineContext, metadataConfig, sConf, commonConfig);
      if (server != null) {
        server.close();
      }
      TimelineServiceTestHarness.Builder builder = TimelineServiceTestHarness.newBuilder();
      builder.withNumberOfSimulatedConnectionFailures(numberOfSimulatedConnectionFailures);
      server = builder.build(
          HadoopFSUtils.getStorageConf().unwrap(),
          TimelineService.Config.builder().serverPort(0).build(),
          viewManager);
      server.startService();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    LOG.info("Connecting to Timeline Server :{}", server.getServerPort());
    view = initFsView(metaClient, server.getServerPort(), false);
    return view;
  }

  @Test
  public void testRemoteHoodieTableFileSystemViewWithRetry() {
    // Validate remote FS view without any failures in the timeline service.
    view.getLatestBaseFiles();

    // Simulate only a single failure and ensure the request fails.
    validateRequestFailed(() -> getFileSystemView(metaClient.getActiveTimeline(), 1));
    validateRequestFailed(view::getLatestBaseFiles);

    // Simulate 3 failures, but make sure the request succeeds as retries are enabled
    validateRequestFailed(() -> getFileSystemView(metaClient.getActiveTimeline(), 3));
    RemoteHoodieTableFileSystemView viewWithRetries = initFsView(metaClient, server.getServerPort(), true);
    viewWithRetries.getLatestBaseFiles();
  }

  @Test
  public void testJettyServerDaemonThread() {
    // Service is available.
    view.getLatestBaseFiles();
    // org.eclipse.jetty.util.thread.QueuedThreadPool `_name`
    // io.javalin.jetty.JettyUtil.defaultThreadPool `JettyServerThreadPool`
    Thread.getAllStackTraces().keySet().stream().filter(t -> t.getName().startsWith("qtp")
            || t.getName().startsWith("Jetty")
            || t.getName().startsWith("TimelineService-JettyScheduler"))
        .forEach(t -> assertTrue(t.isDaemon()));
    server.close();
  }

  @Test
  public void testListFileGroupDTOPayload() throws IOException, NoSuchFieldException, IllegalAccessException {
    ObjectMapper mapper = new ObjectMapper();
    List<HoodieFileGroup> fileGroups = new ArrayList<>();
    fileGroups.add(createHoodieFileGroup());
    fileGroups.add(createHoodieFileGroup());
    fileGroups.add(createHoodieFileGroup());

    // Timeline exists only in the first file group DTO. Optimisation to reduce payload size.
    Field timelineDTOField = FileGroupDTO.class.getDeclaredField("timeline");
    timelineDTOField.setAccessible(true);
    List<FileGroupDTO> fileGroupDTOs = DTOUtils.fileGroupDTOsfromFileGroups(fileGroups);
    assertNotNull(timelineDTOField.get(fileGroupDTOs.get(0)));
    // Verify other DTO objects do not contain timeline
    assertNull(timelineDTOField.get(fileGroupDTOs.get(1)));
    assertNull(timelineDTOField.get(fileGroupDTOs.get(2)));

    String prettyResult = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(fileGroupDTOs);
    String normalResult = mapper.writeValueAsString(fileGroupDTOs);

    Stream<HoodieFileGroup> prettyFileGroups = readFileGroupStream(prettyResult, mapper);
    Stream<HoodieFileGroup> normalFileGroups = readFileGroupStream(normalResult, mapper);
    // FileGroupDTO.toFileGroup should make sure Timeline is repopulated to all the FileGroups
    prettyFileGroups.forEach(g -> assertNotNull(g.getTimeline()));
    normalFileGroups.forEach(g -> assertNotNull(g.getTimeline()));
  }

  @Test
  void testInitTimelineRemoteBasic() throws IOException {
    // Refresh timeline in remote.
    view = initFsView(metaClient, server.getServerPort(), false);
    // Write data to a single partition.
    String partitionPath = "partition1";
    Paths.get(basePath, partitionPath).toFile().mkdirs();
    String fileId = UUID.randomUUID().toString();
    String instantTime1 = "1";
    String fileName1 = FSUtils.makeBaseFileName(instantTime1,"1-0-1", fileId, HoodieTableConfig.BASE_FILE_FORMAT.defaultValue().getFileExtension());
    Paths.get(basePath, partitionPath, fileName1).toFile().createNewFile();
    HoodieActiveTimeline commitTimeline = metaClient.getActiveTimeline();
    HoodieInstant instant1 = new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, instantTime1, InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
    saveAsComplete(commitTimeline, instant1, new HoodieCommitMetadata());
    // Refresh timeline in remote after reloading timeline.
    metaClient.reloadActiveTimeline();
    view = initFsView(metaClient, server.getServerPort(), false);
    List<HoodieBaseFile> baseFiles = view.getAllBaseFiles("partition1").collect(Collectors.toList());
    assertEquals(1, baseFiles.size());
    List<HoodieInstant> instantsFromMetaClient = metaClient.getActiveTimeline().getInstants();
    List<HoodieInstant> instantsFromDTO = view.getTimeline().getInstants();
    assertEquals(instantsFromMetaClient, instantsFromDTO);
    assertEquals(instantsFromMetaClient.get(0).getCompletionTime(), instantsFromDTO.get(0).getCompletionTime());
  }

  @Test
  void testCloseView() {
    // make sure view is loaded
    view.getLatestBaseFiles();
    assertTrue(viewManager.doesFileSystemViewExists(basePath));
    view.close();
    assertFalse(viewManager.doesFileSystemViewExists(basePath));
  }

  private Stream<HoodieFileGroup> readFileGroupStream(String result, ObjectMapper mapper) throws IOException {
    return DTOUtils.fileGroupDTOsToFileGroups(
        (List<FileGroupDTO>) mapper.readValue(
            result, new TypeReference<List<FileGroupDTO>>() {
            }), metaClient);
  }

  private HoodieFileGroup createHoodieFileGroup() {
    Stream<String> completed = Stream.of("001");
    Stream<String> inflight = Stream.of("002");
    MockHoodieTimeline activeTimeline = new MockHoodieTimeline(completed, inflight);
    return new HoodieFileGroup("", "data",
        activeTimeline.getCommitsTimeline().filterCompletedInstants());
  }

  private static RemoteHoodieTableFileSystemView initFsView(HoodieTableMetaClient metaClient,
                                                            int serverPort,
                                                            boolean enableRetries) {
    FileSystemViewStorageConfig.Builder builder = FileSystemViewStorageConfig.newBuilder().withRemoteServerHost("localhost")
        .withRemoteServerPort(serverPort)
        .withRemoteInitTimeline(true)
        .withRemoteTimelineClientTimeoutSecs(5);
    if (enableRetries) {
      builder.withRemoteTimelineClientTimeoutSecs(300)
          .withRemoteTimelineClientRetry(true)
          .withRemoteTimelineClientMaxRetryIntervalMs(2000L)
          .withRemoteTimelineClientMaxRetryNumbers(5);
    }
    return new RemoteHoodieTableFileSystemView(metaClient, builder.build());
  }

  private static void validateRequestFailed(Executable executable) {
    assertThrows(
        HoodieRemoteException.class,
        executable,
        "Should catch a NoHTTPResponseException'"
    );
  }
}
