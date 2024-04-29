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
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.dto.DTOUtils;
import org.apache.hudi.common.table.timeline.dto.FileGroupDTO;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.table.view.TestHoodieTableFileSystemView;
import org.apache.hudi.common.testutils.MockHoodieTimeline;
import org.apache.hudi.exception.HoodieRemoteException;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.timeline.service.TimelineService;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Bring up a remote Timeline Server and run all test-cases of TestHoodieTableFileSystemView against it.
 */
public class TestRemoteHoodieTableFileSystemView extends TestHoodieTableFileSystemView {

  private static final Logger LOG = LoggerFactory.getLogger(TestRemoteHoodieTableFileSystemView.class);

  private TimelineService server;
  private RemoteHoodieTableFileSystemView view;

  protected SyncableFileSystemView getFileSystemView(HoodieTimeline timeline) {
    FileSystemViewStorageConfig sConf =
        FileSystemViewStorageConfig.newBuilder().withStorageType(FileSystemViewStorageType.SPILLABLE_DISK).build();
    HoodieCommonConfig commonConfig = HoodieCommonConfig.newBuilder().build();
    HoodieLocalEngineContext localEngineContext = new HoodieLocalEngineContext(metaClient.getStorageConf());

    try {
      server = new TimelineService(localEngineContext, new Configuration(),
          TimelineService.Config.builder().serverPort(0).build(),
          HoodieStorageUtils.getStorage(getDefaultStorageConf()),
          FileSystemViewManager.createViewManager(localEngineContext, sConf, commonConfig));
      server.startService();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    LOG.info("Connecting to Timeline Server :" + server.getServerPort());
    view = new RemoteHoodieTableFileSystemView("localhost", server.getServerPort(), metaClient);
    return view;
  }

  @Test
  public void testRemoteHoodieTableFileSystemViewWithRetry() {
    // Service is available.
    view.getLatestBaseFiles();
    // Shut down the service.
    server.close();
    try {
      // Immediately fails and throws a connection refused exception.
      view.getLatestBaseFiles();
      fail("Should be catch Exception 'Connection refused (Connection refused)'");
    } catch (HoodieRemoteException e) {
      assert e.getMessage().contains("Connection refused (Connection refused)");
    }
    // Enable API request retry for remote file system view.
    view =  new RemoteHoodieTableFileSystemView(metaClient, FileSystemViewStorageConfig
            .newBuilder()
            .withRemoteServerHost("localhost")
            .withRemoteServerPort(server.getServerPort())
            .withRemoteTimelineClientRetry(true)
            .withRemoteTimelineClientMaxRetryIntervalMs(2000L)
            .withRemoteTimelineClientMaxRetryNumbers(4)
            .build());
    try {
      view.getLatestBaseFiles();
      fail("Should be catch Exception 'Connection refused (Connection refused)'");
    } catch (HoodieRemoteException e) {
      assert e.getMessage().contains("Connection refused (Connection refused)");
    }
    // Retry succeed after 2 or 3 tries.
    new Thread(() -> {
      try {
        Thread.sleep(5000L);
        LOG.info("Restart server.");
        server.startService();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }).run();
    view.getLatestBaseFiles();
    server.close();
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
}
