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

package org.apache.hudi.table.marker;

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.util.MarkerUtils;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieRemoteException;
import org.apache.hudi.io.util.FileIOUtils;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieClientTestUtils;
import org.apache.hudi.timeline.service.TimelineService;
import org.apache.hudi.timeline.service.TimelineServiceTestHarness;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.view.FileSystemViewStorageType.SPILLABLE_DISK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Slf4j
public class TestTimelineServerBasedWriteMarkers extends TestWriteMarkersBase {
  protected static final int DEFAULT_READ_TIMEOUT_SECS = 60;

  TimelineService timelineService = null;

  @BeforeEach
  public void setup() throws IOException {
    initPath();
    initMetaClient();
    this.jsc = new JavaSparkContext(
        HoodieClientTestUtils.getSparkConfForTest(TestTimelineServerBasedWriteMarkers.class.getName()));
    this.context = new HoodieSparkEngineContext(jsc);
    this.storage = metaClient.getStorage();
    this.markerFolderPath = new StoragePath(metaClient.getMarkerFolderPath("000"));

    restartServerAndClient(0);
    log.info("Connecting to Timeline Server :{}", timelineService.getServerPort());
  }

  @AfterEach
  public void cleanup() {
    if (timelineService != null) {
      timelineService.close();
    }
    jsc.stop();
    context = null;
  }

  @Override
  void verifyMarkersInFileSystem(boolean isTablePartitioned) throws IOException {
    // Verifies the markers
    List<String> allMarkers = MarkerUtils.readTimelineServerBasedMarkersFromFileSystem(
            markerFolderPath.toString(), storage, context, 1)
        .values().stream().flatMap(Collection::stream).sorted()
        .collect(Collectors.toList());
    List<String> expectedMarkers = getRelativeMarkerPathList(isTablePartitioned);
    assertIterableEquals(expectedMarkers, allMarkers);
    // Verifies the marker type file
    StoragePath markerTypeFilePath = new StoragePath(markerFolderPath, MarkerUtils.MARKER_TYPE_FILENAME);
    assertTrue(MarkerUtils.doesMarkerTypeFileExist(storage, markerFolderPath));
    InputStream inputStream = storage.open(markerTypeFilePath);
    assertEquals(MarkerType.TIMELINE_SERVER_BASED.toString(),
        FileIOUtils.readAsUTFString(inputStream));
    closeQuietly(inputStream);
  }

  @ParameterizedTest
  @EnumSource(value = FileSystemViewStorageType.class)
  public void testCreationWithTimelineServiceRetries(FileSystemViewStorageType storageType) throws Exception {
    restartServerAndClient(0, storageType);
    log.info("Connecting to Timeline Server :{}", timelineService.getServerPort());
    // Validate marker creation/ deletion work without any failures in the timeline service.
    createSomeMarkers(true);
    assertTrue(storage.exists(markerFolderPath));
    assertTrue(writeMarkers.doesMarkerDirExist());

    // Simulate only a single failure and ensure the request fails.
    restartServerAndClient(1);
    // validate that subsequent request fails
    validateRequestFailed(writeMarkers::doesMarkerDirExist);

    // Simulate 3 failures, but make sure the request succeeds as retries are enabled
    restartServerAndClient(3);
    // Configure a new client with retries enabled.
    TimelineServerBasedWriteMarkers writeMarkersWithRetries = initWriteMarkers(
        metaClient.getBasePath().toString(),
        markerFolderPath.toString(),
        timelineService.getServerPort(),
        true);
    assertTrue(writeMarkersWithRetries.doesMarkerDirExist());
  }

  private void restartServerAndClient(int numberOfSimulatedConnectionFailures) {
    restartServerAndClient(numberOfSimulatedConnectionFailures, SPILLABLE_DISK);
  }

  private void restartServerAndClient(int numberOfSimulatedConnectionFailures,
                                      FileSystemViewStorageType storageType) {
    if (timelineService != null) {
      timelineService.close();
    }
    try {
      HoodieEngineContext hoodieEngineContext = new HoodieLocalEngineContext(metaClient.getStorageConf());
      FileSystemViewStorageConfig storageConf =
          FileSystemViewStorageConfig.newBuilder().withStorageType(storageType).build();
      HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().build();
      TimelineServiceTestHarness.Builder builder = TimelineServiceTestHarness.newBuilder();
      builder.withNumberOfSimulatedConnectionFailures(numberOfSimulatedConnectionFailures);
      timelineService = builder.build(
          (Configuration) storage.getConf().unwrap(),
          TimelineService.Config.builder().serverPort(0).enableMarkerRequests(true).build(),
          FileSystemViewManager.createViewManager(
              hoodieEngineContext, metadataConfig, storageConf, HoodieCommonConfig.newBuilder().build()));
      timelineService.startService();
      this.writeMarkers = initWriteMarkers(
          metaClient.getBasePath().toString(),
          markerFolderPath.toString(),
          timelineService.getServerPort(),
          false);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Test
  public void testMarkerCreationFailure() throws IOException {
    FileSystemViewStorageConfig.Builder builder = FileSystemViewStorageConfig.newBuilder().withRemoteServerHost("localhost")
        .withRemoteServerPort(timelineService.getServerPort())
        .withRemoteTimelineClientTimeoutSecs(DEFAULT_READ_TIMEOUT_SECS);
    MockTimelineServerBasedWriteMarkers timelineServerBasedWriteMarkers = new MockTimelineServerBasedWriteMarkers(basePath, markerFolderPath.toString(), "000", builder.build());
    // this should succeed.
    timelineServerBasedWriteMarkers.create("2020/06/01", "file1", IOType.MERGE);

    assertTrue(storage.exists(markerFolderPath));
    assertTrue(writeMarkers.doesMarkerDirExist());

    // lets fail the marker creation
    timelineServerBasedWriteMarkers.failMarkerCreation = true;
    try {
      timelineServerBasedWriteMarkers.create("2020/06/01", "file2", IOType.MERGE);
      fail("Should not have reached here");
    } catch (HoodieIOException ioe) {
      assertTrue(ioe.getMessage().contains("[timeline-server-based] Failed to create marker for partition"));
    } finally {
      if (timelineService != null) {
        timelineService.close();
      }
    }
  }

  static class MockTimelineServerBasedWriteMarkers extends TimelineServerBasedWriteMarkers {

    boolean failMarkerCreation = false;

    public MockTimelineServerBasedWriteMarkers(HoodieTable table, String instantTime) {
      super(table, instantTime);
    }

    MockTimelineServerBasedWriteMarkers(String basePath, String markerFolderPath, String instantTime, FileSystemViewStorageConfig fileSystemViewStorageConfig) {
      super(basePath, markerFolderPath, instantTime, fileSystemViewStorageConfig);
    }

    @Override
    boolean executeCreateMarkerRequest(Map<String, String> paramsMap, String partitionPath, String markerFileName) {
      if (!failMarkerCreation) {
        return super.executeCreateMarkerRequest(paramsMap, partitionPath, markerFileName);
      } else {
        return false;
      }
    }
  }

  private static TimelineServerBasedWriteMarkers initWriteMarkers(String basePath,
                                                                  String markerFolderPath,
                                                                  int serverPort,
                                                                  boolean enableRetries) {
    FileSystemViewStorageConfig.Builder builder = FileSystemViewStorageConfig.newBuilder().withRemoteServerHost("localhost")
        .withRemoteServerPort(serverPort)
        .withRemoteTimelineClientTimeoutSecs(DEFAULT_READ_TIMEOUT_SECS);
    if (enableRetries) {
      builder.withRemoteTimelineClientRetry(true)
          .withRemoteTimelineClientMaxRetryIntervalMs(30000L)
          .withRemoteTimelineClientMaxRetryNumbers(5);
    }
    return new TimelineServerBasedWriteMarkers(
        basePath, markerFolderPath, "000", builder.build());
  }

  /**
   * Closes {@code Closeable} quietly.
   *
   * @param closeable {@code Closeable} to close
   */
  private void closeQuietly(Closeable closeable) {
    if (closeable == null) {
      return;
    }
    try {
      closeable.close();
    } catch (IOException e) {
      // Ignore
    }
  }

  private static void validateRequestFailed(Executable executable) {
    assertThrows(
        HoodieRemoteException.class,
        executable,
        "Should catch a NoHTTPResponseException"
    );
  }
}
