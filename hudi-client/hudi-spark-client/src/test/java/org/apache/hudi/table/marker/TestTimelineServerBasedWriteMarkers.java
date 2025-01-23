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
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.MarkerUtils;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieRemoteException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieClientTestUtils;
import org.apache.hudi.timeline.service.TimelineService;
import org.apache.hudi.timeline.service.TimelineServiceTestHarness;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestTimelineServerBasedWriteMarkers extends TestWriteMarkersBase {

  private static final Logger LOG = LoggerFactory.getLogger(TestTimelineServerBasedWriteMarkers.class);
  private static int DEFAULT_READ_TIMEOUT_SECS = 60;

  TimelineService timelineService = null;

  @BeforeEach
  public void setup() throws IOException {
    initPath();
    initMetaClient();
    this.jsc = new JavaSparkContext(
        HoodieClientTestUtils.getSparkConfForTest(TestTimelineServerBasedWriteMarkers.class.getName()));
    this.context = new HoodieSparkEngineContext(jsc);
    this.fs = FSUtils.getFs(metaClient.getBasePath(), metaClient.getHadoopConf());
    this.markerFolderPath =  new Path(metaClient.getMarkerFolderPath("000"));

    restartServerAndClient(0);
    LOG.info("Connecting to Timeline Server :" + timelineService.getServerPort());
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
            markerFolderPath.toString(), fs, context, 1)
        .values().stream().flatMap(Collection::stream).sorted()
        .collect(Collectors.toList());
    assertEquals(3, allMarkers.size());
    List<String> expectedMarkers = isTablePartitioned
        ? CollectionUtils.createImmutableList(
        "2020/06/01/file1.marker.MERGE", "2020/06/02/file2.marker.APPEND",
        "2020/06/03/file3.marker.CREATE")
        : CollectionUtils.createImmutableList(
        "file1.marker.MERGE", "file2.marker.APPEND", "file3.marker.CREATE");
    assertIterableEquals(expectedMarkers, allMarkers);
    // Verifies the marker type file
    Path markerTypeFilePath = new Path(markerFolderPath, MarkerUtils.MARKER_TYPE_FILENAME);
    assertTrue(MarkerUtils.doesMarkerTypeFileExist(fs, markerFolderPath.toString()));
    FSDataInputStream fsDataInputStream = fs.open(markerTypeFilePath);
    assertEquals(MarkerType.TIMELINE_SERVER_BASED.toString(),
        FileIOUtils.readAsUTFString(fsDataInputStream));
    closeQuietly(fsDataInputStream);
  }

  @Test
  public void testCreationWithTimelineServiceRetries() throws Exception {
    // Validate marker creation/ deletion work without any failures in the timeline service.
    createSomeMarkers(true);
    assertTrue(fs.exists(markerFolderPath));
    writeMarkers.doesMarkerDirExist();

    // Simulate only a single failure and ensure the request fails.
    restartServerAndClient(1);
    // validate that subsequent request fails
    validateRequestFailed(writeMarkers::doesMarkerDirExist);

    // Simulate 3 failures, but make sure the request succeeds as retries are enabled
    restartServerAndClient(3);
    // Configure a new client with retries enabled.
    TimelineServerBasedWriteMarkers writeMarkersWithRetries = initWriteMarkers(metaClient.getBasePath(), markerFolderPath.toString(), timelineService.getServerPort(), true);
    writeMarkersWithRetries.doesMarkerDirExist();
  }

  private void restartServerAndClient(int numberOfSimulatedConnectionFailures) {
    if (timelineService != null) {
      timelineService.close();
    }
    try {
      HoodieEngineContext hoodieEngineContext = new HoodieLocalEngineContext(metaClient.getHadoopConf());
      FileSystemViewStorageConfig storageConf =
          FileSystemViewStorageConfig.newBuilder().withStorageType(FileSystemViewStorageType.SPILLABLE_DISK).build();
      HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().build();
      TimelineServiceTestHarness.Builder builder = TimelineServiceTestHarness.newBuilder();
      builder.withNumberOfSimulatedConnectionFailures(numberOfSimulatedConnectionFailures);
      timelineService = builder.build(hoodieEngineContext, new Configuration(),
          TimelineService.Config.builder().serverPort(0).enableMarkerRequests(true).build(),
          FileSystemViewManager.createViewManager(
              hoodieEngineContext, metadataConfig, storageConf, HoodieCommonConfig.newBuilder().build()));
      timelineService.startService();
      this.writeMarkers = initWriteMarkers(metaClient.getBasePath(), markerFolderPath.toString(), timelineService.getServerPort(), false);
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

    assertTrue(fs.exists(markerFolderPath));
    assertTrue(writeMarkers.doesMarkerDirExist());

    // lets fail the marker creation
    timelineServerBasedWriteMarkers.failMarkerCreation = true;
    try {
      timelineServerBasedWriteMarkers.create("2020/06/01", "file2", IOType.MERGE);
      fail("Shold not have reached here");
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
    return new TimelineServerBasedWriteMarkers(basePath, markerFolderPath, "000", builder.build());
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
