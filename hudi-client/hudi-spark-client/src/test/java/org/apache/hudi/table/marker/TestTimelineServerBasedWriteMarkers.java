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
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.MarkerUtils;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.testutils.HoodieClientTestUtils;
import org.apache.hudi.timeline.service.TimelineService;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestTimelineServerBasedWriteMarkers extends TestWriteMarkersBase {
  TimelineService timelineService;

  @BeforeEach
  public void setup() throws IOException {
    initPath();
    initMetaClient();
    this.jsc = new JavaSparkContext(
        HoodieClientTestUtils.getSparkConfForTest(TestTimelineServerBasedWriteMarkers.class.getName()));
    this.context = new HoodieSparkEngineContext(jsc);
    this.storage = metaClient.getStorage();
    this.markerFolderPath = new StoragePath(metaClient.getMarkerFolderPath("000"));

    FileSystemViewStorageConfig storageConf =
        FileSystemViewStorageConfig.newBuilder().withStorageType(FileSystemViewStorageType.SPILLABLE_DISK).build();
    HoodieLocalEngineContext localEngineContext = new HoodieLocalEngineContext(metaClient.getStorageConf());

    try {
      timelineService = new TimelineService(localEngineContext, new Configuration(),
          TimelineService.Config.builder().serverPort(0).enableMarkerRequests(true).build(),
          storage,
          FileSystemViewManager.createViewManager(localEngineContext, storageConf, HoodieCommonConfig.newBuilder().build()));
      timelineService.startService();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    this.writeMarkers = new TimelineServerBasedWriteMarkers(
        metaClient.getBasePath().toString(), markerFolderPath.toString(), "000", "localhost", timelineService.getServerPort(), 300);
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
    assertEquals(3, allMarkers.size());
    List<String> expectedMarkers = isTablePartitioned
        ? CollectionUtils.createImmutableList(
        "2020/06/01/file1.marker.MERGE", "2020/06/02/file2.marker.APPEND",
        "2020/06/03/file3.marker.CREATE")
        : CollectionUtils.createImmutableList(
        "file1.marker.MERGE", "file2.marker.APPEND", "file3.marker.CREATE");
    assertIterableEquals(expectedMarkers, allMarkers);
    // Verifies the marker type file
    StoragePath markerTypeFilePath = new StoragePath(markerFolderPath, MarkerUtils.MARKER_TYPE_FILENAME);
    assertTrue(MarkerUtils.doesMarkerTypeFileExist(storage, markerFolderPath.toString()));
    InputStream inputStream = storage.open(markerTypeFilePath);
    assertEquals(MarkerType.TIMELINE_SERVER_BASED.toString(),
        FileIOUtils.readAsUTFString(inputStream));
    closeQuietly(inputStream);
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
}
