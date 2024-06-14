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
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.MarkerUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestTable.listRecursive;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class TestWriteMarkersBase extends HoodieCommonTestHarness {

  protected WriteMarkers writeMarkers;
  protected HoodieStorage storage;
  protected StoragePath markerFolderPath;
  protected JavaSparkContext jsc;
  protected HoodieSparkEngineContext context;

  private void createSomeMarkers(boolean isTablePartitioned) {
    writeMarkers.create(isTablePartitioned ? "2020/06/01" : "", "file1", IOType.MERGE);
    writeMarkers.create(isTablePartitioned ? "2020/06/02" : "", "file2", IOType.APPEND);
    writeMarkers.create(isTablePartitioned ? "2020/06/03" : "", "file3", IOType.CREATE);
  }

  private void createInvalidFile(String partitionPath, String invalidFileName) {
    StoragePath path = FSUtils.constructAbsolutePath(markerFolderPath, partitionPath);
    StoragePath invalidFilePath = new StoragePath(path, invalidFileName);
    try {
      storage.create(invalidFilePath, false).close();
    } catch (IOException e) {
      throw new HoodieException("Failed to create invalid file " + invalidFilePath, e);
    }
  }

  abstract void verifyMarkersInFileSystem(boolean isTablePartitioned) throws IOException;

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testCreation(boolean isTablePartitioned) throws Exception {
    // when
    createSomeMarkers(isTablePartitioned);

    // then
    assertTrue(storage.exists(markerFolderPath));
    verifyMarkersInFileSystem(isTablePartitioned);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testDeletionWhenMarkerDirExists(boolean isTablePartitioned) throws IOException {
    //when
    writeMarkers.create(isTablePartitioned ? "2020/06/01" : "", "file1", IOType.MERGE);

    // then
    assertTrue(writeMarkers.doesMarkerDirExist());
    assertTrue(writeMarkers.deleteMarkerDir(context, 2));
    assertFalse(writeMarkers.doesMarkerDirExist());
  }

  @Test
  public void testDeletionWhenMarkerDirNotExists() throws IOException {
    // then
    assertFalse(writeMarkers.doesMarkerDirExist());
    assertTrue(writeMarkers.allMarkerFilePaths().isEmpty());
    assertFalse(writeMarkers.deleteMarkerDir(context, 2));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testDataPathsWhenCreatingOrMerging(boolean isTablePartitioned) throws IOException {
    // add marker files
    createSomeMarkers(isTablePartitioned);
    // add invalid file
    createInvalidFile(isTablePartitioned ? "2020/06/01" : "", "invalid_file3");
    long fileSize = listRecursive(storage, markerFolderPath).stream()
        .filter(fileStatus -> !fileStatus.getPath().getName().contains(MarkerUtils.MARKER_TYPE_FILENAME))
        .count();
    assertEquals(fileSize, 4);

    List<String> expectedPaths = isTablePartitioned
        ? CollectionUtils.createImmutableList("2020/06/01/file1", "2020/06/03/file3")
        : CollectionUtils.createImmutableList("file1", "file3");
    // then
    assertIterableEquals(expectedPaths,
        writeMarkers.createdAndMergedDataPaths(context, 2).stream().sorted().collect(Collectors.toList())
    );
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testGetAppendedLogPaths(boolean isTablePartitioned) throws IOException {
    // add marker files
    createSomeMarkers(isTablePartitioned);
    // add invalid file
    createInvalidFile(isTablePartitioned ? "2020/06/01" : "", "invalid_file3");
    long fileSize = listRecursive(storage, markerFolderPath).stream()
        .filter(fileStatus -> !fileStatus.getPath().getName().contains(MarkerUtils.MARKER_TYPE_FILENAME))
        .count();
    assertEquals(fileSize, 4);

    List<String> expectedPaths = isTablePartitioned
        ? CollectionUtils.createImmutableList("2020/06/02/file2")
        : CollectionUtils.createImmutableList("file2");
    // then
    assertIterableEquals(expectedPaths,
        writeMarkers.getAppendedLogPaths(context, 2).stream().sorted().collect(Collectors.toList())
    );
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testAllMarkerPaths(boolean isTablePartitioned) throws IOException {
    // given
    createSomeMarkers(isTablePartitioned);

    List<String> expectedPaths = isTablePartitioned
        ? CollectionUtils.createImmutableList("2020/06/01/file1.marker.MERGE",
        "2020/06/02/file2.marker.APPEND", "2020/06/03/file3.marker.CREATE")
        : CollectionUtils.createImmutableList(
        "file1.marker.MERGE", "file2.marker.APPEND", "file3.marker.CREATE");
    // then
    assertIterableEquals(expectedPaths,
        writeMarkers.allMarkerFilePaths().stream()
            .filter(path -> !path.contains(MarkerUtils.MARKER_TYPE_FILENAME))
            .sorted().collect(Collectors.toList())
    );
  }

  @Test
  public void testStripMarkerSuffix() {
    // Given
    final String pathPrefix = "file://" + metaClient.getMetaPath() + "/file";
    final String markerFilePath = pathPrefix + ".marker.APPEND";

    // when-then
    assertEquals(pathPrefix, WriteMarkers.stripMarkerSuffix(markerFilePath));
  }
}
