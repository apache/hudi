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

package org.apache.hudi.table;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.testutils.FileSystemTestUtils;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;

import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.exception.HoodieException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestMarkerFiles extends HoodieCommonTestHarness {

  private MarkerFiles markerFiles;
  private FileSystem fs;
  private Path markerFolderPath;

  @BeforeEach
  public void setup() throws IOException {
    initPath();
    initMetaClient();
    this.fs = FSUtils.getFs(metaClient.getBasePath(), metaClient.getHadoopConf());
    this.markerFolderPath =  new Path(metaClient.getMarkerFolderPath("000"));
    this.markerFiles = new MarkerFiles(fs, metaClient.getBasePath(), markerFolderPath.toString(), "000");
  }

  private void createSomeMarkerFiles() {
    markerFiles.createMarkerFile("2020/06/01", "file1", MarkerFiles.MarkerType.MERGE);
    markerFiles.createMarkerFile("2020/06/02", "file2", MarkerFiles.MarkerType.APPEND);
    markerFiles.createMarkerFile("2020/06/03", "file3", MarkerFiles.MarkerType.CREATE);
  }

  private void createInvalidFile(String partitionPath, String invalidFileName) {
    Path path = FSUtils.getPartitionPath(markerFolderPath.toString(), partitionPath);
    Path invalidFilePath = new Path(path, invalidFileName);
    try {
      fs.create(invalidFilePath, false).close();
    } catch (IOException e) {
      throw new HoodieException("Failed to create invalid file " + invalidFilePath, e);
    }
  }

  @Test
  public void testCreation() throws Exception {
    // when
    createSomeMarkerFiles();

    // then
    assertTrue(fs.exists(markerFolderPath));
    List<FileStatus> markerFiles = FileSystemTestUtils.listPathRecursively(fs, markerFolderPath)
        .stream().filter(status -> status.getPath().getName().contains(".marker"))
        .sorted().collect(Collectors.toList());
    assertEquals(3, markerFiles.size());
    assertIterableEquals(CollectionUtils.createImmutableList(
        "file:" + markerFolderPath.toString() + "/2020/06/01/file1.marker.MERGE",
        "file:" + markerFolderPath.toString() + "/2020/06/02/file2.marker.APPEND",
        "file:" + markerFolderPath.toString() + "/2020/06/03/file3.marker.CREATE"),
        markerFiles.stream().map(m -> m.getPath().toString()).collect(Collectors.toList())
    );
  }

  @Test
  public void testDeletionWhenMarkerDirExists() throws IOException {
    //when
    markerFiles.createMarkerFile("2020/06/01", "file1", MarkerFiles.MarkerType.MERGE);

    // then
    assertTrue(markerFiles.doesMarkerDirExist());
    assertTrue(markerFiles.deleteMarkerDir());
    assertFalse(markerFiles.doesMarkerDirExist());
  }

  @Test
  public void testDeletionWhenMarkerDirNotExists() throws IOException {
    // then
    assertFalse(markerFiles.doesMarkerDirExist());
    assertFalse(markerFiles.deleteMarkerDir());
  }

  @Test
  public void testDataPathsWhenCreatingOrMerging() throws IOException {
    // add markfiles
    createSomeMarkerFiles();
    // add invalid file
    createInvalidFile("2020/06/01", "invalid_file3");
    int fileSize = FileSystemTestUtils.listPathRecursively(fs, markerFolderPath).size();
    assertEquals(fileSize,4);

    // then
    assertIterableEquals(CollectionUtils.createImmutableList(
        metaClient.getBasePath() + "/2020/06/01/file1",
        metaClient.getBasePath() + "/2020/06/03/file3"),
        markerFiles.getCreatedOrMergedDataPaths().stream().sorted().collect(Collectors.toList())
    );
  }

  @Test
  public void testRelativeMarkerPaths() throws IOException {
    // given
    createSomeMarkerFiles();

    // then
    assertIterableEquals(CollectionUtils.createImmutableList("2020/06/01/file1.marker.MERGE",
        "2020/06/02/file2.marker.APPEND", "2020/06/03/file3.marker.CREATE"),
        markerFiles.relativeMarkerFilePaths().stream().sorted().collect(Collectors.toList())
    );
  }

  @Test
  public void testStripMarkerSuffix() {
    // Given
    final String pathPrefix = "file://" + metaClient.getMetaPath() + "/file";
    final String markerFilePath = pathPrefix + ".marker.APPEND";

    // when-then
    assertEquals(pathPrefix, MarkerFiles.stripMarkerSuffix(markerFilePath));
  }
}
