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

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.testutils.FileSystemTestUtils;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.testutils.HoodieClientTestUtils;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterEach;
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
  private JavaSparkContext jsc;
  private HoodieSparkEngineContext context;

  @BeforeEach
  public void setup() throws IOException {
    initPath();
    initMetaClient();
    this.jsc = new JavaSparkContext(HoodieClientTestUtils.getSparkConfForTest(TestMarkerFiles.class.getName()));
    this.context = new HoodieSparkEngineContext(jsc);
    this.fs = FSUtils.getFs(metaClient.getBasePath(), metaClient.getHadoopConf());
    this.markerFolderPath =  new Path(metaClient.getMarkerFolderPath("000"));
    this.markerFiles = new MarkerFiles(fs, metaClient.getBasePath(), markerFolderPath.toString(), "000");
  }

  @AfterEach
  public void cleanup() {
    jsc.stop();
    context = null;
  }

  private void createSomeMarkerFiles() {
    markerFiles.create("2020/06/01", "file1", IOType.MERGE);
    markerFiles.create("2020/06/02", "file2", IOType.APPEND);
    markerFiles.create("2020/06/03", "file3", IOType.CREATE);
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
    List<FileStatus> markerFiles = FileSystemTestUtils.listRecursive(fs, markerFolderPath)
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
    markerFiles.create("2020/06/01", "file1", IOType.MERGE);

    // then
    assertTrue(markerFiles.doesMarkerDirExist());
    assertTrue(markerFiles.deleteMarkerDir(context, 2));
    assertFalse(markerFiles.doesMarkerDirExist());
  }

  @Test
  public void testDeletionWhenMarkerDirNotExists() throws IOException {
    // then
    assertFalse(markerFiles.doesMarkerDirExist());
    assertTrue(markerFiles.allMarkerFilePaths().isEmpty());
    assertFalse(markerFiles.deleteMarkerDir(context, 2));
  }

  @Test
  public void testDataPathsWhenCreatingOrMerging() throws IOException {
    // add markfiles
    createSomeMarkerFiles();
    // add invalid file
    createInvalidFile("2020/06/01", "invalid_file3");
    int fileSize = FileSystemTestUtils.listRecursive(fs, markerFolderPath).size();
    assertEquals(fileSize,4);

    // then
    assertIterableEquals(CollectionUtils.createImmutableList(
        "2020/06/01/file1", "2020/06/03/file3"),
        markerFiles.createdAndMergedDataPaths(context, 2).stream().sorted().collect(Collectors.toList())
    );
  }

  @Test
  public void testAllMarkerPaths() throws IOException {
    // given
    createSomeMarkerFiles();

    // then
    assertIterableEquals(CollectionUtils.createImmutableList("2020/06/01/file1.marker.MERGE",
        "2020/06/02/file2.marker.APPEND", "2020/06/03/file3.marker.CREATE"),
        markerFiles.allMarkerFilePaths().stream().sorted().collect(Collectors.toList())
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
