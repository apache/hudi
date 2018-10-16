/*
 *  Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.uber.hoodie.common.table.view;

import static org.junit.Assert.assertEquals;

import com.uber.hoodie.common.model.HoodieDataFile;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.util.FSUtils;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Test;


public class LatestFileSliceOnlyFSViewImplTest extends HoodieTableFileSystemViewTest {

  protected HoodieTableFileSystemView getFileSystemView(HoodieTimeline timeline) {
    return new LatestFileSliceOnlyFSViewImpl(metaClient, timeline, true);
  }

  protected HoodieTableFileSystemView getFileSystemView(HoodieTimeline timeline, FileStatus[] statuses) {
    return new LatestFileSliceOnlyFSViewImpl(metaClient, timeline, statuses, true);
  }

  /**
   * Test case for view generation on a file group where the only file-slice does not have data-file. This is the case
   * where upserts directly go to log-files
   */
  @Test
  public void testViewForFileSlicesWithNoBaseFile() throws Exception {
    testViewForFileSlicesWithNoBaseFile(1, 0);
  }

  @Test
  public void testViewForFileSlicesWithNoBaseFileAndRequestedCompaction() throws Exception {
    testViewForFileSlicesWithAsyncCompaction(true, false, 1, 1, false);
  }

  @Test
  public void testViewForFileSlicesWithBaseFileAndRequestedCompaction() throws Exception {
    testViewForFileSlicesWithAsyncCompaction(false, false, 1, 1, false);
  }

  @Test
  public void testViewForFileSlicesWithNoBaseFileAndInflightCompaction() throws Exception {
    testViewForFileSlicesWithAsyncCompaction(true, true, 1, 1, false);
  }

  @Test
  public void testViewForFileSlicesWithBaseFileAndInflightCompaction() throws Exception {
    testViewForFileSlicesWithAsyncCompaction(false, true, 1, 1, false);
  }

  @Test
  public void testStreamEveryVersionInPartition() throws IOException {
    testStreamEveryVersionInPartition(true);
  }

  @Test
  public void testStreamLatestVersionsBefore() throws IOException {
    testStreamLatestVersionsBefore(true);
  }

  @Test
  public void testStreamLatestVersionInRange() throws IOException {
    testStreamLatestVersionInRange(true);
  }

  @Test
  public void testStreamLatestVersionInPartition() throws IOException {
    testStreamLatestVersionInPartition(true);
  }

  @Test
  public void testStreamLatestVersions() throws IOException {
    testStreamLatestVersions(true);
  }

  @Test(expected = IllegalStateException.class)
  public void testBrokenSeal() throws IOException {
    // Put some files in the partition
    String partitionPath = "2016/05/01/";
    String fullPartitionPath = basePath + "/" + partitionPath;
    new File(fullPartitionPath).mkdirs();
    String commitTime1 = "1";
    String commitTime2 = "2";
    String commitTime3 = "3";
    String commitTime4 = "4";
    String fileId1 = UUID.randomUUID().toString();
    String fileId2 = UUID.randomUUID().toString();
    String fileId3 = UUID.randomUUID().toString();

    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime1, 1, fileId1)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime1, 1, fileId2)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime2, 1, fileId2)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime3, 1, fileId2)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime3, 1, fileId3)).createNewFile();

    new File(basePath + "/.hoodie/" + commitTime1 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime2 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime3 + ".commit").createNewFile();

    // Now we list the entire partition
    FileStatus[] statuses = metaClient.getFs().listStatus(new Path(fullPartitionPath));
    assertEquals(5, statuses.length);
    refreshFsView(null);
    List<HoodieDataFile> dataFiles = roView.getLatestDataFilesBeforeOrOn(partitionPath, commitTime3)
        .collect(Collectors.toList());
    assertEquals(3, dataFiles.size());

    // Seal the view
    ((LatestFileSliceOnlyFSViewImpl) fsView).seal();

    String newPartitionPath = "2016/05/02/";
    String fullNewPartitionPath = basePath + "/" + newPartitionPath;
    new File(fullNewPartitionPath).mkdirs();
    new File(fullNewPartitionPath + FSUtils.makeDataFileName(commitTime4, 1, fileId1)).createNewFile();
    new File(fullNewPartitionPath + FSUtils.makeDataFileName(commitTime4, 1, fileId3)).createNewFile();
    new File(basePath + "/.hoodie/" + commitTime4 + ".commit").createNewFile();

    roView.getLatestDataFilesBeforeOrOn(newPartitionPath, commitTime4).collect(Collectors.toList());
  }
}