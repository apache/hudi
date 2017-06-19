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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.uber.hoodie.common.model.HoodieDataFile;
import com.uber.hoodie.common.model.HoodieFileGroup;
import com.uber.hoodie.common.model.HoodieTestUtils;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.TableFileSystemView;
import com.uber.hoodie.common.table.timeline.HoodieActiveTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.util.FSUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

@SuppressWarnings("ResultOfMethodCallIgnored")
public class HoodieTableFileSystemViewTest {
    private HoodieTableMetaClient metaClient;
    private String basePath;
    private TableFileSystemView fsView;

    @Before
    public void init() throws IOException {
        TemporaryFolder folder = new TemporaryFolder();
        folder.create();
        this.basePath = folder.getRoot().getAbsolutePath();
        metaClient = HoodieTestUtils.init(basePath);
        fsView = new HoodieTableFileSystemView(metaClient,
            metaClient.getActiveTimeline().getCommitTimeline().filterCompletedInstants());
    }

    private void refreshFsView(FileStatus[] statuses) {
        metaClient = new HoodieTableMetaClient(HoodieTestUtils.fs, basePath, true);
        if (statuses != null) {
            fsView = new HoodieTableFileSystemView(metaClient,
                    metaClient.getActiveTimeline().getCommitTimeline().filterCompletedInstants(),
                    statuses);
        } else {
            fsView = new HoodieTableFileSystemView(metaClient,
                    metaClient.getActiveTimeline().getCommitTimeline().filterCompletedInstants());
        }
    }

    @Test
    public void testGetLatestDataFilesForFileId() throws IOException {
        String partitionPath = "2016/05/01";
        new File(basePath + "/" + partitionPath).mkdirs();
        String fileId = UUID.randomUUID().toString();

        assertFalse("No commit, should not find any data file",
            fsView.getLatestDataFiles(partitionPath)
                    .filter(dfile -> dfile.getFileId().equals(fileId)).findFirst().isPresent());

        // Only one commit, but is not safe
        String commitTime1 = "1";
        String fileName1 = FSUtils.makeDataFileName(commitTime1, 1, fileId);
        new File(basePath + "/" + partitionPath + "/" + fileName1).createNewFile();
        refreshFsView(null);
        assertFalse("No commit, should not find any data file",
            fsView.getLatestDataFiles(partitionPath)
                    .filter(dfile -> dfile.getFileId().equals(fileId))
                    .findFirst().isPresent());

        // Make this commit safe
        HoodieActiveTimeline commitTimeline = metaClient.getActiveTimeline();
        HoodieInstant instant1 =
            new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, commitTime1);
        commitTimeline.saveAsComplete(instant1, Optional.empty());
        refreshFsView(null);
        assertEquals("", fileName1, fsView
                .getLatestDataFiles(partitionPath)
                .filter(dfile -> dfile.getFileId().equals(fileId))
                .findFirst().get()
                .getFileName());

        // Do another commit, but not safe
        String commitTime2 = "2";
        String fileName2 = FSUtils.makeDataFileName(commitTime2, 1, fileId);
        new File(basePath + "/" + partitionPath + "/" + fileName2).createNewFile();
        refreshFsView(null);
        assertEquals("", fileName1, fsView
                .getLatestDataFiles(partitionPath)
                .filter(dfile -> dfile.getFileId().equals(fileId))
                .findFirst().get()
                .getFileName());

        // Make it safe
        HoodieInstant instant2 =
            new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, commitTime2);
        commitTimeline.saveAsComplete(instant2, Optional.empty());
        refreshFsView(null);
        assertEquals("", fileName2, fsView
                .getLatestDataFiles(partitionPath)
                .filter(dfile -> dfile.getFileId().equals(fileId))
                .findFirst().get()
                .getFileName());
    }

    @Test
    public void testStreamLatestVersionInPartition() throws IOException {
        // Put some files in the partition
        String fullPartitionPath = basePath + "/2016/05/01/";
        new File(fullPartitionPath).mkdirs();
        String commitTime1 = "1";
        String commitTime2 = "2";
        String commitTime3 = "3";
        String commitTime4 = "4";
        String fileId1 = UUID.randomUUID().toString();
        String fileId2 = UUID.randomUUID().toString();
        String fileId3 = UUID.randomUUID().toString();

        new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime1, 1, fileId1))
            .createNewFile();
        new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime4, 1, fileId1))
            .createNewFile();
        new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime1, 1, fileId2))
            .createNewFile();
        new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime2, 1, fileId2))
            .createNewFile();
        new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime3, 1, fileId2))
            .createNewFile();
        new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime3, 1, fileId3))
            .createNewFile();
        new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime4, 1, fileId3))
            .createNewFile();

        new File(basePath + "/.hoodie/" + commitTime1 + ".commit").createNewFile();
        new File(basePath + "/.hoodie/" + commitTime2 + ".commit").createNewFile();
        new File(basePath + "/.hoodie/" + commitTime3 + ".commit").createNewFile();
        new File(basePath + "/.hoodie/" + commitTime4 + ".commit").createNewFile();

        // Now we list the entire partition
        FileStatus[] statuses = HoodieTestUtils.fs.listStatus(new Path(fullPartitionPath));
        assertEquals(statuses.length, 7);

        refreshFsView(null);
        List<HoodieDataFile> dataFileList =
            fsView.getLatestDataFilesBeforeOrOn("2016/05/01", commitTime4)
                .collect(Collectors.toList());
        assertEquals(dataFileList.size(), 3);
        Set<String> filenames = Sets.newHashSet();
        for (HoodieDataFile status : dataFileList) {
            filenames.add(status.getFileName());
        }
        assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime4, 1, fileId1)));
        assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime3, 1, fileId2)));
        assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime4, 1, fileId3)));

        // Reset the max commit time
        List<HoodieDataFile> statuses2 =
            fsView.getLatestDataFilesBeforeOrOn("2016/05/01", commitTime3)
                .collect(Collectors.toList());
        assertEquals(statuses2.size(), 3);
        filenames = Sets.newHashSet();
        for (HoodieDataFile status : statuses2) {
            filenames.add(status.getFileName());
        }
        assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime1, 1, fileId1)));
        assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime3, 1, fileId2)));
        assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime3, 1, fileId3)));
    }

    @Test
    public void testStreamEveryVersionInPartition() throws IOException {
        // Put some files in the partition
        String fullPartitionPath = basePath + "/2016/05/01/";
        new File(fullPartitionPath).mkdirs();
        String commitTime1 = "1";
        String commitTime2 = "2";
        String commitTime3 = "3";
        String commitTime4 = "4";
        String fileId1 = UUID.randomUUID().toString();
        String fileId2 = UUID.randomUUID().toString();
        String fileId3 = UUID.randomUUID().toString();

        new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime1, 1, fileId1))
            .createNewFile();
        new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime4, 1, fileId1))
            .createNewFile();
        new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime1, 1, fileId2))
            .createNewFile();
        new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime2, 1, fileId2))
            .createNewFile();
        new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime3, 1, fileId2))
            .createNewFile();
        new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime3, 1, fileId3))
            .createNewFile();
        new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime4, 1, fileId3))
            .createNewFile();

        new File(basePath + "/.hoodie/" + commitTime1 + ".commit").createNewFile();
        new File(basePath + "/.hoodie/" + commitTime2 + ".commit").createNewFile();
        new File(basePath + "/.hoodie/" + commitTime3 + ".commit").createNewFile();
        new File(basePath + "/.hoodie/" + commitTime4 + ".commit").createNewFile();

        // Now we list the entire partition
        FileStatus[] statuses = HoodieTestUtils.fs.listStatus(new Path(fullPartitionPath));
        assertEquals(statuses.length, 7);

        refreshFsView(null);
        List<HoodieFileGroup> fileGroups =
            fsView.getAllFileGroups("2016/05/01").collect(Collectors.toList());
        assertEquals(fileGroups.size(), 3);

        for (HoodieFileGroup fileGroup : fileGroups) {
            String fileId = fileGroup.getId();
            Set<String> filenames = Sets.newHashSet();
            fileGroup.getAllDataFiles().forEach(dataFile -> {
                assertEquals("All same fileId should be grouped", fileId, dataFile.getFileId());
                filenames.add(dataFile.getFileName());
            });
            if (fileId.equals(fileId1)) {
                assertEquals(filenames,
                    Sets.newHashSet(FSUtils.makeDataFileName(commitTime1, 1, fileId1),
                        FSUtils.makeDataFileName(commitTime4, 1, fileId1)));
            } else if (fileId.equals(fileId2)) {
                assertEquals(filenames,
                    Sets.newHashSet(FSUtils.makeDataFileName(commitTime1, 1, fileId2),
                        FSUtils.makeDataFileName(commitTime2, 1, fileId2),
                        FSUtils.makeDataFileName(commitTime3, 1, fileId2)));
            } else {
                assertEquals(filenames,
                    Sets.newHashSet(FSUtils.makeDataFileName(commitTime3, 1, fileId3),
                        FSUtils.makeDataFileName(commitTime4, 1, fileId3)));
            }
        }
    }

    @Test
    public void streamLatestVersionInRange() throws IOException {
        // Put some files in the partition
        String fullPartitionPath = basePath + "/2016/05/01/";
        new File(fullPartitionPath).mkdirs();
        String commitTime1 = "1";
        String commitTime2 = "2";
        String commitTime3 = "3";
        String commitTime4 = "4";
        String fileId1 = UUID.randomUUID().toString();
        String fileId2 = UUID.randomUUID().toString();
        String fileId3 = UUID.randomUUID().toString();

        new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime1, 1, fileId1))
            .createNewFile();
        new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime4, 1, fileId1))
            .createNewFile();
        new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime1, 1, fileId2))
            .createNewFile();
        new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime2, 1, fileId2))
            .createNewFile();
        new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime3, 1, fileId2))
            .createNewFile();
        new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime3, 1, fileId3))
            .createNewFile();
        new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime4, 1, fileId3))
            .createNewFile();

        new File(basePath + "/.hoodie/" + commitTime1 + ".commit").createNewFile();
        new File(basePath + "/.hoodie/" + commitTime2 + ".commit").createNewFile();
        new File(basePath + "/.hoodie/" + commitTime3 + ".commit").createNewFile();
        new File(basePath + "/.hoodie/" + commitTime4 + ".commit").createNewFile();

        // Now we list the entire partition
        FileStatus[] statuses = HoodieTestUtils.fs.listStatus(new Path(fullPartitionPath));
        assertEquals(statuses.length, 7);

        refreshFsView(statuses);
        List<HoodieDataFile> statuses1 = fsView
            .getLatestDataFilesInRange(Lists.newArrayList(commitTime2, commitTime3))
            .collect(Collectors.toList());
        assertEquals(statuses1.size(), 2);
        Set<String> filenames = Sets.newHashSet();
        for (HoodieDataFile status : statuses1) {
            filenames.add(status.getFileName());
        }
        assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime3, 1, fileId2)));
        assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime3, 1, fileId3)));
    }

    @Test
    public void streamLatestVersionsBefore() throws IOException {
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

        new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime1, 1, fileId1))
            .createNewFile();
        new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime4, 1, fileId1))
            .createNewFile();
        new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime1, 1, fileId2))
            .createNewFile();
        new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime2, 1, fileId2))
            .createNewFile();
        new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime3, 1, fileId2))
            .createNewFile();
        new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime3, 1, fileId3))
            .createNewFile();
        new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime4, 1, fileId3))
            .createNewFile();

        new File(basePath + "/.hoodie/" + commitTime1 + ".commit").createNewFile();
        new File(basePath + "/.hoodie/" + commitTime2 + ".commit").createNewFile();
        new File(basePath + "/.hoodie/" + commitTime3 + ".commit").createNewFile();
        new File(basePath + "/.hoodie/" + commitTime4 + ".commit").createNewFile();

        // Now we list the entire partition
        FileStatus[] statuses = HoodieTestUtils.fs.listStatus(new Path(fullPartitionPath));
        assertEquals(statuses.length, 7);

        refreshFsView(null);
        List<HoodieDataFile> statuses1 =
            fsView.getLatestDataFilesBeforeOrOn(partitionPath, commitTime2)
                .collect(Collectors.toList());
        assertEquals(statuses1.size(), 2);
        Set<String> filenames = Sets.newHashSet();
        for (HoodieDataFile status : statuses1) {
            filenames.add(status.getFileName());
        }
        assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime1, 1, fileId1)));
        assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime2, 1, fileId2)));

    }

    @Test
    public void streamLatestVersions() throws IOException {
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

        new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime1, 1, fileId1))
            .createNewFile();
        new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime4, 1, fileId1))
            .createNewFile();
        new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime1, 1, fileId2))
            .createNewFile();
        new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime2, 1, fileId2))
            .createNewFile();
        new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime3, 1, fileId2))
            .createNewFile();
        new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime3, 1, fileId3))
            .createNewFile();
        new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime4, 1, fileId3))
            .createNewFile();

        new File(basePath + "/.hoodie/" + commitTime1 + ".commit").createNewFile();
        new File(basePath + "/.hoodie/" + commitTime2 + ".commit").createNewFile();
        new File(basePath + "/.hoodie/" + commitTime3 + ".commit").createNewFile();
        new File(basePath + "/.hoodie/" + commitTime4 + ".commit").createNewFile();

        // Now we list the entire partition
        FileStatus[] statuses = HoodieTestUtils.fs.listStatus(new Path(fullPartitionPath));
        assertEquals(statuses.length, 7);

        refreshFsView(statuses);
        List<HoodieDataFile> statuses1 =
            fsView.getLatestDataFiles().collect(Collectors.toList());
        assertEquals(statuses1.size(), 3);
        Set<String> filenames = Sets.newHashSet();
        for (HoodieDataFile status : statuses1) {
            filenames.add(status.getFileName());
        }
        assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime4, 1, fileId1)));
        assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime3, 1, fileId2)));
        assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime4, 1, fileId3)));
    }
}
