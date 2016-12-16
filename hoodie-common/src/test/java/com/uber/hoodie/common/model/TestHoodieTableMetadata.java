/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.common.model;

import com.google.common.collect.Sets;

import com.uber.hoodie.common.util.FSUtils;

import com.uber.hoodie.exception.HoodieIOException;
import com.uber.hoodie.exception.HoodieRecordMissingException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestHoodieTableMetadata {
    private String basePath = null;
    private HoodieTableMetadata metadata = null;
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Before
    public void init() throws Exception {
        basePath = HoodieTestUtils.initializeTempHoodieBasePath();
        metadata = new HoodieTableMetadata(FSUtils.getFs(), basePath, "testTable");
    }

    @Test
    public void testScanCommitTs() throws Exception {
        // Empty commit dir
        assertTrue(metadata.getAllCommits().isEmpty());

        // Create some commit files
        new File(basePath + "/.hoodie/20160504123032.commit").createNewFile();
        new File(basePath + "/.hoodie/20160503122032.commit").createNewFile();
        metadata = new HoodieTableMetadata(FSUtils.getFs(), basePath, "testTable");
        List<String> list = metadata.getAllCommits().getCommitList();
        assertEquals(list.size(), 2);
        assertTrue(list.contains("20160504123032"));
        assertTrue(list.contains("20160503122032"));

        // Check the .inflight files
        assertTrue(metadata.getAllInflightCommits().isEmpty());
        new File(basePath + "/.hoodie/20160505123032.inflight").createNewFile();
        new File(basePath + "/.hoodie/20160506122032.inflight").createNewFile();
        metadata = new HoodieTableMetadata(FSUtils.getFs(), basePath, "testTable");
        list = metadata.getAllInflightCommits();
        assertEquals(list.size(), 2);
        assertTrue(list.contains("20160505123032"));
        assertTrue(list.contains("20160506122032"));
    }

    @Test
    public void testGetLastValidFileNameForRecord() throws Exception {
        FileSystem fs = FSUtils.getFs();
        String partitionPath = "2016/05/01";
        new File(basePath + "/" + partitionPath).mkdirs();
        String fileId = UUID.randomUUID().toString();
        HoodieRecord record = mock(HoodieRecord.class);
        when(record.getPartitionPath()).thenReturn(partitionPath);
        when(record.getCurrentLocation()).thenReturn(new HoodieRecordLocation("001", fileId));

        // First, no commit for this record
        exception.expect(HoodieIOException.class);
        metadata.getFilenameForRecord(fs, record);

        // Only one commit, but is not safe
        String commitTime1 = "20160501123212";
        String fileName1 = FSUtils.makeDataFileName(commitTime1, 1, fileId);
        new File(basePath + "/" + partitionPath + "/" + fileName1).createNewFile();
        assertNull(metadata.getFilenameForRecord(fs, record));

        // Make this commit safe
        new File(basePath + "/.hoodie/" + commitTime1 + ".commit").createNewFile();
        metadata = new HoodieTableMetadata(fs, basePath, "testTable");
        assertTrue(metadata.getFilenameForRecord(fs, record).equals(fileName1));

        // Do another commit, but not safe
        String commitTime2 = "20160502123012";
        String fileName2 = FSUtils.makeDataFileName(commitTime2, 1, fileId);
        new File(basePath + "/" + partitionPath + "/" + fileName2).createNewFile();
        assertTrue(metadata.getFilenameForRecord(fs, record).equals(fileName1));

        // Make it safe
        new File(basePath + "/.hoodie/" + commitTime2 + ".commit").createNewFile();
        metadata = new HoodieTableMetadata(fs, basePath, "testTable");
        assertTrue(metadata.getFilenameForRecord(fs, record).equals(fileName2));
    }

    @Test
    public void testGetAllPartitionPaths() throws IOException {
        FileSystem fs = FSUtils.getFs();

        // Empty
        List<String> partitions = FSUtils.getAllPartitionPaths(fs, basePath);
        assertEquals(partitions.size(), 0);

        // Add some dirs
        new File(basePath + "/2016/04/01").mkdirs();
        new File(basePath + "/2015/04/01").mkdirs();
        partitions = FSUtils.getAllPartitionPaths(fs, basePath);
        assertEquals(partitions.size(), 2);
        assertTrue(partitions.contains("2016/04/01"));
        assertTrue(partitions.contains("2015/04/01"));
    }

    @Test
    public void testGetFileVersionsInPartition() throws IOException {
        // Put some files in the partition
        String fullPartitionPath = basePath + "/2016/05/01/";
        new File(fullPartitionPath).mkdirs();

        String commitTime1 = "20160501123032";
        String commitTime2 = "20160502123032";
        String commitTime3 = "20160503123032";
        String commitTime4 = "20160504123032";

        HoodieTestUtils.createCommitFiles(basePath, commitTime1, commitTime2, commitTime3, commitTime4);

        String fileId1 = UUID.randomUUID().toString();
        String fileId2 = UUID.randomUUID().toString();
        String fileId3 = UUID.randomUUID().toString();

        new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime1, 1, fileId1)).createNewFile();
        new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime4, 1, fileId1)).createNewFile();
        new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime1, 1, fileId2)).createNewFile();
        new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime2, 1, fileId2)).createNewFile();
        new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime3, 1, fileId2)).createNewFile();
        new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime3, 1, fileId3)).createNewFile();
        new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime4, 1, fileId3)).createNewFile();

        metadata = new HoodieTableMetadata(FSUtils.getFs(), basePath, "testTable");

        Map<String, List<FileStatus>> fileVersions = metadata.getAllVersionsInPartition(FSUtils.getFs(), "2016/05/01");
        assertEquals(fileVersions.get(fileId1).size(), 2);
        assertEquals(fileVersions.get(fileId2).size(), 3);
        assertEquals(fileVersions.get(fileId3).size(), 2);
        String commitTs = FSUtils.getCommitTime(fileVersions.get(fileId1).get(fileVersions.get(fileId1).size() - 1).getPath().getName());
        assertTrue(commitTs.equals(commitTime1));
        commitTs = FSUtils.getCommitTime(fileVersions.get(fileId1).get(fileVersions.get(fileId1).size() - 2).getPath().getName());
        assertTrue(commitTs.equals(commitTime4));
    }

    @Test
    public void testGetOnlyLatestVersionFiles() throws Exception {
        // Put some files in the partition
        String fullPartitionPath = basePath + "/2016/05/01/";
        new File(fullPartitionPath).mkdirs();
        String commitTime1 = "20160501123032";
        String commitTime2 = "20160502123032";
        String commitTime3 = "20160503123032";
        String commitTime4 = "20160504123032";
        String fileId1 = UUID.randomUUID().toString();
        String fileId2 = UUID.randomUUID().toString();
        String fileId3 = UUID.randomUUID().toString();

        new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime1, 1, fileId1)).createNewFile();
        new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime4, 1, fileId1)).createNewFile();
        new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime1, 1, fileId2)).createNewFile();
        new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime2, 1, fileId2)).createNewFile();
        new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime3, 1, fileId2)).createNewFile();
        new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime3, 1, fileId3)).createNewFile();
        new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime4, 1, fileId3)).createNewFile();

        new File(basePath + "/.hoodie/" + commitTime1 + ".commit").createNewFile();
        new File(basePath + "/.hoodie/" + commitTime2 + ".commit").createNewFile();
        new File(basePath + "/.hoodie/" + commitTime3 + ".commit").createNewFile();
        new File(basePath + "/.hoodie/" + commitTime4 + ".commit").createNewFile();

        // Now we list the entire partition
        FileSystem fs = FSUtils.getFs();
        FileStatus[] statuses = fs.listStatus(new Path(fullPartitionPath));
        assertEquals(statuses.length, 7);

        metadata = new HoodieTableMetadata(fs, basePath, "testTable");
        FileStatus[] statuses1 = metadata
            .getLatestVersionInPartition(fs, "2016/05/01", commitTime4);
        assertEquals(statuses1.length, 3);
        Set<String> filenames = Sets.newHashSet();
        for (FileStatus status : statuses1) {
            filenames.add(status.getPath().getName());
        }
        assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime4, 1, fileId1)));
        assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime3, 1, fileId2)));
        assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime4, 1, fileId3)));

        // Reset the max commit time
        FileStatus[] statuses2 = metadata
            .getLatestVersionInPartition(fs, "2016/05/01", commitTime3);
        assertEquals(statuses2.length, 3);
        filenames = Sets.newHashSet();
        for (FileStatus status : statuses2) {
            filenames.add(status.getPath().getName());
        }
        assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime1, 1, fileId1)));
        assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime3, 1, fileId2)));
        assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime3, 1, fileId3)));
    }

    @Test
    public void testCommitTimeComparison() {
        String commitTime1 = "20160504123032";
        String commitTime2 = "20151231203159";
        assertTrue(HoodieCommits.isCommit1After(commitTime1, commitTime2));
        assertTrue(HoodieCommits.isCommit1BeforeOrOn(commitTime1, commitTime1));
        assertTrue(HoodieCommits.isCommit1BeforeOrOn(commitTime2, commitTime1));
    }

    @After
    public void cleanup() {
        if (basePath != null) {
            new File(basePath).delete();
        }
    }
}
