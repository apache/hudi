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

package com.uber.hoodie.io;

import com.google.common.collect.Lists;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.common.HoodieTestDataGenerator;
import com.uber.hoodie.common.model.HoodieCommitMetadata;
import com.uber.hoodie.common.model.HoodieTableMetadata;
import com.uber.hoodie.common.model.HoodieTestUtils;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.config.HoodieCompactionConfig;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestHoodieCommitArchiveLog {
    private String basePath;
    private FileSystem fs;

    @Before
    public void init() throws Exception {
        TemporaryFolder folder = new TemporaryFolder();
        folder.create();
        basePath = folder.getRoot().getAbsolutePath();
        HoodieTestUtils.initializeHoodieDirectory(basePath);
        fs = FSUtils.getFs();
    }

    @Test
    public void testArchiveEmptyDataset() throws IOException {
        HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(basePath)
            .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
            .forTable("test-trip-table").build();
        HoodieCommitArchiveLog archiveLog = new HoodieCommitArchiveLog(cfg);
        boolean result = archiveLog.archiveIfRequired();
        assertTrue(result);
    }

    @Test
    public void testArchiveDatasetWithNoArchival() throws IOException {
        HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(basePath)
            .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
            .forTable("test-trip-table").withCompactionConfig(
                HoodieCompactionConfig.newBuilder().archiveCommitsWith(2, 5).build()).build();
        HoodieCommitArchiveLog archiveLog = new HoodieCommitArchiveLog(cfg);
        HoodieTestDataGenerator.createCommitFile(basePath, "100");
        HoodieTestDataGenerator.createCommitFile(basePath, "101");
        HoodieTestDataGenerator.createCommitFile(basePath, "102");
        HoodieTestDataGenerator.createCommitFile(basePath, "103");

        HoodieTableMetadata metadata = new HoodieTableMetadata(fs, basePath);
        assertEquals("Loaded 4 commits and the count should match", 4,
            metadata.getAllCommits().getCommitList().size());
        boolean result = archiveLog.archiveIfRequired();
        assertTrue(result);
        metadata = new HoodieTableMetadata(fs, basePath);
        assertEquals("Should not archive commits when maxCommitsToKeep is 5", 4,
            metadata.getAllCommits().getCommitList().size());
    }

    @Test
    public void testArchiveDatasetWithArchival() throws IOException {
        HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(basePath)
            .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
            .forTable("test-trip-table").withCompactionConfig(
                HoodieCompactionConfig.newBuilder().archiveCommitsWith(2, 5).build()).build();
        HoodieCommitArchiveLog archiveLog = new HoodieCommitArchiveLog(cfg);
        HoodieTestDataGenerator.createCommitFile(basePath, "100");
        HoodieTestDataGenerator.createCommitFile(basePath, "101");
        HoodieTestDataGenerator.createCommitFile(basePath, "102");
        HoodieTestDataGenerator.createCommitFile(basePath, "103");
        HoodieTestDataGenerator.createCommitFile(basePath, "104");
        HoodieTestDataGenerator.createCommitFile(basePath, "105");

        HoodieTableMetadata metadata = new HoodieTableMetadata(fs, basePath);
        SortedMap<String, HoodieCommitMetadata> originalCommits = new TreeMap<>(metadata.getAllCommitMetadata());

        assertEquals("Loaded 6 commits and the count should match", 6,
            metadata.getAllCommits().getCommitList().size());
        boolean result = archiveLog.archiveIfRequired();
        assertTrue(result);
        metadata = new HoodieTableMetadata(fs, basePath);
        assertEquals(
            "Should archive commits when maxCommitsToKeep is 5 and now the commits length should be minCommitsToKeep which is 2",
            2, metadata.getAllCommits().getCommitList().size());
        assertEquals("Archive should not archive the last 2 commits",
            Lists.newArrayList("104", "105"), metadata.getAllCommits().getCommitList());

        // Remove all the commits from the original commits, make it ready to be checked against the read map
        for(String key:metadata.getAllCommitMetadata().keySet()) {
            originalCommits.remove(key);
        }

        // Read back the commits to make sure
        SequenceFile.Reader reader = new SequenceFile.Reader(fs.getConf(),
            SequenceFile.Reader.file(archiveLog.getArchiveFilePath()));
        Text key = new Text();
        Text val = new Text();
        SortedMap<String, HoodieCommitMetadata> readCommits = new TreeMap<>();
        while (reader.next(key, val)) {
            HoodieCommitMetadata meta = HoodieCommitMetadata.fromJsonString(val.toString());
            readCommits.put(key.toString(), meta);
        }

        assertEquals(
            "Read commits map should match the originalCommits - commitsLoadedAfterArchival",
            originalCommits, readCommits);
        reader.close();
    }

    @Test
    public void testArchiveCommitSafety() throws IOException {
        HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(basePath)
            .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
            .forTable("test-trip-table").withCompactionConfig(
                HoodieCompactionConfig.newBuilder().archiveCommitsWith(2, 5).build()).build();
        HoodieCommitArchiveLog archiveLog = new HoodieCommitArchiveLog(cfg);
        HoodieTestDataGenerator.createCommitFile(basePath, "100");
        HoodieTestDataGenerator.createCommitFile(basePath, "101");
        HoodieTestDataGenerator.createCommitFile(basePath, "102");
        HoodieTestDataGenerator.createCommitFile(basePath, "103");
        HoodieTestDataGenerator.createCommitFile(basePath, "104");
        HoodieTestDataGenerator.createCommitFile(basePath, "105");

        HoodieTableMetadata metadata = new HoodieTableMetadata(fs, basePath);
        assertEquals("Loaded 6 commits and the count should match", 6,
            metadata.getAllCommits().getCommitList().size());
        boolean result = archiveLog.archiveIfRequired();
        assertTrue(result);
        metadata = new HoodieTableMetadata(fs, basePath);
        assertTrue("Archived commits should always be safe", metadata.isCommitTsSafe("100"));
        assertTrue("Archived commits should always be safe", metadata.isCommitTsSafe("101"));
        assertTrue("Archived commits should always be safe", metadata.isCommitTsSafe("102"));
        assertTrue("Archived commits should always be safe", metadata.isCommitTsSafe("103"));
    }



}
