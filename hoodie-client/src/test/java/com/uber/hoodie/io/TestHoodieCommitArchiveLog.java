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
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.common.HoodieTestDataGenerator;
import com.uber.hoodie.common.model.HoodieCommitMetadata;
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
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

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
        HoodieTestUtils.init(basePath);
        fs = FSUtils.getFs();
    }

    @Test
    public void testArchiveEmptyDataset() throws IOException {
        HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(basePath)
            .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
            .forTable("test-trip-table").build();
        HoodieCommitArchiveLog archiveLog = new HoodieCommitArchiveLog(cfg, fs);
        boolean result = archiveLog.archiveIfRequired();
        assertTrue(result);
    }

    @Test
    public void testArchiveDatasetWithNoArchival() throws IOException {
        HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(basePath)
            .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
            .forTable("test-trip-table").withCompactionConfig(
                HoodieCompactionConfig.newBuilder().archiveCommitsWith(2, 5).build()).build();
        HoodieTableMetaClient metadata = new HoodieTableMetaClient(fs, basePath);
        HoodieCommitArchiveLog archiveLog = new HoodieCommitArchiveLog(cfg, fs);
        HoodieTestDataGenerator.createCommitFile(basePath, "100");
        HoodieTestDataGenerator.createCommitFile(basePath, "101");
        HoodieTestDataGenerator.createCommitFile(basePath, "102");
        HoodieTestDataGenerator.createCommitFile(basePath, "103");

        HoodieTimeline timeline = metadata.getActiveCommitTimeline();

        assertEquals("Loaded 4 commits and the count should match", 4,
            timeline.getTotalInstants());
        boolean result = archiveLog.archiveIfRequired();
        assertTrue(result);
        timeline = timeline.reload();
        assertEquals("Should not archive commits when maxCommitsToKeep is 5", 4,
            timeline.getTotalInstants());
    }

    @Test
    public void testArchiveDatasetWithArchival() throws IOException {
        HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(basePath)
            .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
            .forTable("test-trip-table").withCompactionConfig(
                HoodieCompactionConfig.newBuilder().archiveCommitsWith(2, 5).build()).build();
        HoodieTableMetaClient metadata = new HoodieTableMetaClient(fs, basePath);
        HoodieCommitArchiveLog archiveLog = new HoodieCommitArchiveLog(cfg, fs);
        HoodieTestDataGenerator.createCommitFile(basePath, "100");
        HoodieTestDataGenerator.createCommitFile(basePath, "101");
        HoodieTestDataGenerator.createCommitFile(basePath, "102");
        HoodieTestDataGenerator.createCommitFile(basePath, "103");
        HoodieTestDataGenerator.createCommitFile(basePath, "104");
        HoodieTestDataGenerator.createCommitFile(basePath, "105");

        HoodieTimeline timeline = metadata.getActiveCommitTimeline();
        List<String> originalCommits = timeline.getInstants().collect(
            Collectors.toList());

        assertEquals("Loaded 6 commits and the count should match", 6, timeline.getTotalInstants());
        boolean result = archiveLog.archiveIfRequired();
        assertTrue(result);
        timeline = timeline.reload();
        assertEquals(
            "Should archive commits when maxCommitsToKeep is 5 and now the commits length should be minCommitsToKeep which is 2",
            2, timeline.getTotalInstants());
        assertEquals("Archive should not archive the last 2 commits",
            Lists.newArrayList("104", "105"), timeline.getInstants().collect(Collectors.toList()));

        // Remove all the commits from the original commits, make it ready to be checked against the read map
        timeline.getInstants().forEach(originalCommits::remove);

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
            originalCommits, new ArrayList<>(readCommits.keySet()));
        reader.close();
    }

    @Test
    public void testArchiveCommitSafety() throws IOException {
        HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(basePath)
            .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
            .forTable("test-trip-table").withCompactionConfig(
                HoodieCompactionConfig.newBuilder().archiveCommitsWith(2, 5).build()).build();
        HoodieTableMetaClient metadata = new HoodieTableMetaClient(fs, basePath);
        HoodieCommitArchiveLog archiveLog = new HoodieCommitArchiveLog(cfg, fs);
        HoodieTestDataGenerator.createCommitFile(basePath, "100");
        HoodieTestDataGenerator.createCommitFile(basePath, "101");
        HoodieTestDataGenerator.createCommitFile(basePath, "102");
        HoodieTestDataGenerator.createCommitFile(basePath, "103");
        HoodieTestDataGenerator.createCommitFile(basePath, "104");
        HoodieTestDataGenerator.createCommitFile(basePath, "105");

        HoodieTimeline timeline = metadata.getActiveCommitTimeline();
        assertEquals("Loaded 6 commits and the count should match", 6, timeline.getTotalInstants());
        boolean result = archiveLog.archiveIfRequired();
        assertTrue(result);
        timeline = timeline.reload();
        assertTrue("Archived commits should always be safe", timeline.containsOrBeforeTimelineStarts("100"));
        assertTrue("Archived commits should always be safe", timeline.containsOrBeforeTimelineStarts("101"));
        assertTrue("Archived commits should always be safe", timeline.containsOrBeforeTimelineStarts("102"));
        assertTrue("Archived commits should always be safe", timeline.containsOrBeforeTimelineStarts("103"));
    }



}
