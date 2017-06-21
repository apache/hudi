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

import com.uber.hoodie.avro.model.HoodieArchivedMetaEntry;
import com.uber.hoodie.common.HoodieTestDataGenerator;
import com.uber.hoodie.common.model.HoodieArchivedLogFile;
import com.uber.hoodie.common.model.HoodieLogFile;
import com.uber.hoodie.common.model.HoodieTestUtils;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.log.HoodieLogFormat;
import com.uber.hoodie.common.table.log.block.HoodieAvroDataBlock;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.config.HoodieCompactionConfig;
import com.uber.hoodie.config.HoodieWriteConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
    public void testArchiveDatasetWithArchival() throws IOException {
        HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(basePath)
                .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
                .withCompactionConfig(HoodieCompactionConfig.newBuilder().archiveCommitsWith(2, 4).build())
                .forTable("test-trip-table").build();
        HoodieTestUtils.init(basePath);
        HoodieTestDataGenerator.createCommitFile(basePath, "100");
        HoodieTestDataGenerator.createCommitFile(basePath, "101");
        HoodieTestDataGenerator.createCommitFile(basePath, "102");
        HoodieTestDataGenerator.createCommitFile(basePath, "103");
        HoodieTestDataGenerator.createCommitFile(basePath, "104");
        HoodieTestDataGenerator.createCommitFile(basePath, "105");

        HoodieTableMetaClient metadata = new HoodieTableMetaClient(fs, basePath);
        HoodieTimeline timeline =
                metadata.getActiveTimeline().getCommitsAndCompactionsTimeline().filterCompletedInstants();

        assertEquals("Loaded 6 commits and the count should match", 6, timeline.countInstants());

        HoodieTestUtils.createCleanFiles(basePath, "100");
        HoodieTestUtils.createCleanFiles(basePath, "101");
        HoodieTestUtils.createCleanFiles(basePath, "102");
        HoodieTestUtils.createCleanFiles(basePath, "103");
        HoodieTestUtils.createCleanFiles(basePath, "104");
        HoodieTestUtils.createCleanFiles(basePath, "105");

        //reload the timeline and get all the commmits before archive
        timeline = metadata.getActiveTimeline().reload().getAllCommitsTimeline().filterCompletedInstants();
        List<HoodieInstant> originalCommits = timeline.getInstants().collect(Collectors.toList());

        assertEquals("Loaded 6 commits and the count should match", 12, timeline.countInstants());

        HoodieCommitArchiveLog archiveLog = new HoodieCommitArchiveLog(cfg, fs);

        assertTrue(archiveLog.archiveIfRequired());

        //reload the timeline and remove the remaining commits
        timeline = metadata.getActiveTimeline().reload().getAllCommitsTimeline().filterCompletedInstants();
        originalCommits.removeAll(timeline.getInstants().collect(Collectors.toList()));

        //read the file
        HoodieLogFormat.Reader reader = HoodieLogFormat.newReader(FSUtils.getFs(),
                new HoodieLogFile(new Path(basePath + "/.hoodie/.commits_.archive.1")), HoodieArchivedMetaEntry.getClassSchema());

        int archivedRecordsCount = 0;
        List<IndexedRecord> readRecords = new ArrayList<>();
        //read the avro blocks and validate the number of records written in each avro block
        while(reader.hasNext()) {
            HoodieAvroDataBlock blk = (HoodieAvroDataBlock) reader.next();
            List<IndexedRecord> records = blk.getRecords();
            readRecords.addAll(records);
            assertEquals("Archived and read records for each block are same", 8, records.size());
            archivedRecordsCount += records.size();
        }
        assertEquals("Total archived records and total read records are the same count", 8, archivedRecordsCount);

        //make sure the archived commits are the same as the (originalcommits - commitsleft)
        List<String> readCommits = readRecords.stream().map(r -> (GenericRecord)r).map(r -> {
            return r.get("commitTime").toString();
        }).collect(Collectors.toList());
        Collections.sort(readCommits);

        assertEquals(
                "Read commits map should match the originalCommits - commitsLoadedFromArchival",
                originalCommits.stream().map(HoodieInstant::getTimestamp).collect(Collectors.toList()),
                readCommits);
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

        HoodieTimeline timeline =
            metadata.getActiveTimeline().getCommitsAndCompactionsTimeline().filterCompletedInstants();

        assertEquals("Loaded 4 commits and the count should match", 4, timeline.countInstants());
        boolean result = archiveLog.archiveIfRequired();
        assertTrue(result);
        timeline =
            metadata.getActiveTimeline().reload().getCommitsAndCompactionsTimeline().filterCompletedInstants();
        assertEquals("Should not archive commits when maxCommitsToKeep is 5", 4,
            timeline.countInstants());
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

        HoodieTimeline timeline =
            metadata.getActiveTimeline().getCommitsAndCompactionsTimeline().filterCompletedInstants();
        assertEquals("Loaded 6 commits and the count should match", 6, timeline.countInstants());
        boolean result = archiveLog.archiveIfRequired();
        assertTrue(result);
        timeline =
            metadata.getActiveTimeline().reload().getCommitsAndCompactionsTimeline().filterCompletedInstants();
        assertTrue("Archived commits should always be safe",
            timeline.containsOrBeforeTimelineStarts("100"));
        assertTrue("Archived commits should always be safe",
            timeline.containsOrBeforeTimelineStarts("101"));
        assertTrue("Archived commits should always be safe",
            timeline.containsOrBeforeTimelineStarts("102"));
        assertTrue("Archived commits should always be safe",
            timeline.containsOrBeforeTimelineStarts("103"));
    }

    @Test
    public void testArchiveCommitSavepointNoHole() throws IOException {
        HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(basePath)
            .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
            .forTable("test-trip-table").withCompactionConfig(
                HoodieCompactionConfig.newBuilder().archiveCommitsWith(2, 5).build()).build();
        HoodieTableMetaClient metadata = new HoodieTableMetaClient(fs, basePath);
        HoodieCommitArchiveLog archiveLog = new HoodieCommitArchiveLog(cfg, fs);
        HoodieTestDataGenerator.createCommitFile(basePath, "100");
        HoodieTestDataGenerator.createCommitFile(basePath, "101");
        HoodieTestDataGenerator.createSavepointFile(basePath, "101");
        HoodieTestDataGenerator.createCommitFile(basePath, "102");
        HoodieTestDataGenerator.createCommitFile(basePath, "103");
        HoodieTestDataGenerator.createCommitFile(basePath, "104");
        HoodieTestDataGenerator.createCommitFile(basePath, "105");

        HoodieTimeline timeline =
            metadata.getActiveTimeline().getCommitsAndCompactionsTimeline().filterCompletedInstants();
        assertEquals("Loaded 6 commits and the count should match", 6, timeline.countInstants());
        boolean result = archiveLog.archiveIfRequired();
        assertTrue(result);
        timeline =
            metadata.getActiveTimeline().reload().getCommitsAndCompactionsTimeline().filterCompletedInstants();
        assertEquals(
            "Since we have a savepoint at 101, we should never archive any commit after 101 (we only archive 100)",
            5, timeline.countInstants());
        assertTrue("Archived commits should always be safe",
            timeline.containsInstant(new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "101")));
        assertTrue("Archived commits should always be safe",
            timeline.containsInstant(new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "102")));
        assertTrue("Archived commits should always be safe",
            timeline.containsInstant(new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "103")));
    }


}
