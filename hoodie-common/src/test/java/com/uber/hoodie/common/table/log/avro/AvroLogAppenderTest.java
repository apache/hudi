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

package com.uber.hoodie.common.table.log.avro;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.uber.hoodie.common.minicluster.MiniClusterUtil;
import com.uber.hoodie.common.table.log.HoodieLogAppendConfig;
import com.uber.hoodie.common.table.log.HoodieLogFile;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.common.util.SchemaTestUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.commons.collections.IteratorUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AvroLogAppenderTest {
    private FileSystem fs;
    private Path partitionPath;

    @BeforeClass
    public static void setUpClass() throws IOException, InterruptedException {
        // Append is not supported in LocalFileSystem. HDFS needs to be setup.
        MiniClusterUtil.setUp();
    }

    @AfterClass
    public static void tearDownClass() {
        MiniClusterUtil.shutdown();
    }

    @Before
    public void setUp() throws IOException, InterruptedException {
        this.fs = MiniClusterUtil.fileSystem;
        TemporaryFolder folder = new TemporaryFolder();
        folder.create();
        assertTrue(fs.mkdirs(new Path(folder.getRoot().getPath())));
        this.partitionPath = new Path(folder.getRoot().getPath());
    }

    @After
    public void tearDown() throws IOException {
        fs.delete(partitionPath, true);
    }

    @Test
    public void testBasicAppend() throws IOException, URISyntaxException, InterruptedException {
        HoodieLogAppendConfig logConfig =
            HoodieLogAppendConfig.newBuilder().onPartitionPath(partitionPath)
                .withLogFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId("test-fileid1")
                .withBaseCommitTime("100")
                .withSchema(SchemaTestUtil.getSimpleSchema()).withFs(fs).build();
        RollingAvroLogAppender logAppender = new RollingAvroLogAppender(logConfig);
        logAppender.append(SchemaTestUtil.generateTestRecords(0, 100));
        long size1 = logAppender.getCurrentSize();
        assertTrue("", size1 > 0);
        assertEquals("", size1, fs.getFileStatus(logConfig.getLogFile().getPath()).getLen());
        logAppender.close();

        // Close and Open again and append 100 more records
        logAppender = new RollingAvroLogAppender(logConfig);
        logAppender.append(SchemaTestUtil.generateTestRecords(100, 100));
        long size2 = logAppender.getCurrentSize();
        assertTrue("", size2 > size1);
        assertEquals("", size2, fs.getFileStatus(logConfig.getLogFile().getPath()).getLen());
        logAppender.close();

        // Close and Open again and append 100 more records
        logAppender = new RollingAvroLogAppender(logConfig);
        logAppender.append(SchemaTestUtil.generateTestRecords(200, 100));
        long size3 = logAppender.getCurrentSize();
        assertTrue("", size3 > size2);
        assertEquals("", size3, fs.getFileStatus(logConfig.getLogFile().getPath()).getLen());
        logAppender.close();
        // Cannot get the current size after closing the log
        try {
            logAppender.getCurrentSize();
            fail("getCurrentSize should fail after the logAppender is closed");
        } catch (IllegalStateException e) {
            // pass
        }
    }

    @Test
    public void testLeaseRecovery() throws IOException, URISyntaxException, InterruptedException {
        HoodieLogAppendConfig logConfig =
            HoodieLogAppendConfig.newBuilder().onPartitionPath(partitionPath)
                .withLogFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId("test-fileid1")
                .withBaseCommitTime("100")
                .withSchema(SchemaTestUtil.getSimpleSchema()).withFs(fs).build();
        RollingAvroLogAppender logAppender = new RollingAvroLogAppender(logConfig);
        logAppender.append(SchemaTestUtil.generateTestRecords(0, 100));
        // do not close this log appender
        // logAppender.close();

        // Try opening again and append 100 more records
        logAppender = new RollingAvroLogAppender(logConfig);
        logAppender.append(SchemaTestUtil.generateTestRecords(100, 100));
        assertEquals("", logAppender.getCurrentSize(),
            fs.getFileStatus(logConfig.getLogFile().getPath()).getLen());
        logAppender.close();
    }

    @Test
    public void testAppendOnCorruptedBlock()
        throws IOException, URISyntaxException, InterruptedException {
        HoodieLogAppendConfig logConfig =
            HoodieLogAppendConfig.newBuilder().onPartitionPath(partitionPath)
                .withLogFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId("test-fileid1")
                .withBaseCommitTime("100")
                .withSchema(SchemaTestUtil.getSimpleSchema()).withFs(fs).build();
        RollingAvroLogAppender logAppender = new RollingAvroLogAppender(logConfig);
        logAppender.append(SchemaTestUtil.generateTestRecords(0, 100));
        logAppender.close();

        // Append some arbit byte[] to thee end of the log (mimics a partially written commit)
        assertTrue(fs.exists(logConfig.getLogFile().getPath()));
        fs = FileSystem.get(fs.getConf());
        FSDataOutputStream outputStream =
            fs.append(logConfig.getLogFile().getPath(), logConfig.getBufferSize());
        outputStream.write("something-random".getBytes());
        outputStream.flush();
        outputStream.close();

        logAppender = new RollingAvroLogAppender(logConfig);
        logAppender.append(SchemaTestUtil.generateTestRecords(100, 100));
        logAppender.close();
    }


    @SuppressWarnings("unchecked")
    @Test
    public void testBasicWriteAndRead()
        throws IOException, URISyntaxException, InterruptedException {
        HoodieLogAppendConfig logConfig =
            HoodieLogAppendConfig.newBuilder().onPartitionPath(partitionPath)
                .withLogFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId("test-fileid1")
                .withBaseCommitTime("100")
                .withSchema(SchemaTestUtil.getSimpleSchema()).withFs(fs).build();
        RollingAvroLogAppender logAppender = new RollingAvroLogAppender(logConfig);
        long size1 = logAppender.getCurrentSize();

        List<IndexedRecord> inputRecords = SchemaTestUtil.generateTestRecords(0, 100);
        logAppender.append(inputRecords);
        logAppender.close();

        AvroLogReader logReader =
            new AvroLogReader(logConfig.getLogFile(), fs, logConfig.getSchema());
        List<GenericRecord> result = IteratorUtils.toList(logReader.readBlock(size1));
        assertEquals("Random access should return 100 records", 100, result.size());
        assertEquals("both lists should be the same. (ordering guaranteed)", inputRecords, result);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testBasicAppendAndRead()
        throws IOException, URISyntaxException, InterruptedException {
        HoodieLogAppendConfig logConfig =
            HoodieLogAppendConfig.newBuilder().onPartitionPath(partitionPath)
                .withLogFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId("test-fileid1")
                .withBaseCommitTime("100")
                .withSchema(SchemaTestUtil.getSimpleSchema()).withFs(fs).build();
        RollingAvroLogAppender logAppender = new RollingAvroLogAppender(logConfig);
        logAppender.append(SchemaTestUtil.generateTestRecords(0, 100));
        long size1 = logAppender.getCurrentSize();
        logAppender.close();

        // Close and Open again and append 100 more records
        logAppender = new RollingAvroLogAppender(logConfig);
        List<IndexedRecord> secondBatchInput = SchemaTestUtil.generateTestRecords(100, 100);
        logAppender.append(secondBatchInput);
        long size2 = logAppender.getCurrentSize();
        logAppender.close();

        // Close and Open again and append 100 more records
        logAppender = new RollingAvroLogAppender(logConfig);
        List<IndexedRecord> lastBatchInput = SchemaTestUtil.generateTestRecords(200, 100);
        logAppender.append(lastBatchInput);
        long size3 = logAppender.getCurrentSize();
        logAppender.close();

        AvroLogReader logReader =
            new AvroLogReader(logConfig.getLogFile(), fs, logConfig.getSchema());

        // Try to grab the middle block here
        List<GenericRecord> secondBatch = IteratorUtils.toList(logReader.readBlock(size1));
        assertEquals("Stream collect should return 100 records", 100, secondBatch.size());
        assertEquals("Collected list should match the input list (ordering guaranteed)",
            secondBatchInput, secondBatch);

        // Try to grab the middle block here
        List<GenericRecord> lastBatch = IteratorUtils.toList(logReader.readBlock(size2));
        assertEquals("Stream collect should return 100 records", 100, secondBatch.size());
        assertEquals("Collected list should match the input list (ordering guaranteed)",
            lastBatchInput, lastBatch);

        List<GenericRecord> imaginaryBatch = IteratorUtils.toList(logReader.readBlock(size3));
        assertEquals("Stream collect should return 0 records", 0, imaginaryBatch.size());
    }

    @Test
    public void testAppendAndReadOnCorruptedLog()
        throws IOException, URISyntaxException, InterruptedException {
        HoodieLogAppendConfig logConfig =
            HoodieLogAppendConfig.newBuilder().onPartitionPath(partitionPath)
                .withLogFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId("test-fileid1")
                .withBaseCommitTime("100")
                .withSchema(SchemaTestUtil.getSimpleSchema()).withFs(fs).build();
        RollingAvroLogAppender logAppender = new RollingAvroLogAppender(logConfig);
        long size1 = logAppender.getCurrentSize();
        logAppender.append(SchemaTestUtil.generateTestRecords(0, 100));
        logAppender.close();

        // Append some arbit byte[] to thee end of the log (mimics a partially written commit)
        assertTrue(fs.exists(logConfig.getLogFile().getPath()));
        fs = FileSystem.get(fs.getConf());
        FSDataOutputStream outputStream =
            fs.append(logConfig.getLogFile().getPath(), logConfig.getBufferSize());
        outputStream.write("something-random".getBytes());
        outputStream.flush();
        outputStream.close();

        logAppender = new RollingAvroLogAppender(logConfig);
        long size2 = logAppender.getCurrentSize();
        logAppender.append(SchemaTestUtil.generateTestRecords(100, 100));
        logAppender.close();

        AvroLogReader logReader =
            new AvroLogReader(logConfig.getLogFile(), fs, logConfig.getSchema());

        // Try to grab the middle block here
        List<GenericRecord> secondBatch = IteratorUtils.toList(logReader.readBlock(size1));
        assertEquals("Stream collect should return 100 records", 100, secondBatch.size());

        // Try to grab the last block here
        List<GenericRecord> lastBatch = IteratorUtils.toList(logReader.readBlock(size2));
        assertEquals("Stream collect should return 100 records", 100, lastBatch.size());
    }

    @Test
    public void testCompositeAvroLogReader()
        throws IOException, URISyntaxException, InterruptedException {
        // Set a small threshold so that every block is a new version
        HoodieLogAppendConfig logConfig =
            HoodieLogAppendConfig.newBuilder().onPartitionPath(partitionPath)
                .withLogFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId("test-fileid1")
                .withBaseCommitTime("100")
                .withSchema(SchemaTestUtil.getSimpleSchema()).withSizeThreshold(500).withFs(fs)
                .build();

        RollingAvroLogAppender logAppender = new RollingAvroLogAppender(logConfig);
        long size1 = logAppender.getCurrentSize();
        List<IndexedRecord> input1 = SchemaTestUtil.generateTestRecords(0, 100);
        logAppender.append(input1);
        logAppender.close();

        // Need to rebuild config to set the latest version as path
        logConfig = HoodieLogAppendConfig.newBuilder().onPartitionPath(partitionPath)
            .withLogFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId("test-fileid1")
            .withBaseCommitTime("100")
            .withSchema(SchemaTestUtil.getSimpleSchema()).withSizeThreshold(500).withFs(fs).build();
        logAppender = new RollingAvroLogAppender(logConfig);
        long size2 = logAppender.getCurrentSize();
        List<IndexedRecord> input2 = SchemaTestUtil.generateTestRecords(100, 100);
        logAppender.append(input2);
        logAppender.close();

        logConfig = HoodieLogAppendConfig.newBuilder().onPartitionPath(partitionPath)
            .withLogFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId("test-fileid1")
            .withBaseCommitTime("100")
            .withSchema(SchemaTestUtil.getSimpleSchema()).withSizeThreshold(500).withFs(fs).build();
        List<HoodieLogFile> allLogFiles = FSUtils
            .getAllLogFiles(fs, partitionPath, logConfig.getLogFile().getFileId(),
                HoodieLogFile.DELTA_EXTENSION, logConfig.getLogFile().getBaseCommitTime())
            .collect(Collectors.toList());
        assertEquals("", 2, allLogFiles.size());

        SortedMap<Integer, List<Long>> offsets = Maps.newTreeMap();
        offsets.put(1, Lists.newArrayList(size1));
        offsets.put(2, Lists.newArrayList(size2));
        CompositeAvroLogReader reader =
            new CompositeAvroLogReader(partitionPath, logConfig.getLogFile().getFileId(),
                logConfig.getLogFile().getBaseCommitTime(), fs, logConfig.getSchema(),
                HoodieLogFile.DELTA_EXTENSION);
        Iterator<GenericRecord> results = reader.readBlocks(offsets);
        List<GenericRecord> totalBatch = IteratorUtils.toList(results);
        assertEquals("Stream collect should return all 200 records", 200, totalBatch.size());
        input1.addAll(input2);
        assertEquals("CompositeAvroLogReader should return 200 records from 2 versions", input1,
            totalBatch);
    }
}
