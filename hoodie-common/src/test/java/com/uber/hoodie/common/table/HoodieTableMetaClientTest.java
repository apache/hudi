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

package com.uber.hoodie.common.table;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Lists;
import com.uber.hoodie.common.model.HoodieTestUtils;
import com.uber.hoodie.common.table.timeline.HoodieArchivedCommitTimeline;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayFile;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class HoodieTableMetaClientTest {
    private HoodieTableMetaClient metaClient;
    private String basePath;

    @Before
    public void init() throws IOException {
        TemporaryFolder folder = new TemporaryFolder();
        folder.create();
        this.basePath = folder.getRoot().getAbsolutePath();
        metaClient = HoodieTestUtils.init(basePath);
    }

    @Test
    public void checkMetadata() {
        assertEquals("Table name should be raw_trips", HoodieTestUtils.RAW_TRIPS_TEST_NAME,
            metaClient.getTableConfig().getTableName());
        assertEquals("Basepath should be the one assigned", basePath, metaClient.getBasePath());
        assertEquals("Metapath should be ${basepath}/.hoodie", basePath + "/.hoodie",
            metaClient.getMetaPath());
    }

    @Test
    public void checkSerDe() throws IOException {
        // check if this object is serialized and se-serialized, we are able to read from the file system
        HoodieTableMetaClient deseralizedMetaClient =
            HoodieTestUtils.serializeDeserialize(metaClient, HoodieTableMetaClient.class);
        HoodieTimeline commitTimeline = metaClient.getActiveCommitTimeline();
        commitTimeline.saveInstantAsInflight("1");
        commitTimeline.saveInstantAsComplete("1", Optional.of("test-detail".getBytes()));
        commitTimeline = commitTimeline.reload();
        assertEquals("Commit should be 1", "1", commitTimeline.getInstants().findFirst().get());
        assertArrayEquals("Commit value should be \"test-detail\"", "test-detail".getBytes(),
            commitTimeline.readInstantDetails("1").get());
    }

    @Test
    public void checkCommitTimeline() throws IOException {
        HoodieTimeline commitTimeline = metaClient.getActiveCommitTimeline();
        assertFalse("Should be empty commit timeline",
            commitTimeline.getInstants().findFirst().isPresent());
        assertFalse("Should be empty commit timeline",
            commitTimeline.getInflightInstants().findFirst().isPresent());
        commitTimeline.saveInstantAsInflight("1");
        commitTimeline.saveInstantAsComplete("1", Optional.of("test-detail".getBytes()));

        // Commit timeline should not auto-reload every time getActiveCommitTimeline(), it should be cached
        commitTimeline = metaClient.getActiveCommitTimeline();
        assertFalse("Should be empty commit timeline",
            commitTimeline.getInstants().findFirst().isPresent());
        assertFalse("Should be empty commit timeline",
            commitTimeline.getInflightInstants().findFirst().isPresent());

        commitTimeline = commitTimeline.reload();
        assertTrue("Should be the 1 commit we made",
            commitTimeline.getInstants().findFirst().isPresent());
        assertEquals("Commit should be 1", "1", commitTimeline.getInstants().findFirst().get());
        assertArrayEquals("Commit value should be \"test-detail\"", "test-detail".getBytes(),
            commitTimeline.readInstantDetails("1").get());
    }

    @Test
    public void checkArchiveCommitTimeline() throws IOException {
        Path archiveLogPath =
            HoodieArchivedCommitTimeline.getArchiveLogPath(metaClient.getMetaPath());
        SequenceFile.Writer writer = SequenceFile
            .createWriter(HoodieTestUtils.fs.getConf(), SequenceFile.Writer.file(archiveLogPath),
                SequenceFile.Writer.keyClass(Text.class),
                SequenceFile.Writer.valueClass(Text.class));

        writer.append(new Text("1"), new Text("data1"));
        writer.append(new Text("2"), new Text("data2"));
        writer.append(new Text("3"), new Text("data3"));

        IOUtils.closeStream(writer);

        HoodieTimeline archivedTimeline = metaClient.getArchivedCommitTimeline();
        assertEquals(Lists.newArrayList("1", "2", "3"),
            archivedTimeline.getInstants().collect(Collectors.toList()));
        System.out.println(new String( archivedTimeline.readInstantDetails("1").get()));
        assertArrayEquals(new Text("data1").getBytes(), archivedTimeline.readInstantDetails("1").get());
        assertArrayEquals(new Text("data2").getBytes(), archivedTimeline.readInstantDetails("2").get());
        assertArrayEquals(new Text("data3").getBytes(), archivedTimeline.readInstantDetails("3").get());
    }



}
