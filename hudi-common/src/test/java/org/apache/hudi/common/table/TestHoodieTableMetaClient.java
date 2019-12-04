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

package org.apache.hudi.common.table;

import org.apache.hudi.common.HoodieCommonTestHarness;
import org.apache.hudi.common.model.HoodieTestUtils;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.stream.Collectors;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests hoodie table meta client {@link HoodieTableMetaClient}.
 */
public class TestHoodieTableMetaClient extends HoodieCommonTestHarness {

  @Before
  public void init() throws IOException {
    initMetaClient();
  }

  @Test
  public void checkMetadata() {
    assertEquals("Table name should be raw_trips", HoodieTestUtils.RAW_TRIPS_TEST_NAME,
        metaClient.getTableConfig().getTableName());
    assertEquals("Basepath should be the one assigned", basePath, metaClient.getBasePath());
    assertEquals("Metapath should be ${basepath}/.hoodie", basePath + "/.hoodie", metaClient.getMetaPath());
  }

  @Test
  public void checkSerDe() throws IOException, ClassNotFoundException {
    // check if this object is serialized and de-serialized, we are able to read from the file system
    HoodieTableMetaClient deseralizedMetaClient =
        HoodieTestUtils.serializeDeserialize(metaClient, HoodieTableMetaClient.class);
    assertNotNull(deseralizedMetaClient);
    HoodieActiveTimeline commitTimeline = deseralizedMetaClient.getActiveTimeline();
    HoodieInstant instant = new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, "1");
    commitTimeline.createInflight(instant);
    commitTimeline.saveAsComplete(instant, Option.of("test-detail".getBytes()));
    commitTimeline = commitTimeline.reload();
    HoodieInstant completedInstant = HoodieTimeline.getCompletedInstant(instant);
    assertEquals("Commit should be 1 and completed", completedInstant, commitTimeline.getInstants().findFirst().get());
    assertArrayEquals("Commit value should be \"test-detail\"", "test-detail".getBytes(),
        commitTimeline.getInstantDetails(completedInstant).get());
  }

  @Test
  public void checkCommitTimeline() throws IOException {
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
    HoodieTimeline activeCommitTimeline = activeTimeline.getCommitTimeline();
    assertTrue("Should be empty commit timeline", activeCommitTimeline.empty());

    HoodieInstant instant = new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, "1");
    activeTimeline.createInflight(instant);
    activeTimeline.saveAsComplete(instant, Option.of("test-detail".getBytes()));

    // Commit timeline should not auto-reload every time getActiveCommitTimeline(), it should be cached
    activeTimeline = metaClient.getActiveTimeline();
    activeCommitTimeline = activeTimeline.getCommitTimeline();
    assertTrue("Should be empty commit timeline", activeCommitTimeline.empty());

    HoodieInstant completedInstant = HoodieTimeline.getCompletedInstant(instant);
    activeTimeline = activeTimeline.reload();
    activeCommitTimeline = activeTimeline.getCommitTimeline();
    assertFalse("Should be the 1 commit we made", activeCommitTimeline.empty());
    assertEquals("Commit should be 1", completedInstant, activeCommitTimeline.getInstants().findFirst().get());
    assertArrayEquals("Commit value should be \"test-detail\"", "test-detail".getBytes(),
        activeCommitTimeline.getInstantDetails(completedInstant).get());
  }

  @Test
  public void checkArchiveCommitTimeline() throws IOException {
    Path archiveLogPath = HoodieArchivedTimeline.getArchiveLogPath(metaClient.getArchivePath());
    SequenceFile.Writer writer =
        SequenceFile.createWriter(metaClient.getHadoopConf(), SequenceFile.Writer.file(archiveLogPath),
            SequenceFile.Writer.keyClass(Text.class), SequenceFile.Writer.valueClass(Text.class));

    writer.append(new Text("1"), new Text("data1"));
    writer.append(new Text("2"), new Text("data2"));
    writer.append(new Text("3"), new Text("data3"));

    IOUtils.closeStream(writer);

    HoodieArchivedTimeline archivedTimeline = metaClient.getArchivedTimeline();

    HoodieInstant instant1 = new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "1");
    HoodieInstant instant2 = new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "2");
    HoodieInstant instant3 = new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "3");

    assertEquals(Lists.newArrayList(instant1, instant2, instant3),
        archivedTimeline.getInstants().collect(Collectors.toList()));

    assertArrayEquals(new Text("data1").getBytes(), archivedTimeline.getInstantDetails(instant1).get());
    assertArrayEquals(new Text("data2").getBytes(), archivedTimeline.getInstantDetails(instant2).get());
    assertArrayEquals(new Text("data3").getBytes(), archivedTimeline.getInstantDetails(instant3).get());
  }

}
