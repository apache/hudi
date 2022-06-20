/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.meta;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.metadata.CkpMetadata;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;

import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Test cases for {@link CkpMetadata}.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TestCkpMetadata {

  private CkpMetadata metadata;

  @TempDir
  File tempFile;

  private HoodieTableMetaClient metaClient;

  @BeforeEach
  public void beforeEach() throws Exception {
    String basePath = tempFile.getAbsolutePath();
    FileSystem fs = FSUtils.getFs(tempFile.getAbsolutePath(), HadoopConfigurations.getHadoopConf(new Configuration()));

    Configuration conf = TestConfigurations.getDefaultConf(basePath);
    this.metaClient = StreamerUtil.initTableIfNotExists(conf);

    this.metadata = CkpMetadata.getInstance(fs, basePath);
  }

  @Order(1)
  @Test
  void testWriteAndReadMessage() {
    // write and read 5 committed checkpoints
    IntStream.range(0, 3).forEach(i -> startInstant(i + ""));

    assertThat(metadata.lastPendingInstant(), is("2"));
    metadata.commitInstant("2");
    assertThat(metadata.lastPendingInstant(), equalTo(null));

    // test cleaning
    IntStream.range(3, 6).forEach(i -> startInstant(i + ""));
    assertThat(metadata.getMessages().size(), is(3));
    // commit and abort instant does not trigger cleaning
    metadata.commitInstant("6");
    metadata.abortInstant("7");
    assertThat(metadata.getMessages().size(), is(5));
    // test rollback instant
    startInstant("8");
    assertThat(metadata.lastPendingInstant(), is("8"));
    deleteInstant("8");
    assertThat(metadata.lastPendingInstant(), equalTo(null));
  }

  @Order(2)
  @Test
  void testBootstrap() throws Exception {
    IntStream.range(0, 3).forEach(i -> startInstant(i + ""));
    assertThat(this.metadata.getMessages().size(), is(3));
    // just keep the last pending instant
    this.metadata = CkpMetadata.getInstanceAtFirstTime(this.metaClient);
    assertThat(this.metadata.getMessages().size(), is(1));
    assertThat(metadata.lastPendingInstant(), is("2"));
  }

  @Order(3)
  @Test
  void testLatestPendingInstant() {
    // write and read 5 committed checkpoints
    IntStream.range(0, 3).forEach(i -> startInstant(i + ""));

    assertThat(metadata.lastPendingInstant(), is("2"));
    assertThat(metadata.latestPendingInstant(), is("2"));
    metaClient.reloadActiveTimeline().saveAsComplete(
        new HoodieInstant(true, HoodieActiveTimeline.DELTA_COMMIT_ACTION, "2"), Option.empty());
    assertThat(metadata.lastPendingInstant(), is("2"));
    assertThat(metadata.latestPendingInstant(), equalTo(null));
    startInstant("3");
    metadata.commitInstant("3");
    assertThat(metadata.lastPendingInstant(), equalTo(null));
    assertThat(metadata.latestPendingInstant(), equalTo(null));
  }

  private void startInstant(String instant) {
    HoodieActiveTimeline timeline = this.metaClient.getActiveTimeline().reload();
    HoodieInstant requestInstant =
        new HoodieInstant(
            HoodieInstant.State.REQUESTED, HoodieTimeline.DELTA_COMMIT_ACTION, instant);
    timeline.createNewInstant(requestInstant);
    timeline.transitionRequestedToInflight(requestInstant, Option.empty());
    this.metadata.startInstant(instant);
  }

  private void commitInstant(String instant) {
    HoodieActiveTimeline timeline = this.metaClient.getActiveTimeline().reload();
    HoodieInstant inflightInstant =
        new HoodieInstant(
            HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, instant);
    timeline.saveAsComplete(inflightInstant, Option.empty());
    this.metadata.commitInstant(instant);
  }

  private void deleteInstant(String instant) {
    HoodieActiveTimeline timeline = this.metaClient.getActiveTimeline().reload();
    HoodieInstant inflightInstant =
        new HoodieInstant(
            HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, instant);
    if (timeline.containsInstant(inflightInstant)) {
      timeline.deletePending(inflightInstant);
    }
    timeline.deletePending(
        new HoodieInstant(
            HoodieInstant.State.REQUESTED, HoodieTimeline.DELTA_COMMIT_ACTION, instant));
    this.metadata.deleteInstant(instant);
  }
}
