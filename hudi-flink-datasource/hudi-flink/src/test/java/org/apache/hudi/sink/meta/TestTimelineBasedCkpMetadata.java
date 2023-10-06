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

import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Test cases for {@link CkpMetadata} when timeline based enabled.
 */
public class TestTimelineBasedCkpMetadata extends TestCkpMetadata {

  @Override
  public void setup() throws IOException {
    String basePath = tempFile.getAbsolutePath();
    this.conf = TestConfigurations.getDefaultConf(basePath);
    conf.setString(HoodieWriteConfig.INSTANT_STATE_TIMELINE_SERVER_BASED.key(), "true");
    StreamerUtil.initTableIfNotExists(conf);
    this.writeClient = FlinkWriteClients.createWriteClient(conf);
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "1"})
  public void testFailOver(String uniqueId) {
    CkpMetadata metadata = getCkpMetadata(uniqueId);
    // write and read 5 committed checkpoints
    IntStream.range(0, 3).forEach(i -> metadata.startInstant(i + ""));

    assertThat(metadata.lastPendingInstant(), is("2"));
    metadata.commitInstant("2");
    assertThat(metadata.lastPendingInstant(), equalTo(null));

    // Close write client and timeline server
    cleanup();

    // When timeline server is not responding, we can still read ckp metadata from file system directly
    // test cleaning
    IntStream.range(3, 6).forEach(i -> metadata.startInstant(i + ""));
    assertThat(metadata.getMessages().size(), is(3));
    // commit and abort instant does not trigger cleaning
    metadata.commitInstant("6");
    metadata.abortInstant("7");
    assertThat(metadata.getMessages().size(), is(5));
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "1"})
  public void testRefreshEveryNCommits(String uniqueId) {
    // File system based writing
    writeClient.getConfig().setValue(HoodieWriteConfig.INSTANT_STATE_TIMELINE_SERVER_BASED.key(), "false");
    CkpMetadata writeMetadata = getCkpMetadata(uniqueId);

    // Timeline-based reading
    writeClient.getConfig().setValue(HoodieWriteConfig.INSTANT_STATE_TIMELINE_SERVER_BASED.key(), "true");
    CkpMetadata readOnlyMetadata = getCkpMetadata(uniqueId);

    // write and read 5 committed checkpoints
    IntStream.range(0, 3).forEach(i -> writeMetadata.startInstant(i + ""));
    assertThat(readOnlyMetadata.lastPendingInstant(), is("2"));
    writeMetadata.commitInstant("2");
    // Send 10 requests to server
    readCkpMessagesNTimes(readOnlyMetadata, 10);
    assertThat(readOnlyMetadata.lastPendingInstant(), equalTo("2"));
    // Send 100 requests to server, will trigger refresh
    readCkpMessagesNTimes(readOnlyMetadata, 100);
    assertThat(readOnlyMetadata.lastPendingInstant(), equalTo(null));

    IntStream.range(3, 6).forEach(i -> writeMetadata.startInstant(i + ""));
    readCkpMessagesNTimes(readOnlyMetadata, 200);
    assertThat(readOnlyMetadata.getMessages().size(), is(3));

    writeMetadata.commitInstant("6");
    writeMetadata.abortInstant("7");
    readCkpMessagesNTimes(readOnlyMetadata, 200);
    assertThat(readOnlyMetadata.getMessages().size(), is(5));
  }

  /**
   * Send read requests to server to trigger checkpoint metadata refreshing.
   */
  private void readCkpMessagesNTimes(CkpMetadata metadata, int maxRetry) {
    int retry = 0;
    while (retry < maxRetry) {
      metadata.getMessages();
      retry++;
    }
  }

}
