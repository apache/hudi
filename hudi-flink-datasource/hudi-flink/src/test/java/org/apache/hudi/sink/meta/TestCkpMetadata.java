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

import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Test cases for {@link CkpMetadata}.
 */
public class TestCkpMetadata {

  @TempDir
  File tempFile;

  protected Configuration conf;

  protected HoodieFlinkWriteClient writeClient;

  @BeforeEach
  public void beforeEach() throws Exception {
    setup();
  }

  protected void setup() throws IOException {
    String basePath = tempFile.getAbsolutePath();
    this.conf = TestConfigurations.getDefaultConf(basePath);
    StreamerUtil.initTableIfNotExists(conf);
    this.writeClient = FlinkWriteClients.createWriteClient(conf);
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "1"})
  void testWriteAndReadMessage(String uniqueId) {
    CkpMetadata metadata = getCkpMetadata(uniqueId);
    // write and read 5 committed checkpoints
    IntStream.range(0, 3).forEach(i -> metadata.startInstant(i + ""));

    assertThat(metadata.lastPendingInstant(), is("2"));
    metadata.commitInstant("2");
    assertThat(metadata.lastPendingInstant(), equalTo(null));

    // test cleaning
    IntStream.range(3, 6).forEach(i -> metadata.startInstant(i + ""));
    assertThat(metadata.getMessages().size(), is(3));
    // commit and abort instant does not trigger cleaning
    metadata.commitInstant("6");
    metadata.abortInstant("7");
    assertThat(metadata.getMessages().size(), is(5));
  }

  @Test
  void testBootstrap() throws Exception {
    CkpMetadata metadata = getCkpMetadata("");
    // write 4 instants to the ckp_meta
    IntStream.range(0, 4).forEach(i -> metadata.startInstant(i + ""));
    assertThat("The first instant should be removed from the instant cache",
        metadata.getInstantCache(), is(Arrays.asList("1", "2", "3")));

    // simulate the reboot of coordinator
    CkpMetadata metadata1 = getCkpMetadata("");
    metadata1.bootstrap();
    assertNull(metadata1.getInstantCache(), "The instant cache should be recovered from bootstrap");

    metadata1.startInstant("4");
    assertThat("The first instant should be removed from the instant cache",
        metadata1.getInstantCache(), is(Collections.singletonList("4")));
  }

  protected CkpMetadata getCkpMetadata(String uniqueId) {
    conf.set(FlinkOptions.WRITE_CLIENT_ID, uniqueId);
    return CkpMetadataFactory.getCkpMetadata(writeClient.getConfig(), conf);
  }

  @AfterEach
  public void cleanup() {
    if (writeClient != null) {
      writeClient.close();
      writeClient = null;
    }
  }
}
