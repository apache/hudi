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

package org.apache.hudi.sink.message;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;

import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link MessageBus}.
 */
public class TestMessageBus {

  private String basePath;
  private FileSystem fs;

  private MessageDriver driver;

  @TempDir
  File tempFile;

  @BeforeEach
  public void beforeEach() throws Exception {
    basePath = tempFile.getAbsolutePath();
    this.fs = FSUtils.getFs(tempFile.getAbsolutePath(), StreamerUtil.getHadoopConf());

    Configuration conf = TestConfigurations.getDefaultConf(basePath);
    StreamerUtil.initTableIfNotExists(conf);

    this.driver = MessageDriver.getInstance(fs, basePath);
  }

  @Test
  void testWriteAndReadMessage() {
    MessageClient client = MessageClient.getSingleton(fs, basePath);

    // write and read 5 committed checkpoints
    IntStream.range(0, 5).forEach(i -> driver.commitCkp(i, i + "", i + 1 + ""));

    IntStream.range(0, 5).forEach(i -> {
      Option<MessageBus.CkpMessage> messageOpt = client.getCkpMessage(i);
      assertTrue(messageOpt.isPresent());

      MessageBus.CkpMessage ckpMessage = messageOpt.get();
      assertTrue(ckpMessage.committed);
      assertThat(ckpMessage.commitInstant, is(i + ""));
      assertThat(ckpMessage.inflightInstant, is(i + 1 + ""));
    });

    // write and read 5 aborted checkpoints
    IntStream.range(5, 10).forEach(i -> driver.abortCkp(i));

    IntStream.range(5, 10).forEach(i -> {
      Option<MessageBus.CkpMessage> messageOpt = client.getCkpMessage(i);
      assertTrue(messageOpt.isPresent());

      MessageBus.CkpMessage ckpMessage = messageOpt.get();
      assertFalse(ckpMessage.committed);
      assertThat(ckpMessage.commitInstant, is(MessageBus.ABORTED_CKP_INSTANT));
      assertThat(ckpMessage.inflightInstant, is(MessageBus.ABORTED_CKP_INSTANT));
    });
  }

  @Test
  void testWriteCleaning() {
    // write and read 20 committed checkpoints
    IntStream.range(0, 20).forEach(i -> driver.commitCkp(i, i + "", i + 1 + ""));
    assertThat("The id cache should not be cleaned", driver.getCkpIdCache().size(), is(20));

    // write and read 10 aborted checkpoints
    IntStream.range(20, 29).forEach(i -> driver.abortCkp(i));
    assertThat("The id cache should not be cleaned", driver.getCkpIdCache().size(), is(29));

    driver.commitCkp(29, "29", "30");
    assertThat("The cache should be cleaned", driver.getCkpIdCache().size(), is(20));
    assertThat(longSet2String(driver.getCkpIdCache().keySet()),
        is("10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29"));
  }

  @Test
  void testReadCleaning() {
    MessageClient client = MessageClient.getSingleton(fs, basePath);

    // write and read 20 committed checkpoints
    IntStream.range(0, 20).forEach(i -> driver.commitCkp(i, i + "", i + 1 + ""));

    IntStream.range(0, 10).forEach(client::getCkpMessage);
    assertThat("The checkpoint cache should not be cleaned", client.getCkpCache().size(), is(10));

    client.getCkpMessage(10);
    assertThat("The checkpoint cache should be cleaned", client.getCkpCache().size(), is(10));

    IntStream.range(11, 15).forEach(client::getCkpMessage);
    assertThat("The checkpoint cache should be cleaned", client.getCkpCache().size(), is(10));
    assertThat(longSet2String(client.getCkpCache().keySet()), is("5,6,7,8,9,10,11,12,13,14"));
  }

  private static String longSet2String(Set<Long> longSet) {
    List<String> elements = new ArrayList<>();
    longSet.stream().mapToInt(Long::intValue).forEach(i -> elements.add(i + ""));
    return String.join(",", elements);
  }
}
