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
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;

import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Test cases for {@link CkpMetadata}.
 */
public class TestCkpMetadata {

  private CkpMetadata metadata;

  @TempDir
  File tempFile;

  @BeforeEach
  public void beforeEach() throws Exception {
    String basePath = tempFile.getAbsolutePath();
    FileSystem fs = FSUtils.getFs(tempFile.getAbsolutePath(), HadoopConfigurations.getHadoopConf(new Configuration()));

    Configuration conf = TestConfigurations.getDefaultConf(basePath);
    StreamerUtil.initTableIfNotExists(conf);

    this.metadata = CkpMetadata.getInstance(fs, basePath);
  }

  @Test
  void testWriteAndReadMessage() {
    // write and read 5 committed checkpoints
    IntStream.range(0, 3).forEach(i -> metadata.startInstant(i + ""));

    assertThat(metadata.lastPendingInstant(), is("2"));
    metadata.commitInstant("2");
    assertThat(metadata.lastPendingInstant(), is("1"));

    // test cleaning
    IntStream.range(3, 6).forEach(i -> metadata.startInstant(i + ""));
    assertThat(metadata.getMessages().size(), is(3));
    // commit and abort instant does not trigger cleaning
    metadata.commitInstant("6");
    metadata.abortInstant("7");
    assertThat(metadata.getMessages().size(), is(5));
  }
}
