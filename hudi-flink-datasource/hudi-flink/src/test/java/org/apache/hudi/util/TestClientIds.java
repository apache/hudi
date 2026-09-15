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

package org.apache.hudi.util;

import org.apache.hudi.configuration.FlinkOptions;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link ClientIds}.
 */
public class TestClientIds {

  @TempDir
  Path tempDir;

  @Test
  public void testNextIdSortsNumericallyAcrossDoubleDigitIds() throws IOException {
    createHeartbeatFiles("", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10");
    Configuration conf = confForBasePath();

    String nextId = ClientIds.builder().conf(conf).build().nextId(conf);

    assertEquals("11", nextId);
  }

  @Test
  public void testNextIdSingleDigitIdsStillWork() throws IOException {
    createHeartbeatFiles("", "1", "2");
    Configuration conf = confForBasePath();

    String nextId = ClientIds.builder().conf(conf).build().nextId(conf);

    assertEquals("3", nextId);
  }

  private void createHeartbeatFiles(String... clientIds) throws IOException {
    Path heartbeatDir = tempDir.resolve(".hoodie").resolve(".aux").resolve(".ids");
    Files.createDirectories(heartbeatDir);
    for (String clientId : clientIds) {
      Files.createFile(heartbeatDir.resolve("_" + clientId));
    }
  }

  private Configuration confForBasePath() {
    String uri = tempDir.toUri().toString();
    String basePath = uri.endsWith("/") ? uri.substring(0, uri.length() - 1) : uri;
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.PATH, basePath);
    return conf;
  }
}