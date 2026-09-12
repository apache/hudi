/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.testutils;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.storage.StoragePath;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers the path contract of {@link SparkClientFunctionalTestHarness} (HUDI-6042).
 */
public class TestSparkClientFunctionalTestHarness extends SparkClientFunctionalTestHarness {

  @Test
  public void basePathHasNoSchemeAndBaseUriDoes() {
    assertFalse(basePath().startsWith("file:"),
        "basePath() must be unqualified so java.nio.file.Paths can consume it, but was " + basePath());
    assertTrue(Paths.get(basePath()).isAbsolute(),
        "basePath() must resolve to an absolute path, but was " + basePath());
    assertEquals("file", baseUri().getScheme(), "baseUri() must keep the scheme");
    assertEquals(basePath(), Paths.get(baseUri()).toString(),
        "baseUri() and basePath() must point at the same directory");
  }

  @Test
  public void nioResolvesPartitionsUnderTheTableDirectory() {
    // A scheme-qualified basePath() silently yields a relative path here, placing partitions under
    // the working directory rather than the table. This is the failure mode FileCreateUtils hits.
    java.nio.file.Path partition = Paths.get(basePath(), "2016/03/15");
    assertTrue(partition.isAbsolute(), "partition path must be absolute, but was " + partition);
    assertTrue(partition.startsWith(Paths.get(basePath())),
        "partition path must sit under the table directory, but was " + partition);
  }

  @Test
  public void hoodieTestTableAcceptsBasePath() throws Exception {
    HoodieTableMetaClient metaClient = getHoodieMetaClient(HoodieTableType.COPY_ON_WRITE);
    // HoodieTestTable.of asserts basePath equals metaClient's base path verbatim, so any scheme
    // mismatch between the two fails here.
    HoodieTestTable testTable = HoodieTestTable.of(metaClient).withPartitionMetaFiles("2016/03/15");
    testTable.addCommit("001");

    assertTrue(Files.exists(Paths.get(basePath(), "2016/03/15", ".hoodie_partition_metadata")),
        "partition metadata must be written under the table directory");
  }

  @Test
  public void storageResolvesAgainstTheTableDirectory() throws IOException {
    assertTrue(hoodieStorage().exists(new StoragePath(basePath())),
        "the table directory must be reachable through the harness storage");
  }
}
