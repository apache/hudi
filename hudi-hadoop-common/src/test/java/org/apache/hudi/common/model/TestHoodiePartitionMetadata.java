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

package org.apache.hudi.common.model;

import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link HoodiePartitionMetadata}.
 */
public class TestHoodiePartitionMetadata extends HoodieCommonTestHarness {

  HoodieStorage storage;

  @BeforeEach
  public void setupTest() throws IOException {
    initMetaClient();
    storage = metaClient.getStorage();
  }

  @AfterEach
  public void tearDown() throws Exception {
    storage.close();
    cleanMetaClient();
  }

  static Stream<Arguments> formatProviderFn() {
    return Stream.of(
        Arguments.arguments(Option.empty()),
        Arguments.arguments(Option.of(HoodieFileFormat.PARQUET)),
        Arguments.arguments(Option.of(HoodieFileFormat.ORC))
    );
  }

  @ParameterizedTest
  @MethodSource("formatProviderFn")
  public void testTextFormatMetaFile(Option<HoodieFileFormat> format) throws IOException {
    // given
    final StoragePath partitionPath = new StoragePath(basePath, "a/b/"
        + format.map(Enum::name).orElse("text"));
    storage.createDirectory(partitionPath);
    final String commitTime = "000000000001";
    HoodiePartitionMetadata writtenMetadata = new HoodiePartitionMetadata(
        metaClient.getStorage(), commitTime, new StoragePath(basePath), partitionPath,
        format);
    writtenMetadata.trySave();

    // when
    HoodiePartitionMetadata readMetadata = new HoodiePartitionMetadata(
        metaClient.getStorage(), partitionPath);

    // then
    assertTrue(HoodiePartitionMetadata.hasPartitionMetadata(storage, partitionPath));
    assertEquals(Option.of(commitTime), readMetadata.readPartitionCreatedCommitTime());
    assertEquals(3, readMetadata.getPartitionDepth());
  }

  @Test
  public void testErrorIfAbsent() throws IOException {
    final StoragePath partitionPath = new StoragePath(basePath, "a/b/not-a-partition");
    storage.createDirectory(partitionPath);
    HoodiePartitionMetadata readMetadata = new HoodiePartitionMetadata(
        metaClient.getStorage(), partitionPath);
    assertThrows(HoodieException.class, readMetadata::readPartitionCreatedCommitTime);
  }

  @Test
  public void testFileNames() {
    assertEquals(new StoragePath("/a/b/c/.hoodie_partition_metadata"),
        HoodiePartitionMetadata.textFormatMetaFilePath(new StoragePath("/a/b/c")));
    assertEquals(Arrays.asList(new StoragePath("/a/b/c/.hoodie_partition_metadata.parquet"),
            new StoragePath("/a/b/c/.hoodie_partition_metadata.orc")),
        HoodiePartitionMetadata.baseFormatMetaFilePaths(new StoragePath("/a/b/c")));
  }
}
