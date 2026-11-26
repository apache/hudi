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
import org.apache.hudi.common.util.PartitionPathEncodeUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieKeyException;
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
import static org.junit.jupiter.api.Assertions.assertFalse;
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
    String relativePartitionPath = "a/b/" + format.map(Enum::name).orElse("text");
    final StoragePath partitionPath = new StoragePath(basePath, relativePartitionPath);
    storage.createDirectory(partitionPath);
    final String commitTime = "000000000001";
    HoodiePartitionMetadata writtenMetadata = new HoodiePartitionMetadata(
        metaClient.getStorage(), commitTime, new StoragePath(basePath), partitionPath,
        format);
    writtenMetadata.trySave(relativePartitionPath);

    // when
    HoodiePartitionMetadata readMetadata = new HoodiePartitionMetadata(
        metaClient.getStorage(), partitionPath);

    // then
    assertTrue(HoodiePartitionMetadata.hasPartitionMetadata(storage, partitionPath));
    assertEquals(Option.of(commitTime), readMetadata.readPartitionCreatedCommitTime());
    assertEquals(3, readMetadata.getPartitionDepth());
  }

  @ParameterizedTest
  @MethodSource("formatProviderFn")
  void testRejectsTraversalBeforeCreatingMetadata(Option<HoodieFileFormat> format) throws IOException {
    StoragePath tablePath = new StoragePath(basePath, "table");
    for (String relativePartitionPath : Arrays.asList("..", "../outside", "a/../b", "..\\outside")) {
      StoragePath partitionPath = new StoragePath(tablePath, relativePartitionPath);
      storage.createDirectory(partitionPath);
      HoodiePartitionMetadata metadata = new HoodiePartitionMetadata(
          storage, "0001", tablePath, partitionPath, format);

      assertThrows(HoodieKeyException.class, () -> metadata.trySave(relativePartitionPath));
      assertFalse(HoodiePartitionMetadata.hasPartitionMetadata(storage, partitionPath));
    }
  }

  @ParameterizedTest
  @MethodSource("formatProviderFn")
  void testValidatesOriginalPathBeforeNormalization(Option<HoodieFileFormat> format) throws IOException {
    String relativePartitionPath = "a/../b";
    StoragePath partitionPath = new StoragePath(basePath, relativePartitionPath);
    assertEquals(new StoragePath(basePath, "b"), partitionPath);
    storage.createDirectory(partitionPath);
    HoodiePartitionMetadata metadata = new HoodiePartitionMetadata(
        storage, "0001", new StoragePath(basePath), partitionPath, format);
    assertThrows(HoodieKeyException.class, () -> metadata.trySave(relativePartitionPath));
    assertTrue(storage.listDirectEntries(partitionPath).isEmpty());
  }

  @ParameterizedTest
  @MethodSource("formatProviderFn")
  void testExistingMetadataSkipsTraversalValidation(Option<HoodieFileFormat> format) throws IOException {
    StoragePath partitionPath = new StoragePath(basePath, "b");
    storage.createDirectory(partitionPath);
    HoodiePartitionMetadata original = new HoodiePartitionMetadata(
        storage, "0001", new StoragePath(basePath), partitionPath, format);
    original.trySave("b");

    HoodiePartitionMetadata subsequent = new HoodiePartitionMetadata(
        storage, "0002", new StoragePath(basePath), partitionPath, format);
    subsequent.trySave("a/../b");
    HoodiePartitionMetadata saved = new HoodiePartitionMetadata(storage, partitionPath);
    assertEquals(Option.of("0001"), saved.readPartitionCreatedCommitTime());
  }

  @ParameterizedTest
  @MethodSource("formatProviderFn")
  void testExistingEncodingIsAccepted(Option<HoodieFileFormat> format) throws IOException {
    String partitionValue = "../a/./b";
    String encodedPath = PartitionPathEncodeUtils.escapePathName(partitionValue);
    StoragePath partitionPath = new StoragePath(basePath, encodedPath);
    storage.createDirectory(partitionPath);
    HoodiePartitionMetadata metadata = new HoodiePartitionMetadata(
        storage, "0001", new StoragePath(basePath), partitionPath, format);
    metadata.trySave(encodedPath);
    assertTrue(HoodiePartitionMetadata.hasPartitionMetadata(storage, partitionPath));
    assertEquals(new StoragePath(basePath), partitionPath.getParent());
    assertEquals(partitionValue, PartitionPathEncodeUtils.unescapePathName(encodedPath));
  }

  @ParameterizedTest
  @MethodSource("formatProviderFn")
  void testEmptyRelativePartitionPath(Option<HoodieFileFormat> format) {
    StoragePath tablePath = new StoragePath(basePath);
    HoodiePartitionMetadata metadata = new HoodiePartitionMetadata(
        storage, "0001", tablePath, tablePath, format);
    metadata.trySave("");
    assertTrue(HoodiePartitionMetadata.hasPartitionMetadata(storage, tablePath));
    assertEquals(0, metadata.getPartitionDepth());
  }

  @Test
  void testRejectsNullRelativePartitionPath() {
    StoragePath tablePath = new StoragePath(basePath);
    HoodiePartitionMetadata metadata = new HoodiePartitionMetadata(
        storage, "0001", tablePath, tablePath, Option.empty());
    assertThrows(IllegalArgumentException.class, () -> metadata.trySave(null));
    assertFalse(HoodiePartitionMetadata.hasPartitionMetadata(storage, tablePath));
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
