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

import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests {@link HoodiePartitionMetadata}.
 */
public class TestHoodiePartitionMetadata {

  FileSystem fs;
  String basePath;
  @TempDir
  public java.nio.file.Path tempDir;

  @BeforeEach
  public void setupTest() {
    fs = mock(FileSystem.class);
    try {
      java.nio.file.Path basePath = tempDir.resolve("dataset");
      java.nio.file.Files.createDirectories(basePath);
      this.basePath = basePath.toAbsolutePath().toString();
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
  }

  @AfterEach
  public void tearDown() throws Exception {
    fs = null;
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
    final Path partitionPath = new Path(basePath, "a/b/" + format.map(Enum::name).orElse("text"));
    fs.mkdirs(partitionPath);
    when(fs.exists(partitionPath)).thenReturn(true);
    when(fs.exists(any(Path.class))).thenReturn(true);

    final String commitTime = "000000000001";
    HoodiePartitionMetadata writtenMetadata = new HoodiePartitionMetadata(fs, commitTime, new Path(basePath), partitionPath, format);
    writtenMetadata.trySave(0);
    assertTrue(HoodiePartitionMetadata.hasPartitionMetadata(fs, partitionPath));
    assertEquals(Option.of(commitTime), writtenMetadata.readPartitionCreatedCommitTime());
    assertEquals(3, writtenMetadata.getPartitionDepth());
  }

  @Test
  public void testErrorIfAbsent() throws IOException {
    final Path partitionPath = new Path(basePath, "a/b/not-a-partition");
    fs.mkdirs(partitionPath);
    HoodiePartitionMetadata readMetadata = new HoodiePartitionMetadata(fs, new Path(basePath, partitionPath));
    assertThrows(HoodieException.class, readMetadata::readPartitionCreatedCommitTime);
  }

  @Test
  public void testFileNames() {
    assertEquals(new Path("/a/b/c/.hoodie_partition_metadata"), HoodiePartitionMetadata.textFormatMetaFilePath(new Path("/a/b/c")));
    assertEquals(Arrays.asList(new Path("/a/b/c/.hoodie_partition_metadata.parquet"),
        new Path("/a/b/c/.hoodie_partition_metadata.orc")), HoodiePartitionMetadata.baseFormatMetaFilePaths(new Path("/a/b/c")));
  }
}
