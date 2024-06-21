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

package org.apache.hudi.metadata;

import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestHoodieBackedTableMetadataWriter {

  private static FileStatus createMetadataFileStatus() {
    FileStatus status = mock(FileStatus.class);
    Path path = mock(Path.class);
    String fullName = HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE_PREFIX;
    when(path.getName()).thenReturn(fullName);
    when(status.getPath()).thenReturn(path);
    return status;
  }

  private static FileStatus createFileStatus(String groupName, String splitTwo, String commitTime, String extension) {
    FileStatus status = mock(FileStatus.class);
    Path path = mock(Path.class);
    String fullName = groupName + "_" + splitTwo + "_" + commitTime + extension;
    when(path.getName()).thenReturn(fullName);
    when(status.getPath()).thenReturn(path);
    return status;
  }

  private static Stream<Arguments> generateFileStatusArguments() {
    final String PARQ_EXT = HoodieFileFormat.PARQUET.getFileExtension();
    final String HFILE_EXT = HoodieFileFormat.HFILE.getFileExtension();
    final String ORC_EXT = HoodieFileFormat.ORC.getFileExtension();
    final String LOG_EXT = HoodieFileFormat.HOODIE_LOG.getFileExtension();
    return Stream.of(
        // two different parquet files = not dupe
        Arguments.of(new FileStatus[] {
            createMetadataFileStatus(),
            createFileStatus("file1", "different", "01", PARQ_EXT),
            createFileStatus("file2", "diff", "01", PARQ_EXT)
        }, false),
        // two parquet files with same group different commit = not dupe
        Arguments.of(new FileStatus[] {
            createMetadataFileStatus(),
            createFileStatus("file1", "same", "01", PARQ_EXT),
            createFileStatus("file1", "same", "02", PARQ_EXT)
        }, false),
        // two parquet files with same file group same commit = dupe
        Arguments.of(new FileStatus[] {
            createMetadataFileStatus(),
            createFileStatus("file1", "different", "01", PARQ_EXT),
            createFileStatus("file1", "diff", "01", PARQ_EXT)
        }, true),
        // parquet/hfile with same file group same commit = dupe
        Arguments.of(new FileStatus[] {
            createMetadataFileStatus(),
            createFileStatus("file1", "different", "01", PARQ_EXT),
            createFileStatus("file1", "diff", "01", HFILE_EXT)
        }, true),
        // parquet/orc with same file group same commit = dupe
        Arguments.of(new FileStatus[] {
            createMetadataFileStatus(),
            createFileStatus("file1", "different", "01", PARQ_EXT),
            createFileStatus("file1", "diff", "01", ORC_EXT)
        }, true),
        // parquet/log with same file group same commit = not dupe
        Arguments.of(new FileStatus[] {
            createMetadataFileStatus(),
            createFileStatus("file1", "different", "01", PARQ_EXT),
            createFileStatus("file1", "diff", "01", LOG_EXT)
        }, false),
        // log/log with same file group same commit = not dupe
        Arguments.of(new FileStatus[] {
            createMetadataFileStatus(),
            createFileStatus("file1", "different", "01", LOG_EXT),
            createFileStatus("file1", "diff", "01", LOG_EXT)
        }, false)
    );
  }

  @ParameterizedTest
  @MethodSource("generateFileStatusArguments")
  public void testDirectoryInfoThrowsErrorForDupeNameCommitPairs(FileStatus[] fileStatuses, boolean expectError) {
    if (expectError) {
      assertThrows(HoodieIOException.class,
          () -> new HoodieBackedTableMetadataWriter.DirectoryInfo("any", fileStatuses, "999999")
      );
    } else {
      assertDoesNotThrow(
          () -> new HoodieBackedTableMetadataWriter.DirectoryInfo("any", fileStatuses, "999999")
      );
    }
  }
}
