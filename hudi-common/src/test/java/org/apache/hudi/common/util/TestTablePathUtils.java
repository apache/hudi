/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class TestTablePathUtils {

  @TempDir
  static File tempDir;
  private static FileSystem fs;
  private static Path tablePath;
  private static Path partitionPath1;
  private static Path partitionPath2;
  private static Path filePath1;
  private static Path filePath2;

  @BeforeAll
  static void setup() throws IOException {
    URI tablePathURI = Paths.get(tempDir.getAbsolutePath(),"test_table").toUri();
    tablePath = new Path(tablePathURI);
    fs = tablePath.getFileSystem(new Configuration());

    // Create bootstrap index folder
    assertTrue(new File(
        Paths.get(tablePathURI.getPath(), HoodieTableMetaClient.BOOTSTRAP_INDEX_ROOT_FOLDER_PATH).toUri()).mkdirs());

    // Create partition folders
    URI partitionPathURI1 = Paths.get(tablePathURI.getPath(),"key1=abc/key2=def").toUri();
    partitionPath1 = new Path(partitionPathURI1);
    URI partitionPathURI2 = Paths.get(tablePathURI.getPath(),"key1=xyz/key2=def").toUri();
    partitionPath2 = new Path(partitionPathURI2);

    assertTrue(new File(partitionPathURI1).mkdirs());
    assertTrue(new File(partitionPathURI2).mkdirs());

    HoodiePartitionMetadata partitionMetadata1 = new HoodiePartitionMetadata(fs, Instant.now().toString(), tablePath,
                                                                             partitionPath1);
    partitionMetadata1.trySave(1);
    HoodiePartitionMetadata partitionMetadata2 = new HoodiePartitionMetadata(fs, Instant.now().toString(), tablePath,
                                                                             partitionPath2);
    partitionMetadata2.trySave(2);

    // Create files
    URI filePathURI1 = Paths.get(partitionPathURI1.getPath(), "data1.parquet").toUri();
    filePath1 = new Path(filePathURI1);
    URI filePathURI2 = Paths.get(partitionPathURI2.getPath(), "data2.parquet").toUri();
    filePath2 = new Path(filePathURI2);

    assertTrue(new File(filePathURI1).createNewFile());
    assertTrue(new File(filePathURI2).createNewFile());
  }

  @Test
  void getTablePathFromTablePath() throws IOException {
    Option<Path> inferredTablePath = TablePathUtils.getTablePath(fs, tablePath);
    assertEquals(tablePath, inferredTablePath.get());
  }

  @Test
  void getTablePathFromMetadataFolderPath() throws IOException {
    Path metadataFolder = new Path(tablePath, HoodieTableMetaClient.METAFOLDER_NAME);
    Option<Path> inferredTablePath = TablePathUtils.getTablePath(fs, metadataFolder);
    assertEquals(tablePath, inferredTablePath.get());
  }

  @Test
  void getTablePathFromMetadataSubFolderPath() throws IOException {
    Path auxFolder = new Path(tablePath, HoodieTableMetaClient.AUXILIARYFOLDER_NAME);
    Option<Path> inferredTablePath = TablePathUtils.getTablePath(fs, auxFolder);
    assertEquals(tablePath, inferredTablePath.get());

    Path bootstrapIndexFolder = new Path(tablePath, HoodieTableMetaClient.BOOTSTRAP_INDEX_ROOT_FOLDER_PATH);
    inferredTablePath = TablePathUtils.getTablePath(fs, bootstrapIndexFolder);
    assertEquals(tablePath, inferredTablePath.get());
  }

  @Test
  void getTablePathFromPartitionFolderPath() throws IOException {
    Option<Path> inferredTablePath = TablePathUtils.getTablePath(fs, partitionPath1);
    assertEquals(tablePath, inferredTablePath.get());

    inferredTablePath = TablePathUtils.getTablePath(fs, partitionPath2);
    assertEquals(tablePath, inferredTablePath.get());
  }

  @Test
  void getTablePathFromFilePath() throws IOException {
    Option<Path> inferredTablePath = TablePathUtils.getTablePath(fs, filePath1);
    assertEquals(tablePath, inferredTablePath.get());

    inferredTablePath = TablePathUtils.getTablePath(fs, filePath2);
    assertEquals(tablePath, inferredTablePath.get());
  }
}
