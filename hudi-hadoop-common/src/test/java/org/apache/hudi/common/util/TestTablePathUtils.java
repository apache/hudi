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

import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StoragePath;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link TablePathUtils}.
 */
public final class TestTablePathUtils {
  private static final String BASE_FILE_EXTENSION =
      HoodieTableConfig.BASE_FILE_FORMAT.defaultValue().getFileExtension();

  @TempDir
  public File tempDir;
  private static HoodieStorage storage;
  private static StoragePath tablePath;
  private static StoragePath partitionPath1;
  private static StoragePath partitionPath2;
  private static StoragePath filePath1;
  private static StoragePath filePath2;

  private void setup() throws IOException {
    setup(Option.empty());
  }

  private void setup(Option<HoodieFileFormat> partitionMetafileFormat) throws IOException {
    URI tablePathURI = Paths.get(tempDir.getAbsolutePath(), "test_table").toUri();
    tablePath = new StoragePath(tablePathURI);
    storage = HoodieStorageUtils.getStorage(tablePathURI.toString(), HoodieTestUtils.getDefaultStorageConfWithDefaults());

    // Create bootstrap index folder
    assertTrue(new File(
        Paths.get(tablePathURI.getPath(), HoodieTableMetaClient.BOOTSTRAP_INDEX_ROOT_FOLDER_PATH)
            .toUri()).mkdirs());

    // Create partition folders
    URI partitionPathURI1 = Paths.get(tablePathURI.getPath(), "key1=abc/key2=def").toUri();
    partitionPath1 = new StoragePath(partitionPathURI1);
    URI partitionPathURI2 = Paths.get(tablePathURI.getPath(), "key1=xyz/key2=def").toUri();
    partitionPath2 = new StoragePath(partitionPathURI2);

    assertTrue(new File(partitionPathURI1).mkdirs());
    assertTrue(new File(partitionPathURI2).mkdirs());

    HoodiePartitionMetadata partitionMetadata1 = new HoodiePartitionMetadata(
        storage, Instant.now().toString(), tablePath,
        partitionPath1, partitionMetafileFormat);
    partitionMetadata1.trySave();
    HoodiePartitionMetadata partitionMetadata2 = new HoodiePartitionMetadata(
        storage, Instant.now().toString(), tablePath,
        partitionPath2, partitionMetafileFormat);
    partitionMetadata2.trySave();

    // Create files
    URI filePathURI1 =
        Paths.get(partitionPathURI1.getPath(), "data1" + BASE_FILE_EXTENSION).toUri();
    filePath1 = new StoragePath(filePathURI1);
    URI filePathURI2 =
        Paths.get(partitionPathURI2.getPath(), "data2" + BASE_FILE_EXTENSION).toUri();
    filePath2 = new StoragePath(filePathURI2);

    assertTrue(new File(filePathURI1).createNewFile());
    assertTrue(new File(filePathURI2).createNewFile());
  }

  @Test
  void getTablePathFromTablePath() throws IOException {
    setup();
    Option<StoragePath> inferredTablePath = TablePathUtils.getTablePath(storage, tablePath);
    assertEquals(tablePath, inferredTablePath.get());
  }

  @Test
  void getTablePathFromMetadataFolderPath() throws IOException {
    setup();
    StoragePath metaFolder =
        new StoragePath(tablePath, HoodieTableMetaClient.METAFOLDER_NAME);
    Option<StoragePath> inferredTablePath = TablePathUtils.getTablePath(storage, metaFolder);
    assertEquals(tablePath, inferredTablePath.get());
  }

  @Test
  void getTablePathFromMetadataSubFolderPath() throws IOException {
    setup();
    StoragePath auxFolder =
        new StoragePath(tablePath, HoodieTableMetaClient.AUXILIARYFOLDER_NAME);
    assertEquals(tablePath, TablePathUtils.getTablePath(storage, auxFolder).get());

    StoragePath bootstrapIndexFolder =
        new StoragePath(tablePath, HoodieTableMetaClient.BOOTSTRAP_INDEX_ROOT_FOLDER_PATH);
    assertEquals(tablePath, TablePathUtils.getTablePath(storage, bootstrapIndexFolder).get());

    StoragePath metadataTableFolder =
        new StoragePath(tablePath, HoodieTableMetaClient.METADATA_TABLE_FOLDER_PATH);
    StoragePath metadataTableMetaFolder =
        new StoragePath(metadataTableFolder, HoodieTableMetaClient.METAFOLDER_NAME);
    assertTrue(new File(metadataTableMetaFolder.toUri()).mkdirs());

    assertEquals(metadataTableFolder,
        TablePathUtils.getTablePath(storage, metadataTableFolder).get());

    StoragePath metadataTablePartitionFolder =
        new StoragePath(metadataTableFolder, "column_stats");
    assertTrue(new File(metadataTablePartitionFolder.toUri()).mkdir());
    assertEquals(metadataTableFolder, TablePathUtils.getTablePath(storage,
        metadataTablePartitionFolder).get());
  }

  @ParameterizedTest
  @EnumSource(value = HoodieFileFormat.class, names = {"PARQUET", "ORC"})
  void getTablePathFromPartitionFolderPath(HoodieFileFormat partitionMetafileFormat) throws IOException {
    setup(Option.of(partitionMetafileFormat));
    Option<StoragePath> inferredTablePath = TablePathUtils.getTablePath(storage, partitionPath1);
    assertEquals(tablePath, inferredTablePath.get());

    inferredTablePath = TablePathUtils.getTablePath(storage, partitionPath2);
    assertEquals(tablePath, inferredTablePath.get());
  }

  @Test
  void getTablePathFromFilePath() throws IOException {
    setup();
    Option<StoragePath> inferredTablePath = TablePathUtils.getTablePath(storage, filePath1);
    assertEquals(tablePath, inferredTablePath.get());

    inferredTablePath = TablePathUtils.getTablePath(storage, filePath2);
    assertEquals(tablePath, inferredTablePath.get());
  }
}
