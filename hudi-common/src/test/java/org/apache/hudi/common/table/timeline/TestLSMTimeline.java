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

package org.apache.hudi.common.table.timeline;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathFilter;
import org.apache.hudi.storage.StoragePathInfo;

import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test cases for {@link LSMTimeline}.
 */
public class TestLSMTimeline {
  @Test
  void testParseMinInstantTime() {
    String fileName = "001_002_0.parquet";
    String minInstantTime = LSMTimeline.getMinInstantTime(fileName);
    assertThat(minInstantTime, is("001"));
    assertThrows(HoodieException.class, () -> LSMTimeline.getMinInstantTime("invalid_file_name.parquet"));
  }

  @Test
  void testParseMaxInstantTime() {
    String fileName = "001_002_0.parquet";
    String maxInstantTime = LSMTimeline.getMaxInstantTime(fileName);
    assertThat(maxInstantTime, is("002"));
    assertThrows(HoodieException.class, () -> LSMTimeline.getMaxInstantTime("invalid_file_name.parquet"));
  }

  @Test
  void testParseFileLayer() {
    String fileName = "001_002_0.parquet";
    int layer = LSMTimeline.getFileLayer(fileName);
    assertThat(layer, is(0));
    assertThat("for invalid file name, returns 0", LSMTimeline.getFileLayer("invalid_file_name.parquet"), is(0));
  }

  @Test
  void testManifestFileValidation() {
    StoragePathFilter filter = LSMTimeline.getManifestFilePathFilter();

    assertTrue(filter.accept(new StoragePath("manifest_1")));
    assertTrue(filter.accept(new StoragePath("manifest_114")));
    assertThat(LSMTimeline.getManifestVersion("manifest_114"), is(114));

    assertFalse(filter.accept(new StoragePath("manifest_114.tmp")));
    assertFalse(filter.accept(new StoragePath("manifest_114.a5e022a6-e5c9-4450-b9e5-9296262329b5")));
    assertFalse(filter.accept(new StoragePath("manifest_invalid")));
    assertFalse(filter.accept(new StoragePath("manifest_")));
    assertThrows(HoodieException.class,
        () -> LSMTimeline.getManifestVersion("manifest_114.a5e022a6-e5c9-4450-b9e5-9296262329b5"));
  }

  @Test
  void testLatestSnapshotVersionFallbackIgnoresTemporaryManifest() throws IOException {
    StoragePath archivePath = new StoragePath("/table/.hoodie/timeline/history");
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieStorage storage = mock(HoodieStorage.class);
    List<StoragePathInfo> manifestFiles = Arrays.asList(
        manifestFile(archivePath, "manifest_113"),
        manifestFile(archivePath, "manifest_114"),
        manifestFile(archivePath, "manifest_114.a5e022a6-e5c9-4450-b9e5-9296262329b5"));

    when(metaClient.getStorage()).thenReturn(storage);
    when(storage.open(LSMTimeline.getVersionFilePath(archivePath))).thenThrow(new FileNotFoundException());
    when(storage.listDirectEntries(eq(archivePath), any(StoragePathFilter.class))).thenAnswer(invocation -> {
      StoragePathFilter filter = invocation.getArgument(1);
      return manifestFiles.stream().filter(file -> filter.accept(file.getPath())).collect(Collectors.toList());
    });

    assertThat(LSMTimeline.latestSnapshotVersion(metaClient, archivePath), is(114));
    assertThat(LSMTimeline.allSnapshotVersions(metaClient, archivePath), is(Arrays.asList(113, 114)));
  }

  private static StoragePathInfo manifestFile(StoragePath archivePath, String fileName) {
    return new StoragePathInfo(new StoragePath(archivePath, fileName), 0, false, (short) 1, 0, 0);
  }
}
