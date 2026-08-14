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

package org.apache.hudi.core.read;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.core.read.BaseHoodieTableFileIndex.PartitionPath;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class BaseHoodieTableFileIndexTest {

  @Test
  public void testGetMetadataConfigReturnsFieldValue() throws Exception {
    // Create a mock of BaseHoodieTableFileIndex
    BaseHoodieTableFileIndex fileIndex = mock(BaseHoodieTableFileIndex.class, 
        org.mockito.Mockito.CALLS_REAL_METHODS);
    
    // Create a test metadata config
    HoodieMetadataConfig testConfig = HoodieMetadataConfig.newBuilder()
        .enable(true)
        .withMetadataIndexBloomFilter(true)
        .withMetadataIndexColumnStats(true)
        .build();
    
    // Use reflection to set the private metadataConfig field
    Field metadataConfigField = BaseHoodieTableFileIndex.class.getDeclaredField("metadataConfig");
    metadataConfigField.setAccessible(true);
    metadataConfigField.set(fileIndex, testConfig);
    
    // Test the getMetadataConfig method
    HoodieMetadataConfig result = fileIndex.getMetadataConfig();
    
    assertNotNull(result, "Metadata config should not be null");
    assertSame(testConfig, result, "Should return the same metadata config instance");
    assertEquals(true, result.isEnabled(), "Metadata should be enabled");
    assertEquals(true, result.isBloomFilterIndexEnabled(), "Bloom filter index should be enabled");
    assertEquals(true, result.isColumnStatsIndexEnabled(), "Column stats index should be enabled");
  }

  /**
   * Regression test for the empty-partition NPE that surfaces in {@code getInputFileSlices}
   * when the {@code hoodie.datasource.read.file.index.list.file.statuses.using.ro.path.filter}
   * code path is exercised on a COW (or READ_OPTIMIZED) table that contains a partition
   * holding zero base files.
   *
   * <p>Before the fix, {@link BaseHoodieTableFileIndex#generatePartitionFileSlicesPostROTablePathFilter}
   * built its result map by iterating over the file list, so a partition with no files received
   * no entry. The downstream {@code Collectors.toMap(identity, p -> cache.get(p))} in
   * {@code getInputFileSlices} then dereferenced a null value and threw NPE inside
   * {@code Collectors.uniqKeysMapAccumulator}.
   *
   * <p>After the fix, every input partition appears in the returned map (with an empty list
   * for empty partitions), preserving the contract already honored by the non-RO path
   * ({@code filterFiles}).
   */
  @Test
  public void testGeneratePartitionFileSlicesPostROTablePathFilterIncludesEmptyPartitions() throws Exception {
    BaseHoodieTableFileIndex fileIndex = mock(BaseHoodieTableFileIndex.class,
        org.mockito.Mockito.CALLS_REAL_METHODS);

    StoragePath basePath = new StoragePath("/tmp/hudi_empty_partition_test");
    Field basePathField = BaseHoodieTableFileIndex.class.getDeclaredField("basePath");
    basePathField.setAccessible(true);
    basePathField.set(fileIndex, basePath);

    PartitionPath partitionWithFiles = new PartitionPath("dt=2026-01-01", new Object[]{"2026-01-01"});
    PartitionPath emptyPartition = new PartitionPath("dt=2026-01-02", new Object[]{"2026-01-02"});
    PartitionPath anotherEmpty = new PartitionPath("dt=2026-01-03", new Object[]{"2026-01-03"});
    List<PartitionPath> partitions = Arrays.asList(partitionWithFiles, emptyPartition, anotherEmpty);

    StoragePathInfo file = new StoragePathInfo(
        new StoragePath(basePath, "dt=2026-01-01/file-0_0-0-0_20260101000000001.parquet"),
        100L, false, (short) 1, 1024L, 0L);
    List<StoragePathInfo> allFiles = Collections.singletonList(file);

    Method generateMethod = BaseHoodieTableFileIndex.class.getDeclaredMethod(
        "generatePartitionFileSlicesPostROTablePathFilter", List.class, List.class);
    generateMethod.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<PartitionPath, List<FileSlice>> result =
        (Map<PartitionPath, List<FileSlice>>) generateMethod.invoke(fileIndex, partitions, allFiles);

    assertNotNull(result, "Result map must not be null");
    assertEquals(3, result.size(),
        "Result map must contain an entry for every input partition, including empty ones");
    assertTrue(result.containsKey(partitionWithFiles));
    assertTrue(result.containsKey(emptyPartition),
        "Empty partition must appear in the result so getInputFileSlices does not NPE");
    assertTrue(result.containsKey(anotherEmpty),
        "Empty partition must appear in the result so getInputFileSlices does not NPE");
    assertEquals(1, result.get(partitionWithFiles).size(),
        "Partition with files should retain its file slice");
    assertTrue(result.get(emptyPartition).isEmpty(),
        "Empty partition's file slice list must be present and empty (not null, not missing)");
    assertTrue(result.get(anotherEmpty).isEmpty(),
        "Empty partition's file slice list must be present and empty (not null, not missing)");
  }
}