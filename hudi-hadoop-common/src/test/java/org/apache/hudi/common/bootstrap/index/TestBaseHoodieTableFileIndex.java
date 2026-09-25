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

package org.apache.hudi.common.bootstrap.index;

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieTableQueryType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.core.read.BaseHoodieTableFileIndex;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.storage.StoragePath;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.hadoop.fs.HadoopFSUtils.getStorageConf;

@ExtendWith(MockitoExtension.class)
class TestBaseHoodieTableFileIndex extends HoodieCommonTestHarness {

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testGetFileSlicesCount(boolean useSpillableMap) throws IOException {
    initMetaClient();
    TypedProperties properties = new TypedProperties();
    if (useSpillableMap) {
      properties.put(HoodieCommonConfig.HOODIE_FILE_INDEX_USE_SPILLABLE_MAP.key(), true);
    }
    BaseHoodieTableFileIndex baseHoodieTableFileIndex = new TestLocalIndex(
        new HoodieLocalEngineContext(getStorageConf()),
        metaClient,
        properties,
        HoodieTableQueryType.READ_OPTIMIZED,
        Collections.emptyList(),
        Option.empty(),
        false,
        false,
        true,
        null,
        true,
        Option.empty(),
        Option.empty()
    );
    Assertions.assertTrue(baseHoodieTableFileIndex.getFileSlicesCount() == 0);
  }

  /**
   * The metadata config the index builds is gated on a three-way conjunction, and both of the
   * added conjuncts came from behavior fixes: {@code isFilesPartitionAvailable} from HUDI-5403
   * (#7488, a Trino listing regression) and {@code useLatestBaseFilesPathFilterForListing} from
   * #18136. Nothing else in the repo asserts how that conjunction resolves.
   */
  @ParameterizedTest
  @CsvSource({
      "true,  true,  false, true",
      "true,  true,  true,  false",
      "true,  false, false, false",
      "true,  false, true,  false",
      "false, true,  false, false",
      "false, true,  true,  false",
      "false, false, false, false",
      "false, false, true,  false"
  })
  void testMetadataConfigIsEnabledOnlyWhenEveryConjunctHolds(boolean metadataEnabled,
                                                             boolean filesPartitionAvailable,
                                                             boolean useLatestBaseFilesPathFilterForListing,
                                                             boolean expectedEnabled) throws IOException {
    initMetaClient();
    if (filesPartitionAvailable) {
      metaClient.getTableConfig().setValue(
          HoodieTableConfig.TABLE_METADATA_PARTITIONS, HoodieTableMetadataUtil.PARTITION_NAME_FILES);
    }
    TypedProperties properties = new TypedProperties();
    properties.put(HoodieMetadataConfig.ENABLE.key(), String.valueOf(metadataEnabled));

    TestLocalIndex fileIndex = new TestLocalIndex(
        new HoodieLocalEngineContext(getStorageConf()),
        metaClient,
        properties,
        HoodieTableQueryType.READ_OPTIMIZED,
        Collections.emptyList(),
        Option.empty(),
        useLatestBaseFilesPathFilterForListing,
        false,
        true,
        null,
        true,
        Option.empty(),
        Option.empty()
    );

    Assertions.assertEquals(expectedEnabled, fileIndex.metadataConfig().isEnabled());
  }

  /**
   * Soak test for the unbounded growth of {@code BaseHoodieTableFileIndex.cachedAllInputFileSlices}.
   *
   * <p>With lazy listing the map was only ever inserted into, on the query path, and only ever emptied by
   * {@code doRefresh()}, which runs in the constructor and in {@code refresh()} and not per query. A file index that
   * survives for the lifetime of a session (as it does behind a cached Spark catalog relation) therefore retained one
   * entry for every distinct partition it had ever been asked about: querying N distinct partitions left N entries.
   *
   * <p>This drives one single-partition lookup per distinct partition against one index instance, exactly as the
   * reported reproduction does, and asserts both halves of the fix: the cache stays within its configured bound, and
   * every lookup still returns a complete, non-null answer for the partition it asked about (i.e. a partition that
   * was evicted is transparently re-listed rather than being served as a missing entry).
   */
  @Test
  void testFileSliceCacheStaysBoundedAcrossManyDistinctPartitionLookups() throws Exception {
    initMetaClient();
    int cacheBound = 10;
    int partitionsQueried = 200;

    TypedProperties properties = new TypedProperties();
    properties.put(HoodieCommonConfig.HOODIE_FILE_INDEX_MAX_CACHED_PARTITIONS.key(), cacheBound);

    TestLocalIndex fileIndex = new TestLocalIndex(
        new HoodieLocalEngineContext(getStorageConf()),
        metaClient,
        properties,
        HoodieTableQueryType.SNAPSHOT,
        Collections.emptyList(),
        Option.empty(),
        false,
        false,
        true,
        new BaseHoodieTableFileIndex.NoopCache(),
        // lazy listing: this is the mode the reported Spark SQL / catalog path runs in
        true,
        Option.empty(),
        Option.empty()
    );

    List<Integer> observedCacheSizes = new ArrayList<>();
    for (int i = 0; i < partitionsQueried; i++) {
      String relativePartitionPath = "dt=" + i;
      Files.createDirectories(Paths.get(basePath, relativePartitionPath));
      BaseHoodieTableFileIndex.PartitionPath partition =
          new BaseHoodieTableFileIndex.PartitionPath(relativePartitionPath, new Object[] {relativePartitionPath});

      Map<BaseHoodieTableFileIndex.PartitionPath, List<FileSlice>> fileSlices =
          fileIndex.getInputFileSlicesForTest(partition);

      Assertions.assertTrue(fileSlices.containsKey(partition),
          "Every queried partition must be present in the result for query " + i);
      Assertions.assertNotNull(fileSlices.get(partition),
          "A queried partition must never map to null, even once eviction is in play (query " + i + ")");

      int cacheSize = cachedFileSliceCount(fileIndex);
      observedCacheSizes.add(cacheSize);
      Assertions.assertTrue(cacheSize <= cacheBound,
          "File-slice cache grew to " + cacheSize + " entries after " + (i + 1)
              + " distinct partitions, bound is " + cacheBound);
    }

    // Before the fix this would have been partitionsQueried; sanity-check the cache is actually being used at all
    Assertions.assertEquals(cacheBound, observedCacheSizes.get(observedCacheSizes.size() - 1),
        "A saturated cache should sit at exactly its bound");
  }

  /**
   * Eager listing deliberately holds every partition of the table, and that set is rebuilt from scratch on each
   * refresh rather than accumulating over the session, so the bound must not be applied there: doing so would force
   * partitions the query provably needs to be re-listed.
   */
  @Test
  void testEagerListingIsNotBounded() throws Exception {
    initMetaClient();
    TypedProperties properties = new TypedProperties();
    properties.put(HoodieCommonConfig.HOODIE_FILE_INDEX_MAX_CACHED_PARTITIONS.key(), 1);

    TestLocalIndex fileIndex = new TestLocalIndex(
        new HoodieLocalEngineContext(getStorageConf()),
        metaClient,
        properties,
        HoodieTableQueryType.SNAPSHOT,
        Collections.emptyList(),
        Option.empty(),
        false,
        false,
        true,
        new BaseHoodieTableFileIndex.NoopCache(),
        false,
        Option.empty(),
        Option.empty()
    );

    // BoundedFileSliceCache is package-private in hudi-common, so identify it by name from here
    Assertions.assertEquals(HashMap.class, rawFileSliceCache(fileIndex).getClass(),
        "Eager listing must keep the plain unbounded map");
  }

  private static Object rawFileSliceCache(BaseHoodieTableFileIndex fileIndex) throws Exception {
    Field field = BaseHoodieTableFileIndex.class.getDeclaredField("cachedAllInputFileSlices");
    field.setAccessible(true);
    return field.get(fileIndex);
  }

  @SuppressWarnings("unchecked")
  private static int cachedFileSliceCount(BaseHoodieTableFileIndex fileIndex) throws Exception {
    return ((Map<BaseHoodieTableFileIndex.PartitionPath, List<FileSlice>>) rawFileSliceCache(fileIndex)).size();
  }

  private static class TestLocalIndex extends BaseHoodieTableFileIndex {

    public TestLocalIndex(HoodieEngineContext engineContext, HoodieTableMetaClient metaClient, TypedProperties configProperties, HoodieTableQueryType queryType,
                          List<StoragePath> queryPaths, Option<String> specifiedQueryInstant, boolean useLatestBaseFilesPathFilterForListing,
                          boolean shouldIncludePendingCommits, boolean shouldValidateInstant,
                          FileStatusCache fileStatusCache, boolean shouldListLazily, Option<String> startCompletionTime, Option<String> endCompletionTime) {
      super(engineContext, metaClient, configProperties, queryType, queryPaths, specifiedQueryInstant, useLatestBaseFilesPathFilterForListing, shouldIncludePendingCommits, shouldValidateInstant,
          fileStatusCache, shouldListLazily, startCompletionTime, endCompletionTime);
    }

    HoodieMetadataConfig metadataConfig() {
      return metadataConfig;
    }

    Map<PartitionPath, List<FileSlice>> getInputFileSlicesForTest(PartitionPath... partitions) {
      return getInputFileSlices(partitions);
    }

    @Override
    protected Object[] parsePartitionColumnValues(String[] partitionColumns, String partitionPath) {
      return new Object[0];
    }
  }
}