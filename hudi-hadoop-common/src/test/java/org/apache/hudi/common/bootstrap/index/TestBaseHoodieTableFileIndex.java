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
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.core.read.BaseHoodieTableFileIndex;
import org.apache.hudi.core.read.BaseHoodieTableFileIndex.PartitionPath;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathFilter;

import com.github.benmanes.caffeine.cache.Cache;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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
   * reported reproduction does, and asserts both halves of the fix: the cache stays within its configured memory
   * budget, and every lookup still returns a complete, non-null answer for the partition it asked about (i.e. a
   * partition that was evicted is transparently re-listed rather than being served as a missing entry).
   */
  @Test
  void testFileSliceCacheStaysBoundedAcrossManyDistinctPartitionLookups() throws Exception {
    initMetaClient();
    long maxMemory = 8 * 1024L;
    int partitionsQueried = 200;

    TestLocalIndex fileIndex = newIndex(maxMemoryProps(maxMemory), true);

    for (int i = 0; i < partitionsQueried; i++) {
      PartitionPath partition = createPartition("dt=" + i);
      Map<PartitionPath, List<FileSlice>> fileSlices = fileIndex.getInputFileSlicesForTest(partition);

      Assertions.assertTrue(fileSlices.containsKey(partition),
          "Every queried partition must be present in the result for query " + i);
      Assertions.assertNotNull(fileSlices.get(partition),
          "A queried partition must never map to null, even once eviction is in play (query " + i + ")");

      fileIndex.cleanUpFileSliceCacheForTest();
      Assertions.assertTrue(weightedSize(fileIndex) <= maxMemory,
          "File-slice cache grew to " + weightedSize(fileIndex) + " bytes after " + (i + 1)
              + " distinct partitions, budget is " + maxMemory);
    }

    // Before the fix this held all `partitionsQueried` entries, which is the leak being fixed
    fileIndex.cleanUpFileSliceCacheForTest();
    int resident = cachedFileSliceCount(fileIndex);
    Assertions.assertTrue(resident < partitionsQueried,
        "The cache must not retain every partition ever queried, but held " + resident);
  }

  /**
   * A single query wider than the cache's budget must not cost more than one listing, and must not cost more on its
   * second execution than on its first.
   *
   * <p>This is a regression test for a self-perpetuating listing amplification: when the missing partitions of a query
   * were loaded and cached one at a time, each insertion evicted a partition that the very same query had already
   * consulted, so that partition had to be listed again on its own -- evicting another one in turn. The result was a
   * permanent {@code 1 bulk listing + one single-partition metadata lookup per partition over the budget} cost on
   * every execution of a repeated query, which on object storage is exactly the workload this bound is meant to help.
   */
  @Test
  void testRepeatedWideQueryDoesNotAmplifyListing() throws Exception {
    initMetaClient();
    // A budget far below what 30 partitions need, so every round has to evict the query's own working set
    long maxMemory = 256L;
    int partitionCount = 30;

    ListingCountingIndex fileIndex = new ListingCountingIndex(
        new HoodieLocalEngineContext(getStorageConf()), metaClient, maxMemoryProps(maxMemory),
        HoodieTableQueryType.SNAPSHOT, Collections.emptyList(), Option.empty(), false, false, true,
        new BaseHoodieTableFileIndex.NoopCache(), true, Option.empty(), Option.empty());

    List<PartitionPath> partitions = new ArrayList<>();
    for (int i = 0; i < partitionCount; i++) {
      partitions.add(createPartition("dt=" + i));
    }
    PartitionPath[] query = partitions.toArray(new PartitionPath[0]);

    for (int round = 1; round <= 5; round++) {
      fileIndex.listingCalls = 0;
      Map<PartitionPath, List<FileSlice>> result = fileIndex.getInputFileSlicesForTest(query);

      Assertions.assertEquals(partitionCount, result.size(),
          "Round " + round + " must answer for every queried partition");
      for (PartitionPath partition : partitions) {
        Assertions.assertNotNull(result.get(partition),
            "Round " + round + " returned null for " + partition.getPath());
      }
      Assertions.assertTrue(fileIndex.listingCalls <= 1,
          "Round " + round + " issued " + fileIndex.listingCalls + " listings; a query must cost at most one, "
              + "however much of it the cache had to evict. Before the fix round 1 cost 1 and every later round "
              + "cost 1 + (partitions over the budget), forever.");

      // Guard against the test going vacuous: it only proves anything while the bound is really evicting
      fileIndex.cleanUpFileSliceCacheForTest();
      Assertions.assertTrue(cachedFileSliceCount(fileIndex) < partitionCount,
          "Round " + round + " left all " + partitionCount + " partitions resident, so the budget never bound "
              + "and this test proved nothing");
    }
  }

  /**
   * {@code getTotalCachedFilesSize} is the sole backing of Spark's {@code HoodieFileIndex.sizeInBytes}, and through it
   * of {@code spark.sql.autoBroadcastJoinThreshold}. Summing the (bounded, evicting) file-slice cache would report a
   * fraction of the real size -- so a multi-terabyte table could report a few megabytes and be broadcast to every
   * executor -- and would report different figures for two planning calls of the same query. It therefore has to be
   * independent of what the cache happens to be holding.
   */
  @Test
  void testTotalCachedFilesSizeIsUnaffectedByEviction() throws Exception {
    initMetaClient();
    int partitionCount = 12;
    int fileLength = 2048;
    List<PartitionPath> partitions = createPartitionsWithBaseFiles(partitionCount, fileLength);
    PartitionPath[] query = partitions.toArray(new PartitionPath[0]);
    long expectedTotal = (long) partitionCount * fileLength;

    // Unbounded: establishes what the true total is
    TestLocalIndex unbounded = newIndex(maxMemoryProps(0L), true);
    unbounded.getInputFileSlicesForTest(query);
    Assertions.assertEquals(expectedTotal, unbounded.getTotalCachedFilesSizeForTest(),
        "Sanity check: an unbounded cache must report the sum of all base file lengths");

    // Bounded far below the query's working set, so most partitions are evicted before the query returns
    long maxMemory = 4 * 1024L;
    TestLocalIndex bounded = newIndex(maxMemoryProps(maxMemory), true);
    bounded.getInputFileSlicesForTest(query);
    bounded.cleanUpFileSliceCacheForTest();

    Assertions.assertTrue(cachedFileSliceCount(bounded) < partitionCount,
        "This test is only meaningful if the bound actually evicted something");
    Assertions.assertEquals(expectedTotal, bounded.getTotalCachedFilesSizeForTest(),
        "sizeInBytes must report the size of everything listed, not just of what survived eviction");
    // And it must be stable: asking again (without re-querying) must not move
    Assertions.assertEquals(expectedTotal, bounded.getTotalCachedFilesSizeForTest(),
        "sizeInBytes must not change between two planning calls for the same query");
  }

  /**
   * The file-slice cache is reachable concurrently: {@code HoodieFileIndex} is created per relation and Spark's
   * {@code SessionCatalog.tableRelationCache} keeps it for the session, so two queries planned at the same time share
   * one instance, and nothing on the path from {@code getInputFileSlices} down synchronizes.
   *
   * <p>An access-ordered {@code LinkedHashMap} is catastrophic here and not merely "as unsafe as the {@code HashMap}
   * it replaces": because {@code get} structurally modifies the map, concurrent *readers alone* corrupt its linked
   * list, at which point {@code removeEldestEntry} stops being consulted and the bound silently disappears. The
   * {@code @NotThreadSafe} on {@code HoodieFileIndex}/{@code SparkHoodieTableFileIndex} describes an aspiration about
   * their own mutable query state; it is not a licence for a bound that stops holding.
   *
   * <p>This asserts the three properties that matter: no operation throws, every query gets a complete answer, and
   * the bound still holds once the dust settles.
   */
  @Test
  void testConcurrentQueriesAreSafeAndKeepTheCacheBounded() throws Exception {
    initMetaClient();
    long maxMemory = 8 * 1024L;
    int threads = 8;
    int queriesPerThread = 200;
    int partitionCount = 64;

    TestLocalIndex fileIndex = newIndex(maxMemoryProps(maxMemory), true);
    List<PartitionPath> partitions = new ArrayList<>();
    for (int i = 0; i < partitionCount; i++) {
      partitions.add(createPartition("dt=" + i));
    }

    ExecutorService executor = Executors.newFixedThreadPool(threads + 1);
    CountDownLatch startLine = new CountDownLatch(1);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    AtomicBoolean done = new AtomicBoolean(false);
    List<Future<?>> futures = new ArrayList<>();

    for (int t = 0; t < threads; t++) {
      final int threadNo = t;
      futures.add(executor.submit(() -> {
        try {
          startLine.await();
          for (int q = 0; q < queriesPerThread; q++) {
            // Overlapping, rotating windows, so threads contend on the same entries
            int from = (threadNo * 7 + q) % partitionCount;
            PartitionPath[] window = new PartitionPath[8];
            for (int k = 0; k < window.length; k++) {
              window[k] = partitions.get((from + k) % partitionCount);
            }
            Map<PartitionPath, List<FileSlice>> result = fileIndex.getInputFileSlicesForTest(window);
            for (PartitionPath partition : window) {
              if (result.get(partition) == null) {
                throw new AssertionError("Concurrent query got no answer for " + partition.getPath());
              }
            }
          }
        } catch (Throwable th) {
          failure.compareAndSet(null, th);
        }
        return null;
      }));
    }
    // A concurrent reader of the cache itself: this is what an access-ordered LinkedHashMap turns into a
    // ConcurrentModificationException storm even with no writers at all
    futures.add(executor.submit(() -> {
      try {
        startLine.await();
        while (!done.get()) {
          rawFileSliceCache(fileIndex).values().forEach(slices -> Assertions.assertNotNull(slices));
        }
      } catch (Throwable th) {
        failure.compareAndSet(null, th);
      }
      return null;
    }));

    startLine.countDown();
    for (int i = 0; i < threads; i++) {
      futures.get(i).get(120, TimeUnit.SECONDS);
    }
    done.set(true);
    futures.get(threads).get(120, TimeUnit.SECONDS);
    executor.shutdownNow();

    if (failure.get() != null) {
      throw new AssertionError("Concurrent access to the file-slice cache failed", failure.get());
    }

    fileIndex.cleanUpFileSliceCacheForTest();
    Assertions.assertTrue(weightedSize(fileIndex) <= maxMemory,
        "The bound must still hold after concurrent access, but the cache holds " + weightedSize(fileIndex)
            + " bytes against a budget of " + maxMemory);
  }

  /**
   * Eager listing deliberately holds every partition of the table, and that set is rebuilt from scratch on each
   * refresh rather than accumulating over the session, so the bound must not be applied there: doing so would force
   * partitions the query provably needs to be re-listed.
   */
  @Test
  void testEagerListingIsNotBounded() throws Exception {
    initMetaClient();
    int partitionCount = 50;
    TestLocalIndex fileIndex = newIndex(maxMemoryProps(1024L), false);

    // No eviction policy at all, as opposed to one with a large maximum
    Assertions.assertFalse(fileSliceCache(fileIndex).policy().eviction().isPresent(),
        "Eager listing must configure no eviction policy");

    List<PartitionPath> partitions = new ArrayList<>();
    for (int i = 0; i < partitionCount; i++) {
      partitions.add(createPartition("dt=" + i));
    }
    fileIndex.getInputFileSlicesForTest(partitions.toArray(new PartitionPath[0]));
    Assertions.assertEquals(partitionCount, cachedFileSliceCount(fileIndex),
        "Eager listing must retain every partition it listed");
  }

  /**
   * The budget is denominated in bytes, so that it means the same thing for a table with one file group per partition
   * and for one with hundreds. This pins that the configured value reaches Caffeine as a weight budget, and that a
   * non-positive value means "unbounded".
   */
  @Test
  void testLazyListingAppliesTheConfiguredMemoryBudget() throws Exception {
    initMetaClient();
    long maxMemory = 64 * 1024L;
    TestLocalIndex bounded = newIndex(maxMemoryProps(maxMemory), true);
    Assertions.assertEquals(maxMemory, fileSliceCache(bounded).policy().eviction().get().getMaximum(),
        "The configured byte budget must be the cache's maximum weight");

    TestLocalIndex disabled = newIndex(maxMemoryProps(0L), true);
    Assertions.assertFalse(fileSliceCache(disabled).policy().eviction().isPresent(),
        "A non-positive budget must disable the bound entirely");
  }

  private static TypedProperties maxMemoryProps(long maxMemory) {
    TypedProperties properties = new TypedProperties();
    properties.put(HoodieCommonConfig.HOODIE_FILE_INDEX_MAX_MEMORY.key(), maxMemory);
    return properties;
  }

  private TestLocalIndex newIndex(TypedProperties properties, boolean shouldListLazily) {
    return new TestLocalIndex(
        new HoodieLocalEngineContext(getStorageConf()), metaClient, properties,
        HoodieTableQueryType.SNAPSHOT, Collections.emptyList(), Option.empty(), false, false, true,
        new BaseHoodieTableFileIndex.NoopCache(), shouldListLazily, Option.empty(), Option.empty());
  }

  private PartitionPath createPartition(String relativePartitionPath) throws IOException {
    Files.createDirectories(Paths.get(basePath, relativePartitionPath));
    return new PartitionPath(relativePartitionPath, new Object[] {relativePartitionPath});
  }

  private List<PartitionPath> createPartitionsWithBaseFiles(int partitionCount, int fileLength) throws Exception {
    String instantTime = "20260101000000001";
    HoodieTestTable testTable = HoodieTestTable.of(metaClient).addCommit(instantTime);
    List<PartitionPath> partitions = new ArrayList<>();
    for (int i = 0; i < partitionCount; i++) {
      String relativePartitionPath = "dt=" + i;
      testTable.withBaseFilesInPartition(relativePartitionPath, fileLength);
      partitions.add(new PartitionPath(relativePartitionPath, new Object[] {relativePartitionPath}));
    }
    metaClient = HoodieTableMetaClient.reload(metaClient);
    return partitions;
  }

  @SuppressWarnings("unchecked")
  private static Map<PartitionPath, List<FileSlice>> rawFileSliceCache(BaseHoodieTableFileIndex fileIndex)
      throws Exception {
    Field field = BaseHoodieTableFileIndex.class.getDeclaredField("cachedAllInputFileSlices");
    field.setAccessible(true);
    return (Map<PartitionPath, List<FileSlice>>) field.get(fileIndex);
  }

  @SuppressWarnings("unchecked")
  private static Cache<PartitionPath, List<FileSlice>> fileSliceCache(BaseHoodieTableFileIndex fileIndex)
      throws Exception {
    Field field = BaseHoodieTableFileIndex.class.getDeclaredField("fileSliceCache");
    field.setAccessible(true);
    return (Cache<PartitionPath, List<FileSlice>>) field.get(fileIndex);
  }

  private static long weightedSize(BaseHoodieTableFileIndex fileIndex) throws Exception {
    return fileSliceCache(fileIndex).policy().eviction().get().weightedSize().getAsLong();
  }

  private static int cachedFileSliceCount(BaseHoodieTableFileIndex fileIndex) throws Exception {
    return rawFileSliceCache(fileIndex).size();
  }

  /**
   * A {@link TestLocalIndex} that counts how many times the index went to storage for a listing.
   * {@code getPartitionPathFilter} is consulted exactly once per {@code getAllFilesInPartitions} call.
   */
  private static class ListingCountingIndex extends TestLocalIndex {
    volatile int listingCalls;

    ListingCountingIndex(HoodieEngineContext engineContext, HoodieTableMetaClient metaClient,
                         TypedProperties configProperties, HoodieTableQueryType queryType,
                         List<StoragePath> queryPaths, Option<String> specifiedQueryInstant,
                         boolean useLatestBaseFilesPathFilterForListing, boolean shouldIncludePendingCommits,
                         boolean shouldValidateInstant, FileStatusCache fileStatusCache, boolean shouldListLazily,
                         Option<String> startCompletionTime, Option<String> endCompletionTime) {
      super(engineContext, metaClient, configProperties, queryType, queryPaths, specifiedQueryInstant,
          useLatestBaseFilesPathFilterForListing, shouldIncludePendingCommits, shouldValidateInstant, fileStatusCache,
          shouldListLazily, startCompletionTime, endCompletionTime);
    }

    @Override
    protected Option<StoragePathFilter> getPartitionPathFilter(HoodieTimeline activeTimeline) {
      listingCalls++;
      return super.getPartitionPathFilter(activeTimeline);
    }
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

    long getTotalCachedFilesSizeForTest() {
      return getTotalCachedFilesSize();
    }

    void cleanUpFileSliceCacheForTest() {
      cleanUpFileSliceCache();
    }

    @Override
    protected Object[] parsePartitionColumnValues(String[] partitionColumns, String partitionPath) {
      return new Object[0];
    }
  }
}