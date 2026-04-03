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

package org.apache.hudi.io.storage;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.engine.EngineProperty;
import org.apache.hudi.common.model.EmptyHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.io.SeekableDataInputStream;
import org.apache.hudi.io.hfile.CachingHFileReaderImpl;
import org.apache.hudi.io.hfile.HFileReader;
import org.apache.hudi.io.hfile.HFileReaderCacheManager;
import org.apache.hudi.io.hfile.UTF8StringKey;
import org.apache.hudi.io.hadoop.TestHoodieOrcReaderWriter;
import org.apache.hudi.io.storage.hadoop.HoodieAvroHFileWriter;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathFilter;
import org.apache.hudi.storage.StoragePathInfo;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import com.github.benmanes.caffeine.cache.Cache;
import static org.apache.hudi.common.testutils.SchemaTestUtil.getSchemaFromResource;
import static org.apache.hudi.common.util.CollectionUtils.toStream;
import static org.apache.hudi.io.storage.HoodieAvroHFileReaderImplBase.KEY_BLOOM_FILTER_META_BLOCK;
import static org.apache.hudi.io.storage.HoodieAvroHFileReaderImplBase.SCHEMA_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
public class TestHoodieNativeAvroHFileReaderCaching {
  @TempDir
  public static Path tempDir;

  // Fixed seed for reproducible tests
  private static final Random RANDOM = new Random(42);
  private static final int KEYS_TO_LOOKUP = 10_000;
  private static final List<String> EXISTING_KEYS = new ArrayList<>();
  private static final List<String> MISSING_KEYS = new ArrayList<>();
  private static HoodieStorage storage;

  @BeforeEach
  public void resetCaches() {
    CachingHFileReaderImpl.resetGlobalCache();
  }

  @BeforeAll
  public static void setup() throws Exception {
    storage = HoodieTestUtils.getStorage(getFilePath());
    HoodieSchema avroSchema = getSchemaFromResource(
        TestHoodieOrcReaderWriter.class, "/exampleSchemaWithMetaFields.avsc");
    HoodieAvroHFileWriter writer = createWriter(avroSchema.toAvroSchema(), true);

    // Write records with for realistic testing
    final int numRecords = 50_000;
    log.debug("Creating HFile with {} records...", numRecords);

    for (int i = 0; i < numRecords; i++) {
      String key = String.format("key_%08d", i);
      EXISTING_KEYS.add(key);

      GenericRecord record = new GenericData.Record(avroSchema.toAvroSchema());
      record.put("_row_key", key);
      record.put("time", Integer.toString(RANDOM.nextInt()));
      record.put("number", i);

      writer.writeAvroWithMetadata(
          new HoodieAvroRecord(new HoodieKey(key, "partition_" + (i % 10)),
              new EmptyHoodieRecordPayload()).getKey(), record);
    }
    writer.close();

    // Generate missing keys that don't exist in the HFile
    for (int i = 0; i < KEYS_TO_LOOKUP; i++) {
      String missingKey = String.format("missing_key_%08d", i + numRecords);
      MISSING_KEYS.add(missingKey);
    }

    log.debug("HFile created with {} existing keys", EXISTING_KEYS.size());
    log.debug("Generated {} missing keys for testing", MISSING_KEYS.size());
  }

  @Test
  @Disabled("Enable this for local performance tests")
  public void testBlockCachePerformanceOnRecordLevelIndex() throws Exception {
    log.debug("\n=== HFile BlockCache Performance Test ===");

    // Test existing keys lookup performance
    testExistingKeysLookup();

    // Test missing keys lookup performance 
    testMissingKeysLookup();

    log.debug("================================================================\n");
  }

  @Test
  public void testMetadataInitializationDoesNotOpenStreamOnCacheHit() throws Exception {
    StorageAccessCounter counter = new StorageAccessCounter();
    HoodieStorage countingStorage = createCountingStorage(counter);
    HFileReaderFactory readerFactory = createCachingReaderFactory(countingStorage);

    try (HFileReader reader = readerFactory.createHFileReader()) {
      assertTrue(reader.getMetaInfo(new UTF8StringKey(SCHEMA_KEY)).isPresent());
    }
    assertTrue(counter.getOpenCount() > 0, "Initial metadata load should open the HFile stream");

    counter.reset();
    try (HFileReader reader = readerFactory.createHFileReader()) {
      assertTrue(reader.getMetaInfo(new UTF8StringKey(SCHEMA_KEY)).isPresent());
    }

    assertEquals(0, counter.getOpenCount(), "Metadata cache hit should not reopen the HFile stream");
    assertEquals(0, counter.getReadCount(), "Metadata cache hit should not read from the HFile stream");
  }

  @Test
  public void testInitializeMetadataDoesNotOpenStreamWhenLoadOnOpenBlocksCached() throws Exception {
    StorageAccessCounter counter = new StorageAccessCounter();
    HoodieStorage countingStorage = createCountingStorage(counter);
    HFileReaderFactory readerFactory = createCachingReaderFactory(countingStorage);

    try (HFileReader reader = readerFactory.createHFileReader()) {
      reader.initializeMetadata();
    }
    assertTrue(counter.getOpenCount() > 0, "Initial metadata initialization should open the HFile stream");

    counter.reset();
    try (HFileReader reader = readerFactory.createHFileReader()) {
      reader.initializeMetadata();
    }

    assertEquals(0, counter.getOpenCount(), "Cached load-on-open metadata should not reopen the HFile stream");
    assertEquals(0, counter.getReadCount(), "Cached load-on-open metadata should not read from the HFile stream");
  }

  @Test
  public void testDataBlockReadDoesNotOpenStreamOnFullCacheHit() throws Exception {
    StorageAccessCounter counter = new StorageAccessCounter();
    HoodieStorage countingStorage = createCountingStorage(counter);
    HFileReaderFactory readerFactory = createCachingReaderFactory(countingStorage);

    try (HFileReader reader = readerFactory.createHFileReader()) {
      assertTrue(reader.seekTo());
      assertTrue(reader.getKeyValue().isPresent());
    }
    assertTrue(counter.getOpenCount() > 0, "Initial data read should open the HFile stream");

    counter.reset();
    try (HFileReader reader = readerFactory.createHFileReader()) {
      assertTrue(reader.seekTo());
      assertTrue(reader.getKeyValue().isPresent());
    }

    assertEquals(0, counter.getOpenCount(), "Data block cache hit should not reopen the HFile stream");
    assertEquals(0, counter.getReadCount(), "Data block cache hit should not read from the HFile stream");
  }

  @Test
  public void testMetaBlockReadDoesNotOpenStreamOnCacheHit() throws Exception {
    StorageAccessCounter counter = new StorageAccessCounter();
    HoodieStorage countingStorage = createCountingStorage(counter);
    HFileReaderFactory readerFactory = createCachingReaderFactory(countingStorage);

    try (HFileReader reader = readerFactory.createHFileReader()) {
      assertTrue(reader.getMetaBlock(KEY_BLOOM_FILTER_META_BLOCK).isPresent());
    }
    assertTrue(counter.getOpenCount() > 0, "Initial meta block read should open the HFile stream");

    counter.reset();
    try (HFileReader reader = readerFactory.createHFileReader()) {
      assertTrue(reader.getMetaBlock(KEY_BLOOM_FILTER_META_BLOCK).isPresent());
    }

    assertEquals(0, counter.getOpenCount(), "Meta block cache hit should not reopen the HFile stream");
    assertEquals(0, counter.getReadCount(), "Meta block cache hit should not read from the HFile stream");
  }

  @Test
  public void testInitializeMetadataReloadsLoadOnOpenDataAfterLoadOnOpenCacheCleared() throws Exception {
    StorageAccessCounter counter = new StorageAccessCounter();
    HoodieStorage countingStorage = createCountingStorage(counter);
    HFileReaderFactory readerFactory = createCachingReaderFactory(countingStorage);

    try (HFileReader reader = readerFactory.createHFileReader()) {
      reader.initializeMetadata();
    }
    assertNotNull(getLoadOnOpenDataCacheEntry());
    invalidateLoadOnOpenDataCacheEntry();

    counter.reset();
    try (HFileReader reader = readerFactory.createHFileReader()) {
      reader.initializeMetadata();
    }

    assertTrue(counter.getOpenCount() > 0, "Clearing load-on-open cache should force stream reopen");
    assertTrue(counter.getReadCount() > 0, "Clearing load-on-open cache should force stream reads");
    assertNotNull(getLoadOnOpenDataCacheEntry());
  }

  @Test
  public void testReadersShareSingleCacheManagerInstance() throws Exception {
    HFileReaderFactory firstFactory = createCachingReaderFactory(storage);
    HFileReaderFactory secondFactory = createCachingReaderFactory(storage);

    try (HFileReader ignored = firstFactory.createHFileReader()) {
      HFileReaderCacheManager firstManager = getCacheManagerInstance();
      assertNotNull(firstManager, "Creating the first cached reader should initialize the cache manager");

      try (HFileReader alsoIgnored = secondFactory.createHFileReader()) {
        HFileReaderCacheManager secondManager = getCacheManagerInstance();
        assertTrue(firstManager == secondManager, "Cached readers should share the same cache manager instance");
      }
    }
  }

  private void testExistingKeysLookup() throws Exception {
    log.debug("\n--- Testing {} Existing Key Lookups ---", KEYS_TO_LOOKUP);

    // Select 10K random existing keys
    Collections.shuffle(EXISTING_KEYS, RANDOM);
    List<String> testKeys = EXISTING_KEYS.subList(0, KEYS_TO_LOOKUP);
    testKeys.sort(String::compareTo);

    // Warm up JVM
    warmupReads(testKeys.subList(0, 1000));

    // Test without cache
    long cacheTime = measureLookupTime(testKeys, true);
    long noCacheTime = measureLookupTime(testKeys, false);

    double speedup = (double) noCacheTime / cacheTime;

    log.debug("{} Existing Key Lookups:\n"
        + "  - Without BlockCache: {} ms\n"
        + "  - With BlockCache: {} ms\n"
        + "  - Speedup: {}x\n"
        + "  - Performance Improvement: {}%\n", KEYS_TO_LOOKUP, noCacheTime, cacheTime, speedup, (speedup - 1) * 100);

    assertTrue(speedup > 1.0, "BlockCache should provide speedup for existing key lookups");
  }

  private void testMissingKeysLookup() throws Exception {
    log.debug("\n--- Testing {} Missing Key Lookups ---", KEYS_TO_LOOKUP);

    // Use all 1k missing keys
    List<String> testKeys = new ArrayList<>(MISSING_KEYS);

    // Warm up JVM 
    warmupReads(testKeys.subList(0, 1000));

    // Test without cache
    long noCacheTime = measureLookupTime(testKeys, false, false);

    // Test with cache  
    long cacheTime = measureLookupTime(testKeys, true, false);

    double speedup = (double) noCacheTime / cacheTime;

    log.debug("{} Existing Key Lookups:\n"
        + "  - Without BlockCache: {} ms\n"
        + "  - With BlockCache: {} ms\n"
        + "  - Speedup: {}x\n"
        + "  - Performance Improvement: {}%\n", KEYS_TO_LOOKUP, noCacheTime, cacheTime, speedup, (speedup - 1) * 100);

    // Missing keys may not benefit as much from caching but should not be slower
    assertTrue(speedup >= 0.8, "BlockCache should not significantly slow down missing key lookups");
  }

  private void warmupReads(List<String> keys) throws Exception {
    // Warm up JVM to reduce noise in measurements
    try (HoodieAvroHFileReaderImplBase reader = createReader(storage, true, true)) {
      ClosableIterator<IndexedRecord> iter = reader.getIndexedRecordsByKeysIterator(keys, reader.getSchema());
      // Consume all records
      toStream(iter).forEach(record -> {
      });
    }
  }

  private long measureLookupTime(List<String> keys, boolean enableCache) throws Exception {
    return measureLookupTime(keys, enableCache, false);
  }

  private long measureLookupTime(List<String> keys, boolean enableCache, boolean useBloomFilter) throws Exception {
    // Force garbage collection before measurement
    System.gc();
    Thread.sleep(100); // Allow GC to complete

    long startTime = System.nanoTime();
    int totalRecordCount = 0;

    // Look up each key individually to test realistic usage pattern

    for (String key : keys) {
      try (HoodieAvroHFileReaderImplBase reader = createReader(storage, useBloomFilter, enableCache)) {
        List<String> singleKey = Collections.singletonList(key);
        ClosableIterator<IndexedRecord> iter = reader.getIndexedRecordsByKeysIterator(singleKey, reader.getSchema());

        // Count records for this key
        while (iter.hasNext()) {
          iter.next();
          totalRecordCount++;
        }
        iter.close();
      }
    }
    long endTime = System.nanoTime();

    // Validate we found the expected number of records
    boolean isExistingKeysTest = keys.size() <= EXISTING_KEYS.size()
        && EXISTING_KEYS.containsAll(keys);
    if (isExistingKeysTest) {
      assertTrue(totalRecordCount > 0, "Should find existing records");
    }

    return (endTime - startTime) / 1_000_000; // Convert to milliseconds
  }

  // Helper methods

  private static HoodieAvroHFileWriter createWriter(Schema avroSchema, boolean populateMetaFields) throws Exception {
    String instantTime = "000";
    HoodieStorage storage = HoodieTestUtils.getStorage(getFilePath());
    Properties props = new Properties();
    props.setProperty(HoodieTableConfig.POPULATE_META_FIELDS.key(), Boolean.toString(populateMetaFields));
    TaskContextSupplier taskContextSupplier = new FixedTaskContextSupplier();

    return (HoodieAvroHFileWriter) HoodieFileWriterFactory.getFileWriter(
        instantTime,
        getFilePath(),
        storage,
        HoodieStorageConfig.newBuilder().fromProperties(props).build(),
        HoodieSchema.fromAvroSchema(avroSchema),
        taskContextSupplier,
        HoodieRecord.HoodieRecordType.AVRO);
  }

  private static StoragePath getFilePath() {
    return new StoragePath(tempDir.toString() + "/perf_test.hfile");
  }

  private HoodieAvroHFileReaderImplBase createReader(HoodieStorage storage,
                                                     boolean useBloomFilter,
                                                     boolean enableCache) throws Exception {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieReaderConfig.HFILE_BLOCK_CACHE_ENABLED.key(), String.valueOf(enableCache));
    // Use a cache that can hold 100 blocks
    props.setProperty(HoodieReaderConfig.HFILE_BLOCK_CACHE_SIZE.key(), String.valueOf(100));

    HFileReaderFactory readerFactory = HFileReaderFactory.builder()
        .withStorage(storage)
        .withPath(getFilePath())
        .withProps(props)
        .build();
    return HoodieNativeAvroHFileReader.builder()
        .readerFactory(readerFactory).path(getFilePath()).useBloomFilter(useBloomFilter).build();
  }

  private HFileReaderFactory createCachingReaderFactory(HoodieStorage storage) {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieReaderConfig.HFILE_BLOCK_CACHE_ENABLED.key(), "true");
    props.setProperty(HoodieReaderConfig.HFILE_BLOCK_CACHE_SIZE.key(), "100");
    props.setProperty(HoodieReaderConfig.HFILE_INDEX_BLOCK_CACHE_SIZE.key(), "100");
    props.setProperty(HoodieMetadataConfig.METADATA_FILE_CACHE_MAX_SIZE_MB.key(), "0");

    return HFileReaderFactory.builder()
        .withStorage(storage)
        .withPath(getFilePath())
        .withProps(props)
        .build();
  }

  private HoodieStorage createCountingStorage(StorageAccessCounter counter) throws IOException {
    return new CountingHoodieStorage(storage, counter);
  }

  private static class StorageAccessCounter {
    private final AtomicInteger openCount = new AtomicInteger();
    private final AtomicInteger readCount = new AtomicInteger();

    private void recordOpen() {
      openCount.incrementAndGet();
    }

    private void recordRead() {
      readCount.incrementAndGet();
    }

    private int getOpenCount() {
      return openCount.get();
    }

    private int getReadCount() {
      return readCount.get();
    }

    private synchronized void reset() {
      openCount.set(0);
      readCount.set(0);
    }
  }

  private static class CountingSeekableDataInputStream extends SeekableDataInputStream {
    private final SeekableDataInputStream delegate;

    private CountingSeekableDataInputStream(SeekableDataInputStream delegate, StorageAccessCounter counter) {
      super(new CountingInputStream(delegate, counter));
      this.delegate = delegate;
    }

    @Override
    public long getPos() throws IOException {
      return delegate.getPos();
    }

    @Override
    public void seek(long pos) throws IOException {
      delegate.seek(pos);
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }
  }

  private static class CountingInputStream extends InputStream {
    private final SeekableDataInputStream delegate;
    private final StorageAccessCounter counter;

    private CountingInputStream(SeekableDataInputStream delegate, StorageAccessCounter counter) {
      this.delegate = delegate;
      this.counter = counter;
    }

    @Override
    public int read() throws IOException {
      int result = delegate.read();
      if (result >= 0) {
        counter.recordRead();
      }
      return result;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      int result = delegate.read(b, off, len);
      if (result > 0) {
        counter.recordRead();
      }
      return result;
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }
  }

  private static class FixedTaskContextSupplier extends TaskContextSupplier {
    private final Supplier<Integer> integerSupplier = () -> 10;
    private final Supplier<Long> longSupplier = () -> 0L;

    @Override
    public Supplier<Integer> getPartitionIdSupplier() {
      return integerSupplier;
    }

    @Override
    public Supplier<Integer> getStageIdSupplier() {
      return integerSupplier;
    }

    @Override
    public Supplier<Long> getAttemptIdSupplier() {
      return longSupplier;
    }

    @Override
    public Option<String> getProperty(EngineProperty prop) {
      return Option.empty();
    }

    @Override
    public Supplier<Integer> getTaskAttemptNumberSupplier() {
      return integerSupplier;
    }

    @Override
    public Supplier<Integer> getStageAttemptNumberSupplier() {
      return integerSupplier;
    }
  }

  private static class CountingHoodieStorage extends HoodieStorage {
    private final HoodieStorage delegate;
    private final StorageAccessCounter counter;

    private CountingHoodieStorage(HoodieStorage delegate, StorageAccessCounter counter) {
      super(delegate.getConf());
      this.delegate = delegate;
      this.counter = counter;
    }

    @Override
    public HoodieStorage newInstance(StoragePath path, org.apache.hudi.storage.StorageConfiguration<?> storageConf) {
      return delegate.newInstance(path, storageConf);
    }

    @Override
    public String getScheme() {
      return delegate.getScheme();
    }

    @Override
    public int getDefaultBlockSize(StoragePath path) {
      return delegate.getDefaultBlockSize(path);
    }

    @Override
    public int getDefaultBufferSize() {
      return delegate.getDefaultBufferSize();
    }

    @Override
    public short getDefaultReplication(StoragePath path) {
      return delegate.getDefaultReplication(path);
    }

    @Override
    public URI getUri() {
      return delegate.getUri();
    }

    @Override
    public OutputStream create(StoragePath path, boolean overwrite) throws IOException {
      return delegate.create(path, overwrite);
    }

    @Override
    public OutputStream create(StoragePath path,
                               boolean overwrite,
                               Integer bufferSize,
                               Short replication,
                               Long sizeThreshold) throws IOException {
      return delegate.create(path, overwrite, bufferSize, replication, sizeThreshold);
    }

    @Override
    public InputStream open(StoragePath path) throws IOException {
      return delegate.open(path);
    }

    @Override
    public SeekableDataInputStream openSeekable(StoragePath path,
                                                int bufferSize,
                                                boolean wrapStream) throws IOException {
      counter.recordOpen();
      return new CountingSeekableDataInputStream(delegate.openSeekable(path, bufferSize, wrapStream), counter);
    }

    @Override
    public OutputStream append(StoragePath path) throws IOException {
      return delegate.append(path);
    }

    @Override
    public boolean exists(StoragePath path) throws IOException {
      return delegate.exists(path);
    }

    @Override
    public StoragePathInfo getPathInfo(StoragePath path) throws IOException {
      return delegate.getPathInfo(path);
    }

    @Override
    public boolean createDirectory(StoragePath path) throws IOException {
      return delegate.createDirectory(path);
    }

    @Override
    public List<StoragePathInfo> listDirectEntries(StoragePath path) throws IOException {
      return delegate.listDirectEntries(path);
    }

    @Override
    public List<StoragePathInfo> listFiles(StoragePath path) throws IOException {
      return delegate.listFiles(path);
    }

    @Override
    public List<StoragePathInfo> listDirectEntries(StoragePath path, StoragePathFilter filter) throws IOException {
      return delegate.listDirectEntries(path, filter);
    }

    @Override
    public void setModificationTime(StoragePath path, long modificationTimeInMillisEpoch) throws IOException {
      delegate.setModificationTime(path, modificationTimeInMillisEpoch);
    }

    @Override
    public List<StoragePathInfo> globEntries(StoragePath pathPattern, StoragePathFilter filter) throws IOException {
      return delegate.globEntries(pathPattern, filter);
    }

    @Override
    public boolean rename(StoragePath oldPath, StoragePath newPath) throws IOException {
      return delegate.rename(oldPath, newPath);
    }

    @Override
    public boolean deleteDirectory(StoragePath path) throws IOException {
      return delegate.deleteDirectory(path);
    }

    @Override
    public boolean deleteFile(StoragePath path) throws IOException {
      return delegate.deleteFile(path);
    }

    @Override
    public Object getFileSystem() {
      return delegate.getFileSystem();
    }

    @Override
    public HoodieStorage getRawStorage() {
      return delegate.getRawStorage();
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }
  }

  @SuppressWarnings("unchecked")
  private Object getLoadOnOpenDataCacheEntry() throws Exception {
    Field cacheField = HFileReaderCacheManager.class.getDeclaredField("INSTANCE");
    cacheField.setAccessible(true);
    HFileReaderCacheManager manager = (HFileReaderCacheManager) cacheField.get(null);
    if (manager == null) {
      return null;
    }
    Field loadOnOpenCacheField = HFileReaderCacheManager.class.getDeclaredField("loadOnOpenDataCache");
    loadOnOpenCacheField.setAccessible(true);
    Cache<String, Object> cache = (Cache<String, Object>) loadOnOpenCacheField.get(manager);
    return cache == null ? null : cache.getIfPresent(getFilePath().toString());
  }

  @SuppressWarnings("unchecked")
  private void invalidateLoadOnOpenDataCacheEntry() throws Exception {
    Field cacheField = HFileReaderCacheManager.class.getDeclaredField("INSTANCE");
    cacheField.setAccessible(true);
    HFileReaderCacheManager manager = (HFileReaderCacheManager) cacheField.get(null);
    if (manager == null) {
      return;
    }
    Field loadOnOpenCacheField = HFileReaderCacheManager.class.getDeclaredField("loadOnOpenDataCache");
    loadOnOpenCacheField.setAccessible(true);
    Cache<String, Object> cache = (Cache<String, Object>) loadOnOpenCacheField.get(manager);
    if (cache != null) {
      cache.invalidate(getFilePath().toString());
    }
  }

  private HFileReaderCacheManager getCacheManagerInstance() throws Exception {
    Field instanceField = HFileReaderCacheManager.class.getDeclaredField("INSTANCE");
    instanceField.setAccessible(true);
    return (HFileReaderCacheManager) instanceField.get(null);
  }
}
