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

package org.apache.hudi.hbase.io.hfile.bucket;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.hbase.HBaseConfiguration;
import org.apache.hudi.hbase.TableName;
import org.apache.hudi.hbase.io.ByteBuffAllocator;
import org.apache.hudi.hbase.io.ByteBuffAllocator.Recycler;
import org.apache.hudi.hbase.io.HeapSize;
import org.apache.hudi.hbase.io.hfile.BlockCache;
import org.apache.hudi.hbase.io.hfile.BlockCacheKey;
import org.apache.hudi.hbase.io.hfile.BlockCacheUtil;
import org.apache.hudi.hbase.io.hfile.BlockPriority;
import org.apache.hudi.hbase.io.hfile.BlockType;
import org.apache.hudi.hbase.io.hfile.CacheStats;
import org.apache.hudi.hbase.io.hfile.Cacheable;
import org.apache.hudi.hbase.io.hfile.CachedBlock;
import org.apache.hudi.hbase.io.hfile.HFileBlock;
import org.apache.hudi.hbase.io.hfile.HFileContext;
import org.apache.hudi.hbase.nio.ByteBuff;
import org.apache.hudi.hbase.nio.RefCnt;
import org.apache.hudi.hbase.protobuf.ProtobufMagic;
import org.apache.hudi.hbase.util.Bytes;
import org.apache.hudi.hbase.util.EnvironmentEdgeManager;
import org.apache.hudi.hbase.util.IdReadWriteLock;
import org.apache.hudi.hbase.util.IdReadWriteLock.ReferenceType;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.hudi.hbase.shaded.protobuf.generated.BucketCacheProtos;

/**
 * BucketCache uses {@link BucketAllocator} to allocate/free blocks, and uses
 * BucketCache#ramCache and BucketCache#backingMap in order to
 * determine if a given element is in the cache. The bucket cache can use on-heap or
 * off-heap memory {@link ByteBufferIOEngine} or in a file {@link FileIOEngine} to
 * store/read the block data.
 *
 * <p>Eviction is via a similar algorithm as used in
 * {@link org.apache.hudi.hbase.io.hfile.LruBlockCache}
 *
 * <p>BucketCache can be used as mainly a block cache (see
 * {@link org.apache.hudi.hbase.io.hfile.CombinedBlockCache}), combined with
 * a BlockCache to decrease CMS GC and heap fragmentation.
 *
 * <p>It also can be used as a secondary cache (e.g. using a file on ssd/fusionio to store
 * blocks) to enlarge cache space via a victim cache.
 */
@InterfaceAudience.Private
public class BucketCache implements BlockCache, HeapSize {
  private static final Logger LOG = LoggerFactory.getLogger(BucketCache.class);

  /** Priority buckets config */
  static final String SINGLE_FACTOR_CONFIG_NAME = "hbase.bucketcache.single.factor";
  static final String MULTI_FACTOR_CONFIG_NAME = "hbase.bucketcache.multi.factor";
  static final String MEMORY_FACTOR_CONFIG_NAME = "hbase.bucketcache.memory.factor";
  static final String EXTRA_FREE_FACTOR_CONFIG_NAME = "hbase.bucketcache.extrafreefactor";
  static final String ACCEPT_FACTOR_CONFIG_NAME = "hbase.bucketcache.acceptfactor";
  static final String MIN_FACTOR_CONFIG_NAME = "hbase.bucketcache.minfactor";

  /** Priority buckets */
  static final float DEFAULT_SINGLE_FACTOR = 0.25f;
  static final float DEFAULT_MULTI_FACTOR = 0.50f;
  static final float DEFAULT_MEMORY_FACTOR = 0.25f;
  static final float DEFAULT_MIN_FACTOR = 0.85f;

  private static final float DEFAULT_EXTRA_FREE_FACTOR = 0.10f;
  private static final float DEFAULT_ACCEPT_FACTOR = 0.95f;

  // Number of blocks to clear for each of the bucket size that is full
  private static final int DEFAULT_FREE_ENTIRE_BLOCK_FACTOR = 2;

  /** Statistics thread */
  private static final int statThreadPeriod = 5 * 60;

  final static int DEFAULT_WRITER_THREADS = 3;
  final static int DEFAULT_WRITER_QUEUE_ITEMS = 64;

  // Store/read block data
  transient final IOEngine ioEngine;

  // Store the block in this map before writing it to cache
  transient final RAMCache ramCache;
  // In this map, store the block's meta data like offset, length
  transient ConcurrentHashMap<BlockCacheKey, BucketEntry> backingMap;

  /**
   * Flag if the cache is enabled or not... We shut it off if there are IO
   * errors for some time, so that Bucket IO exceptions/errors don't bring down
   * the HBase server.
   */
  private volatile boolean cacheEnabled;

  /**
   * A list of writer queues.  We have a queue per {@link WriterThread} we have running.
   * In other words, the work adding blocks to the BucketCache is divided up amongst the
   * running WriterThreads.  Its done by taking hash of the cache key modulo queue count.
   * WriterThread when it runs takes whatever has been recently added and 'drains' the entries
   * to the BucketCache.  It then updates the ramCache and backingMap accordingly.
   */
  transient final ArrayList<BlockingQueue<RAMQueueEntry>> writerQueues = new ArrayList<>();
  transient final WriterThread[] writerThreads;

  /** Volatile boolean to track if free space is in process or not */
  private volatile boolean freeInProgress = false;
  private transient final Lock freeSpaceLock = new ReentrantLock();

  private final LongAdder realCacheSize = new LongAdder();
  private final LongAdder heapSize = new LongAdder();
  /** Current number of cached elements */
  private final LongAdder blockNumber = new LongAdder();

  /** Cache access count (sequential ID) */
  private final AtomicLong accessCount = new AtomicLong();

  private static final int DEFAULT_CACHE_WAIT_TIME = 50;

  /**
   * Used in tests. If this flag is false and the cache speed is very fast,
   * bucket cache will skip some blocks when caching. If the flag is true, we
   * will wait until blocks are flushed to IOEngine.
   */
  boolean wait_when_cache = false;

  private final BucketCacheStats cacheStats = new BucketCacheStats();

  private final String persistencePath;
  private final long cacheCapacity;
  /** Approximate block size */
  private final long blockSize;

  /** Duration of IO errors tolerated before we disable cache, 1 min as default */
  private final int ioErrorsTolerationDuration;
  // 1 min
  public static final int DEFAULT_ERROR_TOLERATION_DURATION = 60 * 1000;

  // Start time of first IO error when reading or writing IO Engine, it will be
  // reset after a successful read/write.
  private volatile long ioErrorStartTime = -1;

  /**
   * A ReentrantReadWriteLock to lock on a particular block identified by offset.
   * The purpose of this is to avoid freeing the block which is being read.
   * <p>
   * Key set of offsets in BucketCache is limited so soft reference is the best choice here.
   */
  transient final IdReadWriteLock<Long> offsetLock = new IdReadWriteLock<>(ReferenceType.SOFT);

  private final NavigableSet<BlockCacheKey> blocksByHFile = new ConcurrentSkipListSet<>((a, b) -> {
    int nameComparison = a.getHfileName().compareTo(b.getHfileName());
    if (nameComparison != 0) {
      return nameComparison;
    }
    return Long.compare(a.getOffset(), b.getOffset());
  });

  /** Statistics thread schedule pool (for heavy debugging, could remove) */
  private transient final ScheduledExecutorService scheduleThreadPool =
      Executors.newScheduledThreadPool(1,
          new ThreadFactoryBuilder().setNameFormat("BucketCacheStatsExecutor").setDaemon(true).build());

  // Allocate or free space for the block
  private transient BucketAllocator bucketAllocator;

  /** Acceptable size of cache (no evictions if size < acceptable) */
  private float acceptableFactor;

  /** Minimum threshold of cache (when evicting, evict until size < min) */
  private float minFactor;

  /** Free this floating point factor of extra blocks when evicting. For example free the number of blocks requested * (1 + extraFreeFactor) */
  private float extraFreeFactor;

  /** Single access bucket size */
  private float singleFactor;

  /** Multiple access bucket size */
  private float multiFactor;

  /** In-memory bucket size */
  private float memoryFactor;

  private static final String FILE_VERIFY_ALGORITHM =
      "hbase.bucketcache.persistent.file.integrity.check.algorithm";
  private static final String DEFAULT_FILE_VERIFY_ALGORITHM = "MD5";

  /**
   * Use {@link java.security.MessageDigest} class's encryption algorithms to check
   * persistent file integrity, default algorithm is MD5
   * */
  private String algorithm;

  /* Tracing failed Bucket Cache allocations. */
  private long allocFailLogPrevTs; // time of previous log event for allocation failure.
  private static final int ALLOCATION_FAIL_LOG_TIME_PERIOD = 60000; // Default 1 minute.

  public BucketCache(String ioEngineName, long capacity, int blockSize, int[] bucketSizes,
                     int writerThreadNum, int writerQLen, String persistencePath) throws IOException {
    this(ioEngineName, capacity, blockSize, bucketSizes, writerThreadNum, writerQLen,
        persistencePath, DEFAULT_ERROR_TOLERATION_DURATION, HBaseConfiguration.create());
  }

  public BucketCache(String ioEngineName, long capacity, int blockSize, int[] bucketSizes,
                     int writerThreadNum, int writerQLen, String persistencePath, int ioErrorsTolerationDuration,
                     Configuration conf) throws IOException {
    this.algorithm = conf.get(FILE_VERIFY_ALGORITHM, DEFAULT_FILE_VERIFY_ALGORITHM);
    this.ioEngine = getIOEngineFromName(ioEngineName, capacity, persistencePath);
    this.writerThreads = new WriterThread[writerThreadNum];
    long blockNumCapacity = capacity / blockSize;
    if (blockNumCapacity >= Integer.MAX_VALUE) {
      // Enough for about 32TB of cache!
      throw new IllegalArgumentException("Cache capacity is too large, only support 32TB now");
    }

    this.acceptableFactor = conf.getFloat(ACCEPT_FACTOR_CONFIG_NAME, DEFAULT_ACCEPT_FACTOR);
    this.minFactor = conf.getFloat(MIN_FACTOR_CONFIG_NAME, DEFAULT_MIN_FACTOR);
    this.extraFreeFactor = conf.getFloat(EXTRA_FREE_FACTOR_CONFIG_NAME, DEFAULT_EXTRA_FREE_FACTOR);
    this.singleFactor = conf.getFloat(SINGLE_FACTOR_CONFIG_NAME, DEFAULT_SINGLE_FACTOR);
    this.multiFactor = conf.getFloat(MULTI_FACTOR_CONFIG_NAME, DEFAULT_MULTI_FACTOR);
    this.memoryFactor = conf.getFloat(MEMORY_FACTOR_CONFIG_NAME, DEFAULT_MEMORY_FACTOR);

    sanityCheckConfigs();

    LOG.info("Instantiating BucketCache with acceptableFactor: " + acceptableFactor + ", minFactor: " + minFactor +
        ", extraFreeFactor: " + extraFreeFactor + ", singleFactor: " + singleFactor + ", multiFactor: " + multiFactor +
        ", memoryFactor: " + memoryFactor);

    this.cacheCapacity = capacity;
    this.persistencePath = persistencePath;
    this.blockSize = blockSize;
    this.ioErrorsTolerationDuration = ioErrorsTolerationDuration;

    this.allocFailLogPrevTs = 0;

    bucketAllocator = new BucketAllocator(capacity, bucketSizes);
    for (int i = 0; i < writerThreads.length; ++i) {
      writerQueues.add(new ArrayBlockingQueue<>(writerQLen));
    }

    assert writerQueues.size() == writerThreads.length;
    this.ramCache = new RAMCache();

    this.backingMap = new ConcurrentHashMap<>((int) blockNumCapacity);

    if (ioEngine.isPersistent() && persistencePath != null) {
      try {
        retrieveFromFile(bucketSizes);
      } catch (IOException ioex) {
        LOG.error("Can't restore from file[" + persistencePath + "] because of ", ioex);
      }
    }
    final String threadName = Thread.currentThread().getName();
    this.cacheEnabled = true;
    for (int i = 0; i < writerThreads.length; ++i) {
      writerThreads[i] = new WriterThread(writerQueues.get(i));
      writerThreads[i].setName(threadName + "-BucketCacheWriter-" + i);
      writerThreads[i].setDaemon(true);
    }
    startWriterThreads();

    // Run the statistics thread periodically to print the cache statistics log
    // TODO: Add means of turning this off.  Bit obnoxious running thread just to make a log
    // every five minutes.
    this.scheduleThreadPool.scheduleAtFixedRate(new StatisticsThread(this),
        statThreadPeriod, statThreadPeriod, TimeUnit.SECONDS);
    LOG.info("Started bucket cache; ioengine=" + ioEngineName +
        ", capacity=" + StringUtils.byteDesc(capacity) +
        ", blockSize=" + StringUtils.byteDesc(blockSize) + ", writerThreadNum=" +
        writerThreadNum + ", writerQLen=" + writerQLen + ", persistencePath=" +
        persistencePath + ", bucketAllocator=" + this.bucketAllocator.getClass().getName());
  }

  private void sanityCheckConfigs() {
    Preconditions.checkArgument(acceptableFactor <= 1 && acceptableFactor >= 0, ACCEPT_FACTOR_CONFIG_NAME + " must be between 0.0 and 1.0");
    Preconditions.checkArgument(minFactor <= 1 && minFactor >= 0, MIN_FACTOR_CONFIG_NAME + " must be between 0.0 and 1.0");
    Preconditions.checkArgument(minFactor <= acceptableFactor, MIN_FACTOR_CONFIG_NAME + " must be <= " + ACCEPT_FACTOR_CONFIG_NAME);
    Preconditions.checkArgument(extraFreeFactor >= 0, EXTRA_FREE_FACTOR_CONFIG_NAME + " must be greater than 0.0");
    Preconditions.checkArgument(singleFactor <= 1 && singleFactor >= 0, SINGLE_FACTOR_CONFIG_NAME + " must be between 0.0 and 1.0");
    Preconditions.checkArgument(multiFactor <= 1 && multiFactor >= 0, MULTI_FACTOR_CONFIG_NAME + " must be between 0.0 and 1.0");
    Preconditions.checkArgument(memoryFactor <= 1 && memoryFactor >= 0, MEMORY_FACTOR_CONFIG_NAME + " must be between 0.0 and 1.0");
    Preconditions.checkArgument((singleFactor + multiFactor + memoryFactor) == 1, SINGLE_FACTOR_CONFIG_NAME + ", " +
        MULTI_FACTOR_CONFIG_NAME + ", and " + MEMORY_FACTOR_CONFIG_NAME + " segments must add up to 1.0");
  }

  /**
   * Called by the constructor to start the writer threads. Used by tests that need to override
   * starting the threads.
   */
  protected void startWriterThreads() {
    for (WriterThread thread : writerThreads) {
      thread.start();
    }
  }

  boolean isCacheEnabled() {
    return this.cacheEnabled;
  }

  @Override
  public long getMaxSize() {
    return this.cacheCapacity;
  }

  public String getIoEngine() {
    return ioEngine.toString();
  }

  /**
   * Get the IOEngine from the IO engine name
   * @param ioEngineName
   * @param capacity
   * @param persistencePath
   * @return the IOEngine
   * @throws IOException
   */
  private IOEngine getIOEngineFromName(String ioEngineName, long capacity, String persistencePath)
      throws IOException {
    if (ioEngineName.startsWith("file:") || ioEngineName.startsWith("files:")) {
      // In order to make the usage simple, we only need the prefix 'files:' in
      // document whether one or multiple file(s), but also support 'file:' for
      // the compatibility
      String[] filePaths = ioEngineName.substring(ioEngineName.indexOf(":") + 1)
          .split(FileIOEngine.FILE_DELIMITER);
      return new FileIOEngine(capacity, persistencePath != null, filePaths);
    } else if (ioEngineName.startsWith("offheap")) {
      return new ByteBufferIOEngine(capacity);
    } else if (ioEngineName.startsWith("mmap:")) {
      return new ExclusiveMemoryMmapIOEngine(ioEngineName.substring(5), capacity);
    } else if (ioEngineName.startsWith("pmem:")) {
      // This mode of bucket cache creates an IOEngine over a file on the persistent memory
      // device. Since the persistent memory device has its own address space the contents
      // mapped to this address space does not get swapped out like in the case of mmapping
      // on to DRAM. Hence the cells created out of the hfile blocks in the pmem bucket cache
      // can be directly referred to without having to copy them onheap. Once the RPC is done,
      // the blocks can be returned back as in case of ByteBufferIOEngine.
      return new SharedMemoryMmapIOEngine(ioEngineName.substring(5), capacity);
    } else {
      throw new IllegalArgumentException(
          "Don't understand io engine name for cache- prefix with file:, files:, mmap: or offheap");
    }
  }

  /**
   * Cache the block with the specified name and buffer.
   * @param cacheKey block's cache key
   * @param buf block buffer
   */
  @Override
  public void cacheBlock(BlockCacheKey cacheKey, Cacheable buf) {
    cacheBlock(cacheKey, buf, false);
  }

  /**
   * Cache the block with the specified name and buffer.
   * @param cacheKey block's cache key
   * @param cachedItem block buffer
   * @param inMemory if block is in-memory
   */
  @Override
  public void cacheBlock(BlockCacheKey cacheKey, Cacheable cachedItem, boolean inMemory) {
    cacheBlockWithWait(cacheKey, cachedItem, inMemory, wait_when_cache);
  }

  /**
   * Cache the block to ramCache
   * @param cacheKey block's cache key
   * @param cachedItem block buffer
   * @param inMemory if block is in-memory
   * @param wait if true, blocking wait when queue is full
   */
  public void cacheBlockWithWait(BlockCacheKey cacheKey, Cacheable cachedItem, boolean inMemory,
                                 boolean wait) {
    if (cacheEnabled) {
      if (backingMap.containsKey(cacheKey) || ramCache.containsKey(cacheKey)) {
        if (shouldReplaceExistingCacheBlock(cacheKey, cachedItem)) {
          BucketEntry bucketEntry = backingMap.get(cacheKey);
          if (bucketEntry != null && bucketEntry.isRpcRef()) {
            // avoid replace when there are RPC refs for the bucket entry in bucket cache
            return;
          }
          cacheBlockWithWaitInternal(cacheKey, cachedItem, inMemory, wait);
        }
      } else {
        cacheBlockWithWaitInternal(cacheKey, cachedItem, inMemory, wait);
      }
    }
  }

  protected boolean shouldReplaceExistingCacheBlock(BlockCacheKey cacheKey, Cacheable newBlock) {
    return BlockCacheUtil.shouldReplaceExistingCacheBlock(this, cacheKey, newBlock);
  }

  protected void cacheBlockWithWaitInternal(BlockCacheKey cacheKey, Cacheable cachedItem,
                                            boolean inMemory, boolean wait) {
    if (!cacheEnabled) {
      return;
    }
    LOG.trace("Caching key={}, item={}", cacheKey, cachedItem);
    // Stuff the entry into the RAM cache so it can get drained to the persistent store
    RAMQueueEntry re =
        new RAMQueueEntry(cacheKey, cachedItem, accessCount.incrementAndGet(), inMemory);
    /**
     * Don't use ramCache.put(cacheKey, re) here. because there may be a existing entry with same
     * key in ramCache, the heap size of bucket cache need to update if replacing entry from
     * ramCache. But WriterThread will also remove entry from ramCache and update heap size, if
     * using ramCache.put(), It's possible that the removed entry in WriterThread is not the correct
     * one, then the heap size will mess up (HBASE-20789)
     */
    if (ramCache.putIfAbsent(cacheKey, re) != null) {
      return;
    }
    int queueNum = (cacheKey.hashCode() & 0x7FFFFFFF) % writerQueues.size();
    BlockingQueue<RAMQueueEntry> bq = writerQueues.get(queueNum);
    boolean successfulAddition = false;
    if (wait) {
      try {
        successfulAddition = bq.offer(re, DEFAULT_CACHE_WAIT_TIME, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    } else {
      successfulAddition = bq.offer(re);
    }
    if (!successfulAddition) {
      ramCache.remove(cacheKey);
      cacheStats.failInsert();
    } else {
      this.blockNumber.increment();
      this.heapSize.add(cachedItem.heapSize());
      blocksByHFile.add(cacheKey);
    }
  }

  /**
   * Get the buffer of the block with the specified key.
   * @param key block's cache key
   * @param caching true if the caller caches blocks on cache misses
   * @param repeat Whether this is a repeat lookup for the same block
   * @param updateCacheMetrics Whether we should update cache metrics or not
   * @return buffer of specified cache key, or null if not in cache
   */
  @Override
  public Cacheable getBlock(BlockCacheKey key, boolean caching, boolean repeat,
                            boolean updateCacheMetrics) {
    if (!cacheEnabled) {
      return null;
    }
    RAMQueueEntry re = ramCache.get(key);
    if (re != null) {
      if (updateCacheMetrics) {
        cacheStats.hit(caching, key.isPrimary(), key.getBlockType());
      }
      re.access(accessCount.incrementAndGet());
      return re.getData();
    }
    BucketEntry bucketEntry = backingMap.get(key);
    if (bucketEntry != null) {
      long start = System.nanoTime();
      ReentrantReadWriteLock lock = offsetLock.getLock(bucketEntry.offset());
      try {
        lock.readLock().lock();
        // We can not read here even if backingMap does contain the given key because its offset
        // maybe changed. If we lock BlockCacheKey instead of offset, then we can only check
        // existence here.
        if (bucketEntry.equals(backingMap.get(key))) {
          // Read the block from IOEngine based on the bucketEntry's offset and length, NOTICE: the
          // block will use the refCnt of bucketEntry, which means if two HFileBlock mapping to
          // the same BucketEntry, then all of the three will share the same refCnt.
          Cacheable cachedBlock = ioEngine.read(bucketEntry);
          if (ioEngine.usesSharedMemory()) {
            // If IOEngine use shared memory, cachedBlock and BucketEntry will share the
            // same RefCnt, do retain here, in order to count the number of RPC references
            cachedBlock.retain();
          }
          // Update the cache statistics.
          if (updateCacheMetrics) {
            cacheStats.hit(caching, key.isPrimary(), key.getBlockType());
            cacheStats.ioHit(System.nanoTime() - start);
          }
          bucketEntry.access(accessCount.incrementAndGet());
          if (this.ioErrorStartTime > 0) {
            ioErrorStartTime = -1;
          }
          return cachedBlock;
        }
      } catch (IOException ioex) {
        LOG.error("Failed reading block " + key + " from bucket cache", ioex);
        checkIOErrorIsTolerated();
      } finally {
        lock.readLock().unlock();
      }
    }
    if (!repeat && updateCacheMetrics) {
      cacheStats.miss(caching, key.isPrimary(), key.getBlockType());
    }
    return null;
  }

  /**
   * This method is invoked after the bucketEntry is removed from {@link BucketCache#backingMap}
   */
  void blockEvicted(BlockCacheKey cacheKey, BucketEntry bucketEntry, boolean decrementBlockNumber) {
    bucketEntry.markAsEvicted();
    blocksByHFile.remove(cacheKey);
    if (decrementBlockNumber) {
      this.blockNumber.decrement();
    }
    cacheStats.evicted(bucketEntry.getCachedTime(), cacheKey.isPrimary());
  }

  /**
   * Free the {{@link BucketEntry} actually,which could only be invoked when the
   * {@link BucketEntry#refCnt} becoming 0.
   */
  void freeBucketEntry(BucketEntry bucketEntry) {
    bucketAllocator.freeBlock(bucketEntry.offset());
    realCacheSize.add(-1 * bucketEntry.getLength());
  }

  /**
   * Try to evict the block from {@link BlockCache} by force. We'll call this in few cases:<br>
   * 1. Close an HFile, and clear all cached blocks. <br>
   * 2. Call {@link Admin#clearBlockCache(TableName)} to clear all blocks for a given table.<br>
   * <p>
   * Firstly, we'll try to remove the block from RAMCache,and then try to evict from backingMap.
   * Here we evict the block from backingMap immediately, but only free the reference from bucket
   * cache by calling {@link BucketEntry#markedAsEvicted}. If there're still some RPC referring this
   * block, block can only be de-allocated when all of them release the block.
   * <p>
   * NOTICE: we need to grab the write offset lock firstly before releasing the reference from
   * bucket cache. if we don't, we may read an {@link BucketEntry} with refCnt = 0 when
   * {@link BucketCache#getBlock(BlockCacheKey, boolean, boolean, boolean)}, it's a memory leak.
   * @param cacheKey Block to evict
   * @return true to indicate whether we've evicted successfully or not.
   */
  @Override
  public boolean evictBlock(BlockCacheKey cacheKey) {
    return doEvictBlock(cacheKey, null);
  }

  /**
   * Evict the {@link BlockCacheKey} and {@link BucketEntry} from {@link BucketCache#backingMap} and
   * {@link BucketCache#ramCache}. <br/>
   * NOTE:When Evict from {@link BucketCache#backingMap},only the matched {@link BlockCacheKey} and
   * {@link BucketEntry} could be removed.
   * @param cacheKey {@link BlockCacheKey} to evict.
   * @param bucketEntry {@link BucketEntry} matched {@link BlockCacheKey} to evict.
   * @return true to indicate whether we've evicted successfully or not.
   */
  private boolean doEvictBlock(BlockCacheKey cacheKey, BucketEntry bucketEntry) {
    if (!cacheEnabled) {
      return false;
    }
    boolean existedInRamCache = removeFromRamCache(cacheKey);
    if (bucketEntry == null) {
      bucketEntry = backingMap.get(cacheKey);
    }
    final BucketEntry bucketEntryToUse = bucketEntry;

    if (bucketEntryToUse == null) {
      if (existedInRamCache) {
        cacheStats.evicted(0, cacheKey.isPrimary());
      }
      return existedInRamCache;
    } else {
      return bucketEntryToUse.withWriteLock(offsetLock, () -> {
        if (backingMap.remove(cacheKey, bucketEntryToUse)) {
          blockEvicted(cacheKey, bucketEntryToUse, !existedInRamCache);
          return true;
        }
        return false;
      });
    }
  }

  /**
   * <pre>
   * Create the {@link Recycler} for {@link BucketEntry#refCnt},which would be used as
   * {@link RefCnt#recycler} of {@link HFileBlock#buf} returned from {@link BucketCache#getBlock}.
   * NOTE: for {@link BucketCache#getBlock},the {@link RefCnt#recycler} of {@link HFileBlock#buf}
   * from {@link BucketCache#backingMap} and {@link BucketCache#ramCache} are different:
   * 1.For {@link RefCnt#recycler} of {@link HFileBlock#buf} from {@link BucketCache#backingMap},
   *   it is the return value of current {@link BucketCache#createRecycler} method.
   *
   * 2.For {@link RefCnt#recycler} of {@link HFileBlock#buf} from {@link BucketCache#ramCache},
   *   it is {@link ByteBuffAllocator#putbackBuffer}.
   * </pre>
   */
  private Recycler createRecycler(final BucketEntry bucketEntry) {
    return () -> {
      freeBucketEntry(bucketEntry);
      return;
    };
  }

  /**
   * NOTE: This method is only for test.
   */
  public boolean evictBlockIfNoRpcReferenced(BlockCacheKey blockCacheKey) {
    BucketEntry bucketEntry = backingMap.get(blockCacheKey);
    if (bucketEntry == null) {
      return false;
    }
    return evictBucketEntryIfNoRpcReferenced(blockCacheKey, bucketEntry);
  }

  /**
   * Evict {@link BlockCacheKey} and its corresponding {@link BucketEntry} only if
   * {@link BucketEntry#isRpcRef} is false. <br/>
   * NOTE:When evict from {@link BucketCache#backingMap},only the matched {@link BlockCacheKey} and
   * {@link BucketEntry} could be removed.
   * @param blockCacheKey {@link BlockCacheKey} to evict.
   * @param bucketEntry {@link BucketEntry} matched {@link BlockCacheKey} to evict.
   * @return true to indicate whether we've evicted successfully or not.
   */
  boolean evictBucketEntryIfNoRpcReferenced(BlockCacheKey blockCacheKey, BucketEntry bucketEntry) {
    if (!bucketEntry.isRpcRef()) {
      return doEvictBlock(blockCacheKey, bucketEntry);
    }
    return false;
  }

  protected boolean removeFromRamCache(BlockCacheKey cacheKey) {
    return ramCache.remove(cacheKey, re -> {
      if (re != null) {
        this.blockNumber.decrement();
        this.heapSize.add(-1 * re.getData().heapSize());
      }
    });
  }

  /*
   * Statistics thread.  Periodically output cache statistics to the log.
   */
  private static class StatisticsThread extends Thread {
    private final BucketCache bucketCache;

    public StatisticsThread(BucketCache bucketCache) {
      super("BucketCacheStatsThread");
      setDaemon(true);
      this.bucketCache = bucketCache;
    }

    @Override
    public void run() {
      bucketCache.logStats();
    }
  }

  public void logStats() {
    long totalSize = bucketAllocator.getTotalSize();
    long usedSize = bucketAllocator.getUsedSize();
    long freeSize = totalSize - usedSize;
    long cacheSize = getRealCacheSize();
    LOG.info("failedBlockAdditions=" + cacheStats.getFailedInserts() + ", " +
        "totalSize=" + StringUtils.byteDesc(totalSize) + ", " +
        "freeSize=" + StringUtils.byteDesc(freeSize) + ", " +
        "usedSize=" + StringUtils.byteDesc(usedSize) +", " +
        "cacheSize=" + StringUtils.byteDesc(cacheSize) +", " +
        "accesses=" + cacheStats.getRequestCount() + ", " +
        "hits=" + cacheStats.getHitCount() + ", " +
        "IOhitsPerSecond=" + cacheStats.getIOHitsPerSecond() + ", " +
        "IOTimePerHit=" + String.format("%.2f", cacheStats.getIOTimePerHit())+ ", " +
        "hitRatio=" + (cacheStats.getHitCount() == 0 ? "0," :
        (StringUtils.formatPercent(cacheStats.getHitRatio(), 2)+ ", ")) +
        "cachingAccesses=" + cacheStats.getRequestCachingCount() + ", " +
        "cachingHits=" + cacheStats.getHitCachingCount() + ", " +
        "cachingHitsRatio=" +(cacheStats.getHitCachingCount() == 0 ? "0," :
        (StringUtils.formatPercent(cacheStats.getHitCachingRatio(), 2)+ ", ")) +
        "evictions=" + cacheStats.getEvictionCount() + ", " +
        "evicted=" + cacheStats.getEvictedCount() + ", " +
        "evictedPerRun=" + cacheStats.evictedPerEviction() + ", " +
        "allocationFailCount=" + cacheStats.getAllocationFailCount());
    cacheStats.reset();
  }

  public long getRealCacheSize() {
    return this.realCacheSize.sum();
  }

  public long acceptableSize() {
    return (long) Math.floor(bucketAllocator.getTotalSize() * acceptableFactor);
  }

  long getPartitionSize(float partitionFactor) {
    return (long) Math.floor(bucketAllocator.getTotalSize() * partitionFactor * minFactor);
  }

  /**
   * Return the count of bucketSizeinfos still need free space
   */
  private int bucketSizesAboveThresholdCount(float minFactor) {
    BucketAllocator.IndexStatistics[] stats = bucketAllocator.getIndexStatistics();
    int fullCount = 0;
    for (int i = 0; i < stats.length; i++) {
      long freeGoal = (long) Math.floor(stats[i].totalCount() * (1 - minFactor));
      freeGoal = Math.max(freeGoal, 1);
      if (stats[i].freeCount() < freeGoal) {
        fullCount++;
      }
    }
    return fullCount;
  }

  /**
   * This method will find the buckets that are minimally occupied
   * and are not reference counted and will free them completely
   * without any constraint on the access times of the elements,
   * and as a process will completely free at most the number of buckets
   * passed, sometimes it might not due to changing refCounts
   *
   * @param completelyFreeBucketsNeeded number of buckets to free
   **/
  private void freeEntireBuckets(int completelyFreeBucketsNeeded) {
    if (completelyFreeBucketsNeeded != 0) {
      // First we will build a set where the offsets are reference counted, usually
      // this set is small around O(Handler Count) unless something else is wrong
      Set<Integer> inUseBuckets = new HashSet<>();
      backingMap.forEach((k, be) -> {
        if (be.isRpcRef()) {
          inUseBuckets.add(bucketAllocator.getBucketIndex(be.offset()));
        }
      });
      Set<Integer> candidateBuckets =
          bucketAllocator.getLeastFilledBuckets(inUseBuckets, completelyFreeBucketsNeeded);
      for (Map.Entry<BlockCacheKey, BucketEntry> entry : backingMap.entrySet()) {
        if (candidateBuckets.contains(bucketAllocator.getBucketIndex(entry.getValue().offset()))) {
          evictBucketEntryIfNoRpcReferenced(entry.getKey(), entry.getValue());
        }
      }
    }
  }

  /**
   * Free the space if the used size reaches acceptableSize() or one size block
   * couldn't be allocated. When freeing the space, we use the LRU algorithm and
   * ensure there must be some blocks evicted
   * @param why Why we are being called
   */
  private void freeSpace(final String why) {
    // Ensure only one freeSpace progress at a time
    if (!freeSpaceLock.tryLock()) {
      return;
    }
    try {
      freeInProgress = true;
      long bytesToFreeWithoutExtra = 0;
      // Calculate free byte for each bucketSizeinfo
      StringBuilder msgBuffer = LOG.isDebugEnabled()? new StringBuilder(): null;
      BucketAllocator.IndexStatistics[] stats = bucketAllocator.getIndexStatistics();
      long[] bytesToFreeForBucket = new long[stats.length];
      for (int i = 0; i < stats.length; i++) {
        bytesToFreeForBucket[i] = 0;
        long freeGoal = (long) Math.floor(stats[i].totalCount() * (1 - minFactor));
        freeGoal = Math.max(freeGoal, 1);
        if (stats[i].freeCount() < freeGoal) {
          bytesToFreeForBucket[i] = stats[i].itemSize() * (freeGoal - stats[i].freeCount());
          bytesToFreeWithoutExtra += bytesToFreeForBucket[i];
          if (msgBuffer != null) {
            msgBuffer.append("Free for bucketSize(" + stats[i].itemSize() + ")="
                + StringUtils.byteDesc(bytesToFreeForBucket[i]) + ", ");
          }
        }
      }
      if (msgBuffer != null) {
        msgBuffer.append("Free for total=" + StringUtils.byteDesc(bytesToFreeWithoutExtra) + ", ");
      }

      if (bytesToFreeWithoutExtra <= 0) {
        return;
      }
      long currentSize = bucketAllocator.getUsedSize();
      long totalSize = bucketAllocator.getTotalSize();
      if (LOG.isDebugEnabled() && msgBuffer != null) {
        LOG.debug("Free started because \"" + why + "\"; " + msgBuffer.toString() +
            " of current used=" + StringUtils.byteDesc(currentSize) + ", actual cacheSize=" +
            StringUtils.byteDesc(realCacheSize.sum()) + ", total=" + StringUtils.byteDesc(totalSize));
      }

      long bytesToFreeWithExtra = (long) Math.floor(bytesToFreeWithoutExtra
          * (1 + extraFreeFactor));

      // Instantiate priority buckets
      BucketEntryGroup bucketSingle = new BucketEntryGroup(bytesToFreeWithExtra,
          blockSize, getPartitionSize(singleFactor));
      BucketEntryGroup bucketMulti = new BucketEntryGroup(bytesToFreeWithExtra,
          blockSize, getPartitionSize(multiFactor));
      BucketEntryGroup bucketMemory = new BucketEntryGroup(bytesToFreeWithExtra,
          blockSize, getPartitionSize(memoryFactor));

      // Scan entire map putting bucket entry into appropriate bucket entry
      // group
      for (Map.Entry<BlockCacheKey, BucketEntry> bucketEntryWithKey : backingMap.entrySet()) {
        switch (bucketEntryWithKey.getValue().getPriority()) {
          case SINGLE: {
            bucketSingle.add(bucketEntryWithKey);
            break;
          }
          case MULTI: {
            bucketMulti.add(bucketEntryWithKey);
            break;
          }
          case MEMORY: {
            bucketMemory.add(bucketEntryWithKey);
            break;
          }
        }
      }

      PriorityQueue<BucketEntryGroup> bucketQueue = new PriorityQueue<>(3,
          Comparator.comparingLong(BucketEntryGroup::overflow));

      bucketQueue.add(bucketSingle);
      bucketQueue.add(bucketMulti);
      bucketQueue.add(bucketMemory);

      int remainingBuckets = bucketQueue.size();
      long bytesFreed = 0;

      BucketEntryGroup bucketGroup;
      while ((bucketGroup = bucketQueue.poll()) != null) {
        long overflow = bucketGroup.overflow();
        if (overflow > 0) {
          long bucketBytesToFree = Math.min(overflow,
              (bytesToFreeWithoutExtra - bytesFreed) / remainingBuckets);
          bytesFreed += bucketGroup.free(bucketBytesToFree);
        }
        remainingBuckets--;
      }

      // Check and free if there are buckets that still need freeing of space
      if (bucketSizesAboveThresholdCount(minFactor) > 0) {
        bucketQueue.clear();
        remainingBuckets = 3;

        bucketQueue.add(bucketSingle);
        bucketQueue.add(bucketMulti);
        bucketQueue.add(bucketMemory);

        while ((bucketGroup = bucketQueue.poll()) != null) {
          long bucketBytesToFree = (bytesToFreeWithExtra - bytesFreed) / remainingBuckets;
          bytesFreed += bucketGroup.free(bucketBytesToFree);
          remainingBuckets--;
        }
      }

      // Even after the above free we might still need freeing because of the
      // De-fragmentation of the buckets (also called Slab Calcification problem), i.e
      // there might be some buckets where the occupancy is very sparse and thus are not
      // yielding the free for the other bucket sizes, the fix for this to evict some
      // of the buckets, we do this by evicting the buckets that are least fulled
      freeEntireBuckets(DEFAULT_FREE_ENTIRE_BLOCK_FACTOR *
          bucketSizesAboveThresholdCount(1.0f));

      if (LOG.isDebugEnabled()) {
        long single = bucketSingle.totalSize();
        long multi = bucketMulti.totalSize();
        long memory = bucketMemory.totalSize();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Bucket cache free space completed; " + "freed="
              + StringUtils.byteDesc(bytesFreed) + ", " + "total="
              + StringUtils.byteDesc(totalSize) + ", " + "single="
              + StringUtils.byteDesc(single) + ", " + "multi="
              + StringUtils.byteDesc(multi) + ", " + "memory="
              + StringUtils.byteDesc(memory));
        }
      }

    } catch (Throwable t) {
      LOG.warn("Failed freeing space", t);
    } finally {
      cacheStats.evict();
      freeInProgress = false;
      freeSpaceLock.unlock();
    }
  }

  // This handles flushing the RAM cache to IOEngine.
  class WriterThread extends Thread {
    private final BlockingQueue<RAMQueueEntry> inputQueue;
    private volatile boolean writerEnabled = true;

    WriterThread(BlockingQueue<RAMQueueEntry> queue) {
      super("BucketCacheWriterThread");
      this.inputQueue = queue;
    }

    // Used for test
    void disableWriter() {
      this.writerEnabled = false;
    }

    @Override
    public void run() {
      List<RAMQueueEntry> entries = new ArrayList<>();
      try {
        while (cacheEnabled && writerEnabled) {
          try {
            try {
              // Blocks
              entries = getRAMQueueEntries(inputQueue, entries);
            } catch (InterruptedException ie) {
              if (!cacheEnabled || !writerEnabled) {
                break;
              }
            }
            doDrain(entries);
          } catch (Exception ioe) {
            LOG.error("WriterThread encountered error", ioe);
          }
        }
      } catch (Throwable t) {
        LOG.warn("Failed doing drain", t);
      }
      LOG.info(this.getName() + " exiting, cacheEnabled=" + cacheEnabled);
    }
  }

  /**
   * Put the new bucket entry into backingMap. Notice that we are allowed to replace the existing
   * cache with a new block for the same cache key. there's a corner case: one thread cache a block
   * in ramCache, copy to io-engine and add a bucket entry to backingMap. Caching another new block
   * with the same cache key do the same thing for the same cache key, so if not evict the previous
   * bucket entry, then memory leak happen because the previous bucketEntry is gone but the
   * bucketAllocator do not free its memory.
   * @see BlockCacheUtil#shouldReplaceExistingCacheBlock(BlockCache blockCache,BlockCacheKey
   *      cacheKey, Cacheable newBlock)
   * @param key Block cache key
   * @param bucketEntry Bucket entry to put into backingMap.
   */
  protected void putIntoBackingMap(BlockCacheKey key, BucketEntry bucketEntry) {
    BucketEntry previousEntry = backingMap.put(key, bucketEntry);
    if (previousEntry != null && previousEntry != bucketEntry) {
      previousEntry.withWriteLock(offsetLock, () -> {
        blockEvicted(key, previousEntry, false);
        return null;
      });
    }
  }

  /**
   * Prepare and return a warning message for Bucket Allocator Exception
   * @param re The RAMQueueEntry for which the exception was thrown.
   * @return A warning message created from the input RAMQueueEntry object.
   */
  private String getAllocationFailWarningMessage(RAMQueueEntry re) {
    if (re != null && re.getData() instanceof HFileBlock) {
      HFileContext fileContext = ((HFileBlock) re.getData()).getHFileContext();
      String columnFamily = Bytes.toString(fileContext.getColumnFamily());
      String tableName = Bytes.toString(fileContext.getTableName());
      if (tableName != null && columnFamily != null) {
        return ("Most recent failed allocation in " + ALLOCATION_FAIL_LOG_TIME_PERIOD
            + " milliseconds; Table Name = " + tableName + ", Column Family = " + columnFamily
            + ", HFile Name : " + fileContext.getHFileName());
      }
    }
    return ("Most recent failed allocation in " + ALLOCATION_FAIL_LOG_TIME_PERIOD
        + " milliseconds; HFile Name : " + (re == null ? "" : re.getKey()));
  }

  /**
   * Flush the entries in ramCache to IOEngine and add bucket entry to backingMap. Process all that
   * are passed in even if failure being sure to remove from ramCache else we'll never undo the
   * references and we'll OOME.
   * @param entries Presumes list passed in here will be processed by this invocation only. No
   *          interference expected.
   */
  void doDrain(final List<RAMQueueEntry> entries) throws InterruptedException {
    if (entries.isEmpty()) {
      return;
    }
    // This method is a little hard to follow. We run through the passed in entries and for each
    // successful add, we add a non-null BucketEntry to the below bucketEntries. Later we must
    // do cleanup making sure we've cleared ramCache of all entries regardless of whether we
    // successfully added the item to the bucketcache; if we don't do the cleanup, we'll OOME by
    // filling ramCache. We do the clean up by again running through the passed in entries
    // doing extra work when we find a non-null bucketEntries corresponding entry.
    final int size = entries.size();
    BucketEntry[] bucketEntries = new BucketEntry[size];
    // Index updated inside loop if success or if we can't succeed. We retry if cache is full
    // when we go to add an entry by going around the loop again without upping the index.
    int index = 0;
    while (cacheEnabled && index < size) {
      RAMQueueEntry re = null;
      try {
        re = entries.get(index);
        if (re == null) {
          LOG.warn("Couldn't get entry or changed on us; who else is messing with it?");
          index++;
          continue;
        }
        BucketEntry bucketEntry = re.writeToCache(ioEngine, bucketAllocator, realCacheSize,
            this::createRecycler);
        // Successfully added. Up index and add bucketEntry. Clear io exceptions.
        bucketEntries[index] = bucketEntry;
        if (ioErrorStartTime > 0) {
          ioErrorStartTime = -1;
        }
        index++;
      } catch (BucketAllocatorException fle) {
        long currTs = EnvironmentEdgeManager.currentTime();
        cacheStats.allocationFailed(); // Record the warning.
        if (allocFailLogPrevTs == 0 || (currTs - allocFailLogPrevTs) > ALLOCATION_FAIL_LOG_TIME_PERIOD) {
          LOG.warn (getAllocationFailWarningMessage(re), fle);
          allocFailLogPrevTs = currTs;
        }
        // Presume can't add. Too big? Move index on. Entry will be cleared from ramCache below.
        bucketEntries[index] = null;
        index++;
      } catch (CacheFullException cfe) {
        // Cache full when we tried to add. Try freeing space and then retrying (don't up index)
        if (!freeInProgress) {
          freeSpace("Full!");
        } else {
          Thread.sleep(50);
        }
      } catch (IOException ioex) {
        // Hopefully transient. Retry. checkIOErrorIsTolerated disables cache if problem.
        LOG.error("Failed writing to bucket cache", ioex);
        checkIOErrorIsTolerated();
      }
    }

    // Make sure data pages are written on media before we update maps.
    try {
      ioEngine.sync();
    } catch (IOException ioex) {
      LOG.error("Failed syncing IO engine", ioex);
      checkIOErrorIsTolerated();
      // Since we failed sync, free the blocks in bucket allocator
      for (int i = 0; i < entries.size(); ++i) {
        if (bucketEntries[i] != null) {
          bucketAllocator.freeBlock(bucketEntries[i].offset());
          bucketEntries[i] = null;
        }
      }
    }

    // Now add to backingMap if successfully added to bucket cache. Remove from ramCache if
    // success or error.
    for (int i = 0; i < size; ++i) {
      BlockCacheKey key = entries.get(i).getKey();
      // Only add if non-null entry.
      if (bucketEntries[i] != null) {
        putIntoBackingMap(key, bucketEntries[i]);
      }
      // Always remove from ramCache even if we failed adding it to the block cache above.
      boolean existed = ramCache.remove(key, re -> {
        if (re != null) {
          heapSize.add(-1 * re.getData().heapSize());
        }
      });
      if (!existed && bucketEntries[i] != null) {
        // Block should have already been evicted. Remove it and free space.
        final BucketEntry bucketEntry = bucketEntries[i];
        bucketEntry.withWriteLock(offsetLock, () -> {
          if (backingMap.remove(key, bucketEntry)) {
            blockEvicted(key, bucketEntry, false);
          }
          return null;
        });
      }
    }

    long used = bucketAllocator.getUsedSize();
    if (used > acceptableSize()) {
      freeSpace("Used=" + used + " > acceptable=" + acceptableSize());
    }
    return;
  }

  /**
   * Blocks until elements available in {@code q} then tries to grab as many as possible before
   * returning.
   * @param receptacle Where to stash the elements taken from queue. We clear before we use it just
   *          in case.
   * @param q The queue to take from.
   * @return {@code receptacle} laden with elements taken from the queue or empty if none found.
   */
  static List<RAMQueueEntry> getRAMQueueEntries(BlockingQueue<RAMQueueEntry> q,
                                                List<RAMQueueEntry> receptacle) throws InterruptedException {
    // Clear sets all entries to null and sets size to 0. We retain allocations. Presume it
    // ok even if list grew to accommodate thousands.
    receptacle.clear();
    receptacle.add(q.take());
    q.drainTo(receptacle);
    return receptacle;
  }

  /**
   * @see #retrieveFromFile(int[])
   */
  private void persistToFile() throws IOException {
    assert !cacheEnabled;
    if (!ioEngine.isPersistent()) {
      throw new IOException("Attempt to persist non-persistent cache mappings!");
    }
    try (FileOutputStream fos = new FileOutputStream(persistencePath, false)) {
      fos.write(ProtobufMagic.PB_MAGIC);
      BucketProtoUtils.toPB(this).writeDelimitedTo(fos);
    }
  }

  /**
   * @see #persistToFile()
   */
  private void retrieveFromFile(int[] bucketSizes) throws IOException {
    File persistenceFile = new File(persistencePath);
    if (!persistenceFile.exists()) {
      return;
    }
    assert !cacheEnabled;

    try (FileInputStream in = deleteFileOnClose(persistenceFile)) {
      int pblen = ProtobufMagic.lengthOfPBMagic();
      byte[] pbuf = new byte[pblen];
      IOUtils.readFully(in, pbuf, 0, pblen);
      if (! ProtobufMagic.isPBMagicPrefix(pbuf)) {
        // In 3.0 we have enough flexibility to dump the old cache data.
        // TODO: In 2.x line, this might need to be filled in to support reading the old format
        throw new IOException("Persistence file does not start with protobuf magic number. " +
            persistencePath);
      }
      parsePB(BucketCacheProtos.BucketCacheEntry.parseDelimitedFrom(in));
      bucketAllocator = new BucketAllocator(cacheCapacity, bucketSizes, backingMap, realCacheSize);
      blockNumber.add(backingMap.size());
    }
  }

  /**
   * Create an input stream that deletes the file after reading it. Use in try-with-resources to
   * avoid this pattern where an exception thrown from a finally block may mask earlier exceptions:
   * <pre>
   *   File f = ...
   *   try (FileInputStream fis = new FileInputStream(f)) {
   *     // use the input stream
   *   } finally {
   *     if (!f.delete()) throw new IOException("failed to delete");
   *   }
   * </pre>
   * @param file the file to read and delete
   * @return a FileInputStream for the given file
   * @throws IOException if there is a problem creating the stream
   */
  private FileInputStream deleteFileOnClose(final File file) throws IOException {
    return new FileInputStream(file) {
      private File myFile;
      private FileInputStream init(File file) {
        myFile = file;
        return this;
      }
      @Override
      public void close() throws IOException {
        // close() will be called during try-with-resources and it will be
        // called by finalizer thread during GC. To avoid double-free resource,
        // set myFile to null after the first call.
        if (myFile == null) {
          return;
        }

        super.close();
        if (!myFile.delete()) {
          throw new IOException("Failed deleting persistence file " + myFile.getAbsolutePath());
        }
        myFile = null;
      }
    }.init(file);
  }

  private void verifyCapacityAndClasses(long capacitySize, String ioclass, String mapclass)
      throws IOException {
    if (capacitySize != cacheCapacity) {
      throw new IOException("Mismatched cache capacity:"
          + StringUtils.byteDesc(capacitySize) + ", expected: "
          + StringUtils.byteDesc(cacheCapacity));
    }
    if (!ioEngine.getClass().getName().equals(ioclass)) {
      throw new IOException("Class name for IO engine mismatch: " + ioclass
          + ", expected:" + ioEngine.getClass().getName());
    }
    if (!backingMap.getClass().getName().equals(mapclass)) {
      throw new IOException("Class name for cache map mismatch: " + mapclass
          + ", expected:" + backingMap.getClass().getName());
    }
  }

  private void parsePB(BucketCacheProtos.BucketCacheEntry proto) throws IOException {
    if (proto.hasChecksum()) {
      ((PersistentIOEngine) ioEngine).verifyFileIntegrity(proto.getChecksum().toByteArray(),
          algorithm);
    } else {
      // if has not checksum, it means the persistence file is old format
      LOG.info("Persistent file is old format, it does not support verifying file integrity!");
    }
    verifyCapacityAndClasses(proto.getCacheCapacity(), proto.getIoClass(), proto.getMapClass());
    backingMap = BucketProtoUtils.fromPB(proto.getDeserializersMap(), proto.getBackingMap(),
        this::createRecycler);
  }

  /**
   * Check whether we tolerate IO error this time. If the duration of IOEngine
   * throwing errors exceeds ioErrorsDurationTimeTolerated, we will disable the
   * cache
   */
  private void checkIOErrorIsTolerated() {
    long now = EnvironmentEdgeManager.currentTime();
    // Do a single read to a local variable to avoid timing issue - HBASE-24454
    long ioErrorStartTimeTmp = this.ioErrorStartTime;
    if (ioErrorStartTimeTmp > 0) {
      if (cacheEnabled && (now - ioErrorStartTimeTmp) > this.ioErrorsTolerationDuration) {
        LOG.error("IO errors duration time has exceeded " + ioErrorsTolerationDuration +
            "ms, disabling cache, please check your IOEngine");
        disableCache();
      }
    } else {
      this.ioErrorStartTime = now;
    }
  }

  /**
   * Used to shut down the cache -or- turn it off in the case of something broken.
   */
  private void disableCache() {
    if (!cacheEnabled) return;
    cacheEnabled = false;
    ioEngine.shutdown();
    this.scheduleThreadPool.shutdown();
    for (int i = 0; i < writerThreads.length; ++i) writerThreads[i].interrupt();
    this.ramCache.clear();
    if (!ioEngine.isPersistent() || persistencePath == null) {
      // If persistent ioengine and a path, we will serialize out the backingMap.
      this.backingMap.clear();
    }
  }

  private void join() throws InterruptedException {
    for (int i = 0; i < writerThreads.length; ++i)
      writerThreads[i].join();
  }

  @Override
  public void shutdown() {
    disableCache();
    LOG.info("Shutdown bucket cache: IO persistent=" + ioEngine.isPersistent()
        + "; path to write=" + persistencePath);
    if (ioEngine.isPersistent() && persistencePath != null) {
      try {
        join();
        persistToFile();
      } catch (IOException ex) {
        LOG.error("Unable to persist data on exit: " + ex.toString(), ex);
      } catch (InterruptedException e) {
        LOG.warn("Failed to persist data on exit", e);
      }
    }
  }

  @Override
  public CacheStats getStats() {
    return cacheStats;
  }

  public BucketAllocator getAllocator() {
    return this.bucketAllocator;
  }

  @Override
  public long heapSize() {
    return this.heapSize.sum();
  }

  @Override
  public long size() {
    return this.realCacheSize.sum();
  }

  @Override
  public long getCurrentDataSize() {
    return size();
  }

  @Override
  public long getFreeSize() {
    return this.bucketAllocator.getFreeSize();
  }

  @Override
  public long getBlockCount() {
    return this.blockNumber.sum();
  }

  @Override
  public long getDataBlockCount() {
    return getBlockCount();
  }

  @Override
  public long getCurrentSize() {
    return this.bucketAllocator.getUsedSize();
  }

  protected String getAlgorithm() {
    return algorithm;
  }

  /**
   * Evicts all blocks for a specific HFile.
   * <p>
   * This is used for evict-on-close to remove all blocks of a specific HFile.
   *
   * @return the number of blocks evicted
   */
  @Override
  public int evictBlocksByHfileName(String hfileName) {
    Set<BlockCacheKey> keySet = blocksByHFile.subSet(
        new BlockCacheKey(hfileName, Long.MIN_VALUE), true,
        new BlockCacheKey(hfileName, Long.MAX_VALUE), true);

    int numEvicted = 0;
    for (BlockCacheKey key : keySet) {
      if (evictBlock(key)) {
        ++numEvicted;
      }
    }

    return numEvicted;
  }

  /**
   * Used to group bucket entries into priority buckets. There will be a
   * BucketEntryGroup for each priority (single, multi, memory). Once bucketed,
   * the eviction algorithm takes the appropriate number of elements out of each
   * according to configuration parameters and their relative sizes.
   */
  private class BucketEntryGroup {

    private CachedEntryQueue queue;
    private long totalSize = 0;
    private long bucketSize;

    public BucketEntryGroup(long bytesToFree, long blockSize, long bucketSize) {
      this.bucketSize = bucketSize;
      queue = new CachedEntryQueue(bytesToFree, blockSize);
      totalSize = 0;
    }

    public void add(Map.Entry<BlockCacheKey, BucketEntry> block) {
      totalSize += block.getValue().getLength();
      queue.add(block);
    }

    public long free(long toFree) {
      Map.Entry<BlockCacheKey, BucketEntry> entry;
      long freedBytes = 0;
      // TODO avoid a cycling siutation. We find no block which is not in use and so no way to free
      // What to do then? Caching attempt fail? Need some changes in cacheBlock API?
      while ((entry = queue.pollLast()) != null) {
        BlockCacheKey blockCacheKey = entry.getKey();
        BucketEntry be = entry.getValue();
        if (evictBucketEntryIfNoRpcReferenced(blockCacheKey, be)) {
          freedBytes += be.getLength();
        }
        if (freedBytes >= toFree) {
          return freedBytes;
        }
      }
      return freedBytes;
    }

    public long overflow() {
      return totalSize - bucketSize;
    }

    public long totalSize() {
      return totalSize;
    }
  }

  /**
   * Block Entry stored in the memory with key,data and so on
   */
  static class RAMQueueEntry {
    private final BlockCacheKey key;
    private final Cacheable data;
    private long accessCounter;
    private boolean inMemory;

    RAMQueueEntry(BlockCacheKey bck, Cacheable data, long accessCounter, boolean inMemory) {
      this.key = bck;
      this.data = data;
      this.accessCounter = accessCounter;
      this.inMemory = inMemory;
    }

    public Cacheable getData() {
      return data;
    }

    public BlockCacheKey getKey() {
      return key;
    }

    public void access(long accessCounter) {
      this.accessCounter = accessCounter;
    }

    private ByteBuffAllocator getByteBuffAllocator() {
      if (data instanceof HFileBlock) {
        return ((HFileBlock) data).getByteBuffAllocator();
      }
      return ByteBuffAllocator.HEAP;
    }

    public BucketEntry writeToCache(final IOEngine ioEngine, final BucketAllocator alloc,
                                    final LongAdder realCacheSize, Function<BucketEntry, Recycler> createRecycler)
        throws IOException {
      int len = data.getSerializedLength();
      // This cacheable thing can't be serialized
      if (len == 0) {
        return null;
      }
      long offset = alloc.allocateBlock(len);
      boolean succ = false;
      BucketEntry bucketEntry = null;
      try {
        bucketEntry = new BucketEntry(offset, len, accessCounter, inMemory, createRecycler,
            getByteBuffAllocator());
        bucketEntry.setDeserializerReference(data.getDeserializer());
        if (data instanceof HFileBlock) {
          // If an instance of HFileBlock, save on some allocations.
          HFileBlock block = (HFileBlock) data;
          ByteBuff sliceBuf = block.getBufferReadOnly();
          ByteBuffer metadata = block.getMetaData();
          ioEngine.write(sliceBuf, offset);
          ioEngine.write(metadata, offset + len - metadata.limit());
        } else {
          // Only used for testing.
          ByteBuffer bb = ByteBuffer.allocate(len);
          data.serialize(bb, true);
          ioEngine.write(bb, offset);
        }
        succ = true;
      } finally {
        if (!succ) {
          alloc.freeBlock(offset);
        }
      }
      realCacheSize.add(len);
      return bucketEntry;
    }
  }

  /**
   * Only used in test
   * @throws InterruptedException
   */
  void stopWriterThreads() throws InterruptedException {
    for (WriterThread writerThread : writerThreads) {
      writerThread.disableWriter();
      writerThread.interrupt();
      writerThread.join();
    }
  }

  @Override
  public Iterator<CachedBlock> iterator() {
    // Don't bother with ramcache since stuff is in here only a little while.
    final Iterator<Map.Entry<BlockCacheKey, BucketEntry>> i =
        this.backingMap.entrySet().iterator();
    return new Iterator<CachedBlock>() {
      private final long now = System.nanoTime();

      @Override
      public boolean hasNext() {
        return i.hasNext();
      }

      @Override
      public CachedBlock next() {
        final Map.Entry<BlockCacheKey, BucketEntry> e = i.next();
        return new CachedBlock() {
          @Override
          public String toString() {
            return BlockCacheUtil.toString(this, now);
          }

          @Override
          public BlockPriority getBlockPriority() {
            return e.getValue().getPriority();
          }

          @Override
          public BlockType getBlockType() {
            // Not held by BucketEntry.  Could add it if wanted on BucketEntry creation.
            return null;
          }

          @Override
          public long getOffset() {
            return e.getKey().getOffset();
          }

          @Override
          public long getSize() {
            return e.getValue().getLength();
          }

          @Override
          public long getCachedTime() {
            return e.getValue().getCachedTime();
          }

          @Override
          public String getFilename() {
            return e.getKey().getHfileName();
          }

          @Override
          public int compareTo(CachedBlock other) {
            int diff = this.getFilename().compareTo(other.getFilename());
            if (diff != 0) return diff;

            diff = Long.compare(this.getOffset(), other.getOffset());
            if (diff != 0) return diff;
            if (other.getCachedTime() < 0 || this.getCachedTime() < 0) {
              throw new IllegalStateException("" + this.getCachedTime() + ", " +
                  other.getCachedTime());
            }
            return Long.compare(other.getCachedTime(), this.getCachedTime());
          }

          @Override
          public int hashCode() {
            return e.getKey().hashCode();
          }

          @Override
          public boolean equals(Object obj) {
            if (obj instanceof CachedBlock) {
              CachedBlock cb = (CachedBlock)obj;
              return compareTo(cb) == 0;
            } else {
              return false;
            }
          }
        };
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  @Override
  public BlockCache[] getBlockCaches() {
    return null;
  }

  public int getRpcRefCount(BlockCacheKey cacheKey) {
    BucketEntry bucketEntry = backingMap.get(cacheKey);
    if (bucketEntry != null) {
      return bucketEntry.refCnt() - (bucketEntry.markedAsEvicted.get() ? 0 : 1);
    }
    return 0;
  }

  float getAcceptableFactor() {
    return acceptableFactor;
  }

  float getMinFactor() {
    return minFactor;
  }

  float getExtraFreeFactor() {
    return extraFreeFactor;
  }

  float getSingleFactor() {
    return singleFactor;
  }

  float getMultiFactor() {
    return multiFactor;
  }

  float getMemoryFactor() {
    return memoryFactor;
  }

  /**
   * Wrapped the delegate ConcurrentMap with maintaining its block's reference count.
   */
  static class RAMCache {
    /**
     * Defined the map as {@link ConcurrentHashMap} explicitly here, because in
     * {@link RAMCache#get(BlockCacheKey)} and
     * {@link RAMCache#putIfAbsent(BlockCacheKey, BucketCache.RAMQueueEntry)} , we need to
     * guarantee the atomicity of map#computeIfPresent(key, func) and map#putIfAbsent(key, func).
     * Besides, the func method can execute exactly once only when the key is present(or absent)
     * and under the lock context. Otherwise, the reference count of block will be messed up.
     * Notice that the {@link java.util.concurrent.ConcurrentSkipListMap} can not guarantee that.
     */
    final ConcurrentHashMap<BlockCacheKey, RAMQueueEntry> delegate = new ConcurrentHashMap<>();

    public boolean containsKey(BlockCacheKey key) {
      return delegate.containsKey(key);
    }

    public RAMQueueEntry get(BlockCacheKey key) {
      return delegate.computeIfPresent(key, (k, re) -> {
        // It'll be referenced by RPC, so retain atomically here. if the get and retain is not
        // atomic, another thread may remove and release the block, when retaining in this thread we
        // may retain a block with refCnt=0 which is disallowed. (see HBASE-22422)
        re.getData().retain();
        return re;
      });
    }

    /**
     * Return the previous associated value, or null if absent. It has the same meaning as
     * {@link ConcurrentMap#putIfAbsent(Object, Object)}
     */
    public RAMQueueEntry putIfAbsent(BlockCacheKey key, RAMQueueEntry entry) {
      AtomicBoolean absent = new AtomicBoolean(false);
      RAMQueueEntry re = delegate.computeIfAbsent(key, k -> {
        // The RAMCache reference to this entry, so reference count should be increment.
        entry.getData().retain();
        absent.set(true);
        return entry;
      });
      return absent.get() ? null : re;
    }

    public boolean remove(BlockCacheKey key) {
      return remove(key, re->{});
    }

    /**
     * Defined an {@link Consumer} here, because once the removed entry release its reference count,
     * then it's ByteBuffers may be recycled and accessing it outside this method will be thrown an
     * exception. the consumer will access entry to remove before release its reference count.
     * Notice, don't change its reference count in the {@link Consumer}
     */
    public boolean remove(BlockCacheKey key, Consumer<RAMQueueEntry> action) {
      RAMQueueEntry previous = delegate.remove(key);
      action.accept(previous);
      if (previous != null) {
        previous.getData().release();
      }
      return previous != null;
    }

    public boolean isEmpty() {
      return delegate.isEmpty();
    }

    public void clear() {
      Iterator<Map.Entry<BlockCacheKey, RAMQueueEntry>> it = delegate.entrySet().iterator();
      while (it.hasNext()) {
        RAMQueueEntry re = it.next().getValue();
        it.remove();
        re.getData().release();
      }
    }
  }
}
