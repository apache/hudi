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

package org.apache.hudi.common.util.collection;

import org.apache.hudi.common.fs.SizeAwareDataOutputStream;
import org.apache.hudi.common.util.BinaryUtil;
import org.apache.hudi.common.util.BufferedRandomAccessFile;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SampleEstimator;
import org.apache.hudi.common.util.SerializationUtils;
import org.apache.hudi.common.util.SizeEstimator;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieNotSupportedException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

public class SortedAppendOnlyExternalSpillableMap<K extends Serializable, V extends Serializable> implements AbstractExternalSpillableMap<K, V>, ClosableIterable<V> {
  private static final Logger LOG = LoggerFactory.getLogger(SortedAppendOnlyExternalSpillableMap.class);

  private static final int DEFAULT_WRITE_BUFFER = 64 * 1024; // 64KB

  private static final int DEFAULT_READ_BUFFER = 64 * 1024; // 64KB

  private static final String SPILL_FILE_SUFFIX = "sorted";

  private static final String SUBFOLDER_PREFIX = "hudi/sorted_ao_map";
  protected static final int DEFAULT_PROGRESS_LOG_INTERVAL_NUM = 1_000_000;
  protected final String basePath;
  protected final long maxMemoryInBytes;
  protected final Comparator<V> comparator;
  protected final SizeEstimator<K> keySizeEstimator;
  protected final SizeEstimator<V> valueSizeEstimator;
  protected long totalEntryCount;

  private final SortEngine sortEngine;

  private final HoodieTimer spillTimer;

  private long totalMemorySize;

  private SortedFileManager fileManager = new SortedFileManager();

  private Option<CombineFunc<K, V, V>> combineFuncOpt = Option.empty();

  // tick to track time taken to insert and write record, reset on initialization and every #clear
  private HoodieTimer lifeCycleTimer = HoodieTimer.create();

  // TODO: better serializeFunc
  private SerializeFunc<V> serializeFunc = new SerializeFunc<V>() {
    @Override
    public byte[] serialize(V record) {
      try {
        return SerializationUtils.serialize(record);
      } catch (IOException e) {
        LOG.error("Failed to serialize record: {}", record, e);
        throw new HoodieIOException("Failed to serialize record", e);
      }
    }

    @Override
    public V deserialize(byte[] bytes) {
      return SerializationUtils.deserialize(bytes);
    }
  };

  private final Function<V, K> keyGetterFromValue;

  private MemoryCombinedMap memoryEntries;

  public SortedAppendOnlyExternalSpillableMap(String baseFilePath, long maxMemorySize, Comparator<V> comparator,
                                              SizeEstimator<K> keySizeEstimator, SizeEstimator<V> valueSizeEstimator, SortEngine sortEngine, Function<V, K> keyGetterFromValue) throws IOException {
    this(baseFilePath, maxMemorySize, comparator, keySizeEstimator, valueSizeEstimator, sortEngine, keyGetterFromValue, Option.empty());
  }

  public SortedAppendOnlyExternalSpillableMap(String baseFilePath, long maxMemorySize, Comparator<V> comparator,
                                              SizeEstimator<K> keySizeEstimator, SizeEstimator<V> valueSizeEstimator, SortEngine sortEngine, Function<V, K> keyGetterFromValue,
                                              Option<CombineFunc<K, V, V>> combineFuncOpt)
      throws IOException {
    this.basePath = String.format("%s/%s-%s", baseFilePath, SUBFOLDER_PREFIX, UUID.randomUUID());
    this.maxMemoryInBytes = maxMemorySize;
    this.comparator = comparator;
    this.keySizeEstimator = keySizeEstimator;
    this.valueSizeEstimator = valueSizeEstimator;
    initBaseDir();
    this.sortEngine = sortEngine;
    this.keyGetterFromValue = keyGetterFromValue;
    this.spillTimer = HoodieTimer.create();
    this.combineFuncOpt = combineFuncOpt;
    this.memoryEntries = new MemoryCombinedMap(this.combineFuncOpt);
    this.lifeCycleTimer.startTimer();
  }

  public void initBaseDir() throws IOException {
    File baseDir = new File(basePath);
    FileIOUtils.deleteDirectory(baseDir);
    FileIOUtils.mkdir(baseDir);
    baseDir.deleteOnExit();
  }

  public SortMergeReader<K, V> mergeSort() {
    long currentMemorySize = this.memoryEntries.getCurrentMemorySize();
    List<SortedEntryReader<Pair<K, V>>> readers = new ArrayList<>(currentMemorySize == 0 ? fileManager.fileCount : fileManager.fileCount + 1);
    // TODO: more suitable batchSize
    if (fileManager.fileCount > 0) {
      long maxBatchSize = (maxMemoryInBytes - currentMemorySize) / fileManager.fileCount;
      for (int i = 0; i < fileManager.fileCount; i++) {
        readers.add(new SortedEntryFileReader(i, maxBatchSize, fileManager.files.get(i)));
      }
    }
    if (currentMemorySize > 0) {
      // sort the memory entries
      readers.add(new SortedEntryMemoryReader(fileManager.fileCount, memoryEntries.getSortedKVIterator()));
    }

    return SortMergeReader.create(sortEngine, readers, comparator, combineFuncOpt);
  }

  @Override
  public ClosableIterator<V> iterator() {
    return mergeSort();
  }

  class DiskFileWriter implements Closeable {
    private final FileOutputStream fileOutputStream;
    private final SizeAwareDataOutputStream writeStream;

    public DiskFileWriter(File file, int writeBufferSize) throws IOException {
      this.fileOutputStream = new FileOutputStream(file, true);
      this.writeStream = new SizeAwareDataOutputStream(fileOutputStream, writeBufferSize);
    }

    public void write(V record) throws IOException {
      // write the record to the file
      Entry entry = Entry.newEntry(serializeFunc.serialize(record));
      entry.writeToFile(writeStream);
    }

    @Override
    public void close() throws IOException {
      writeStream.flush();
      writeStream.close();
    }
  }

  private class SortedFileManager {
    public static final int DEFAULT_FILE_DELETE_RETRY_TIMES = 3;
    private int fileCount;
    private long totalFileSize;
    private List<File> files;

    public SortedFileManager() {
      this.fileCount = 0;
      this.totalFileSize = 0;
      this.files = new LinkedList<>();
    }

    public CompletedCallback<File> createFileForWrite() throws IOException {
      String fileName = String.format("%s/%d.%s", basePath, fileCount, SPILL_FILE_SUFFIX);
      File file = new File(fileName);
      if (!deleteWithRetry(file, DEFAULT_FILE_DELETE_RETRY_TIMES)) {
        throw new HoodieIOException("Failed to delete spill file: " + file.getAbsolutePath());
      }
      if (!file.getParentFile().exists()) {
        file.getParentFile().mkdir();
      }
      file.createNewFile();
      LOG.debug("Created file for write: " + file.getAbsolutePath());
      file.deleteOnExit();
      return CompletedCallback.of(file, success -> {
        if (success) {
          addFile(file);
          return;
        }
        LOG.error("Failed to write file: " + file.getAbsolutePath());
        if (!deleteWithRetry(file, DEFAULT_FILE_DELETE_RETRY_TIMES)) {
          throw new HoodieIOException("Failed to delete spill file: " + file.getAbsolutePath());
        }
      });
    }

    private boolean deleteWithRetry(File file, int retryTimes) {
      for (int i = 0; i < retryTimes && file.exists() && !file.delete(); i++) {
        LOG.warn("Failed to delete spill file: {} for {} times", file.getAbsolutePath(), i + 1);
      }
      return !file.exists();
    }

    public void addFile(File file) {
      this.files.add(file);
      this.fileCount++;
      this.totalFileSize += file.length();
    }

    public long getTotalFileSize() {
      return totalFileSize;
    }

    void clear() {
      files.forEach(File::delete);
      files.clear();
      fileCount = 0;
      totalFileSize = 0;
    }
  }

  private static final class Entry {
    public static final int MAGIC = 0x123321;
    private Integer magic;
    private Long crc;
    private Integer recordSize;
    private byte[] record;

    public Entry(Integer magic, Long crc, Integer recordSize, byte[] record) {
      this.magic = magic;
      this.crc = crc;
      this.recordSize = recordSize;
      this.record = record;
    }

    public Integer getMagic() {
      return magic;
    }

    public Long getCrc() {
      return crc;
    }

    public Integer getRecordSize() {
      return recordSize;
    }

    public byte[] getRecord() {
      return record;
    }

    public static Entry newEntry(byte[] record) {
      return new Entry(MAGIC, BinaryUtil.generateChecksum(record), record.length, record);
    }

    public void writeToFile(SizeAwareDataOutputStream outputStream) throws IOException {
      outputStream.writeInt(magic);
      outputStream.writeLong(crc);
      outputStream.writeInt(recordSize);
      outputStream.write(record);
    }
  }

  private class SortedEntryFileReader extends SortedEntryReader {
    private final File file;
    private final BufferedRandomAccessFile reader;
    private final SizeEstimator<V> sizeEstimator;
    private List<Pair<K, V>> batchRecords;
    private boolean closed;
    private boolean isEOF;
    private long currentReadBatchSize;

    public SortedEntryFileReader(int readerId, long batchSize, File file) {
      super(readerId, batchSize);
      if (SortedAppendOnlyExternalSpillableMap.this.valueSizeEstimator instanceof SampleEstimator) {
        this.sizeEstimator = ((SampleEstimator<V>) SortedAppendOnlyExternalSpillableMap.this.valueSizeEstimator).newInstance();
      } else {
        this.sizeEstimator = SortedAppendOnlyExternalSpillableMap.this.valueSizeEstimator;
      }
      this.file = file;
      try {
        this.reader = new BufferedRandomAccessFile(file, "r", DEFAULT_READ_BUFFER);
      } catch (IOException e) {
        throw new HoodieIOException("Failed to create BufferedRandomAccessFile", e);
      }
    }

    private long sizeEstimate(V record) {
      return sizeEstimator.sizeEstimate(record);
    }

    @Override
    public Iterator<Pair<K, V>> readBatch() {
      if (isEOF) {
        return null;
      }
      // TODO: Suitable initial size
      this.batchRecords = new ArrayList<>();
      while (currentReadBatchSize <= batchSize) {
        // fetch the next record from the file, and add it to the batch
        Option<Entry> entryOpt = readEntry(reader);
        if (entryOpt.isEmpty()) {
          // reach end
          isEOF = true;
          break;
        }
        Entry entry = entryOpt.get();
        V record = (V) serializeFunc.deserialize(entry.getRecord());
        K key = keyGetterFromValue.apply(record);
        batchRecords.add(Pair.of(key, record));
        currentReadBatchSize += sizeEstimate(record);
      }
      if (isEOF && currentReadBatchSize == 0) {
        LOG.debug("SortedEntryFileReader: {} reach end", readerId);
        return null;
      }
      LOG.debug("SortedEntryFileReader: {} read batchId: {} with records num: {}, batch total size: {}", readerId, batchId, batchRecords.size(), currentReadBatchSize);
      batchId++;
      return this.batchRecords.iterator();
    }

    private Option<Entry> readEntry(BufferedRandomAccessFile reader) {
      int magic;
      try {
        magic = reader.readInt();
      } catch (IOException e) {
        // reach end
        return Option.empty();
      }
      if (magic != Entry.MAGIC) {
        throw new HoodieIOException("Invalid magic number");
      }
      try {
        long crc = reader.readLong();
        int recordSize = reader.readInt();
        byte[] record = new byte[recordSize];
        reader.readFully(record, 0, recordSize);
        // check crc
        long crcOfReadValue = BinaryUtil.generateChecksum(record);
        if (crc != crcOfReadValue) {
          throw new HoodieIOException("CRC mismatch");
        }
        return Option.of(new Entry(magic, crc, recordSize, record));
      } catch (IOException e) {
        throw new HoodieIOException("Failed to read entry", e);
      }
    }

    @Override
    public void releaseBatch() {
      this.batchRecords.clear();
      this.currentReadBatchSize = 0;
    }

    @Override
    public void close() throws IOException {
      if (!closed) {
        batchRecords.clear();
        reader.close();
        closed = true;
      }
    }
  }

  private class SortedEntryMemoryReader<V> extends SortedEntryReader {
    private final Iterator<Pair<K, V>> entries;

    public SortedEntryMemoryReader(int readerId, Iterator<Pair<K, V>> memoryEntriesIterator) {
      super(readerId, 0);
      this.entries = memoryEntriesIterator;
    }

    @Override
    public Iterator<Pair<K, V>> readBatch() {
      if (batchId == 0) {
        batchId++;
        return this.entries;
      }
      return null;
    }

    @Override
    public void releaseBatch() {
      // no-op
    }

    @Override
    public void close() throws IOException {
      // no-op
    }
  }

  private class MemoryCombinedMap {
    Map<K, V> map;
    CombineFunc<K, V, V> combineFunc;
    long estimatedAverageKeyValueSize;

    public MemoryCombinedMap(Option<CombineFunc<K, V, V>> func) {
      this.map = new HashMap<K, V>();
      if (func.isEmpty()) {
        this.combineFunc = CombineFunc.defaultCombineFunc();
      } else {
        this.combineFunc = func.get();
      }
    }

    public V insert(K key, V value) {
      map.compute(key, (k, oldValue) -> {
        if (oldValue == null) {
          V initValue = combineFunc.initCombine(key, value);
          sampleSize(key, initValue);
          return initValue;
        } else {
          V combined = combineFunc.combine(key, value, oldValue);
          sampleSize(key, combined);
          return combined;
        }
      });
      return map.get(key);
    }

    private void sampleSize(K k, V v) {
      estimatedAverageKeyValueSize = keySizeEstimator.sizeEstimate(k) + valueSizeEstimator.sizeEstimate(v);
    }

    public V get(K key) {
      return map.get(key);
    }

    public void clear() {
      map.clear();
      map = null;
    }

    public int size() {
      return map.size();
    }

    public long getCurrentMemorySize() {
      return map.size() * estimatedAverageKeyValueSize;
    }

    public Iterator<V> getSortedIterator() {
      return map.values().stream().sorted(comparator).iterator();
    }

    public Iterator<Pair<K, V>> getSortedKVIterator() {
      return map.entrySet().stream().map(e -> Pair.of(e.getKey(), e.getValue())).sorted((pair1, pair2) -> comparator.compare(pair1.getValue(), pair2.getValue())).iterator();
    }
  }

  @Override
  public void close() throws IOException {
    long lifeTime = lifeCycleTimer.endTimer();
    this.totalMemorySize += this.memoryEntries.getCurrentMemorySize();
    this.totalEntryCount += this.memoryEntries.size();
    LOG.info("SortedAppendOnlyExternalSpillableMap closed. {} \n LifeTime: {}", generateLogInfo(), lifeTime);
    this.totalMemorySize = 0;
    this.totalEntryCount = 0;
    this.memoryEntries.clear();
    this.fileManager.clear();
  }

  public String generateLogInfo() {
    return String.format("max memory size => %d, total memory size => %d, current memory size => %d \n"
            + " total entry count => %d, current memory entry count => %d \n"
            + " total sorted file count => %d, total sorted file size => %d .",
        maxMemoryInBytes, totalMemorySize, this.memoryEntries.getCurrentMemorySize(),
        totalEntryCount, this.memoryEntries.size(),
        fileManager.fileCount, fileManager.totalFileSize);
  }

  @Override
  public void clear() {
    long liftTime = lifeCycleTimer.endTimer();
    this.totalMemorySize += this.memoryEntries.getCurrentMemorySize();
    this.totalEntryCount += this.memoryEntries.size();
    LOG.info("SortedAppendOnlyExternalSpillableMap cleared. {} \n LifeTime: {}", generateLogInfo(), liftTime);
    this.totalMemorySize = 0;
    this.totalEntryCount = 0;
    // clear memory entries
    this.memoryEntries.clear();
    // clear spilled files
    this.fileManager.clear();
    // restart the timer
    this.lifeCycleTimer.startTimer();
  }

  @Override
  public int size() {
    return 0;
  }

  @Override
  public boolean isEmpty() {
    return false;
  }

  @Override
  public boolean containsKey(Object key) {
    return false;
  }

  @Override
  public boolean containsValue(Object value) {
    return false;
  }

  @Override
  public V get(Object key) {
    return this.memoryEntries.map.get(key);
  }

  public void spillMemoryEntries() {
    long spilledEntries = this.memoryEntries.size();
    long spilledSize = this.memoryEntries.getCurrentMemorySize();
    // write the sorted entries to disk
    CompletedCallback<File> fileToWriteCb;
    boolean success = false;
    try {
      fileToWriteCb = fileManager.createFileForWrite();
    } catch (IOException e) {
      LOG.error("Failed to create file for write", e);
      throw new HoodieIOException("Failed to create file for write in ExternalSorter", e);
    }
    File fileToWrite = fileToWriteCb.get();
    // write the sorted entries to the file
    spillTimer.startTimer();
    try {
      DiskFileWriter diskFileWriter = new DiskFileWriter(fileToWrite, DEFAULT_WRITE_BUFFER);
      Iterator<V> memoryEntriesSortedIterator = this.memoryEntries.getSortedIterator();
      while (memoryEntriesSortedIterator.hasNext()) {
        diskFileWriter.write(memoryEntriesSortedIterator.next());
      }
      diskFileWriter.close();
      // renew the memory entries
      this.totalMemorySize += this.memoryEntries.getCurrentMemorySize();
      this.totalEntryCount += this.memoryEntries.size();
      this.memoryEntries.clear();
      this.memoryEntries = new MemoryCombinedMap(this.combineFuncOpt);
      success = true;
    } catch (IOException e) {
      LOG.error("Failed to write sorted entries to file", e);
      throw new HoodieIOException("Failed to write sorted entries to file in ExternalSorter", e);
    } finally {
      fileToWriteCb.done(success);
      long diskWriteTaken = spillTimer.endTimer();
      LOG.info("Spill entries result: {} . Spilled {} entries, {} bytes to disk in {} ms",
          success, spilledEntries, spilledSize, diskWriteTaken);
    }
  }

  @Override
  public V put(K key, V value) {
    V newValue = this.memoryEntries.insert(key, value);
    if (this.memoryEntries.getCurrentMemorySize() > this.maxMemoryInBytes) {
      spillMemoryEntries();
    }
    return newValue;
  }

  public V put(V value) {
    return put(keyGetterFromValue.apply(value), value);
  }

  public void putAll(Iterator<V> values) {
    while (values.hasNext()) {
      put(values.next());
    }
  }

  @Override
  public V remove(Object key) {
    throw new HoodieNotSupportedException("Remove is not supported in SortedAppendOnlyExternalSpillableMap");
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    for (Map.Entry<? extends K, ? extends V> entry : m.entrySet()) {
      put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public Set<K> keySet() {
    return Collections.emptySet();
  }

  @Override
  public Collection<V> values() {
    return Collections.emptyList();
  }

  @Override
  public Set<Map.Entry<K, V>> entrySet() {
    return Collections.emptySet();
  }

  interface SerializeFunc<R> {
    byte[] serialize(R record);

    R deserialize(byte[] bytes);
  }
}
