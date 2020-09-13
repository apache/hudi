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
import org.apache.hudi.common.util.BufferedRandomAccessFile;
import org.apache.hudi.common.util.SerializationUtils;
import org.apache.hudi.common.util.SpillableMapUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieNotSupportedException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

/**
 * This class provides a disk spillable only map implementation. All of the data is currenly written to one file,
 * without any rollover support. It uses the following : 1) An in-memory map that tracks the key-> latest ValueMetadata.
 * 2) Current position in the file NOTE : Only String.class type supported for Key
 */
public final class DiskBasedMap<K extends Serializable, V extends Serializable> implements Map<K, V>, Iterable<V> {

  public static final int BUFFER_SIZE = 128 * 1024;  // 128 KB
  private static final Logger LOG = LogManager.getLogger(DiskBasedMap.class);
  // Stores the key and corresponding value's latest metadata spilled to disk
  private final Map<K, ValueMetadata> valueMetadataMap;
  // Write only file
  private File writeOnlyFile;
  // Write only OutputStream to be able to ONLY append to the file
  private SizeAwareDataOutputStream writeOnlyFileHandle;
  // FileOutputStream for the file handle to be able to force fsync
  // since FileOutputStream's flush() does not force flush to disk
  private FileOutputStream fileOutputStream;
  // Current position in the file
  private AtomicLong filePosition;
  // FilePath to store the spilled data
  private String filePath;
  // Thread-safe random access file
  private ThreadLocal<BufferedRandomAccessFile> randomAccessFile = new ThreadLocal<>();
  private Queue<BufferedRandomAccessFile> openedAccessFiles = new ConcurrentLinkedQueue<>();

  // Locks to control concurrency.
  // Cleanup operations use a write lock read operations use a read lock
  // Adding this to ensure read operations are n
  private final ReentrantReadWriteLock globalLock = new ReentrantReadWriteLock();
  private final ReentrantReadWriteLock.ReadLock readLock = globalLock.readLock();
  private final ReentrantReadWriteLock.WriteLock writeLock = globalLock.writeLock();

  public DiskBasedMap(String baseFilePath) throws IOException {
    this.valueMetadataMap = new ConcurrentHashMap<>();
    this.writeOnlyFile = new File(baseFilePath, UUID.randomUUID().toString());
    this.filePath = writeOnlyFile.getPath();
    initFile(writeOnlyFile);
    this.fileOutputStream = new FileOutputStream(writeOnlyFile, true);
    this.writeOnlyFileHandle = new SizeAwareDataOutputStream(fileOutputStream, BUFFER_SIZE);
    this.filePosition = new AtomicLong(0L);
  }

  /**
   * RandomAcessFile is not thread-safe. This API opens a new file handle per thread and returns.
   * 
   * @return
   */
  private BufferedRandomAccessFile getRandomAccessFile() {
    try {
      BufferedRandomAccessFile readHandle = randomAccessFile.get();
      if (readHandle == null) {
        readHandle = new BufferedRandomAccessFile(filePath, "r", BUFFER_SIZE);
        readHandle.seek(0);
        randomAccessFile.set(readHandle);
        // needed for cleanup
        openedAccessFiles.offer(readHandle);
      }
      return readHandle;
    } catch (IOException ioe) {
      throw new HoodieException(ioe);
    }
  }

  private void initFile(File writeOnlyFile) throws IOException {
    // delete the file if it exists
    if (writeOnlyFile.exists()) {
      writeOnlyFile.delete();
    }
    if (!writeOnlyFile.getParentFile().exists()) {
      writeOnlyFile.getParentFile().mkdir();
    }
    writeOnlyFile.createNewFile();
    LOG.info("Spilling to file location " + writeOnlyFile.getAbsolutePath() + " in host ("
        + InetAddress.getLocalHost().getHostAddress() + ") with hostname (" + InetAddress.getLocalHost().getHostName()
        + ")");
    // Make sure file is deleted when JVM exits
    writeOnlyFile.deleteOnExit();
    addShutDownHook();
  }

  /**
   * Register shutdown hook to force flush contents of the data written to FileOutputStream from OS page cache
   * (typically 4 KB) to disk.
   */
  private void addShutDownHook() {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> cleanup()));
  }

  private void flushToDisk() {
    try {
      writeOnlyFileHandle.flush();
    } catch (IOException e) {
      throw new HoodieIOException("Failed to flush to DiskBasedMap file", e);
    }
  }

  /**
   * Custom iterator to iterate over values written to disk.
   */
  @Override
  public Iterator<V> iterator() {
    try {
      readLock.lock();
      return new LazyFileIterable(filePath, valueMetadataMap).iterator();
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Number of bytes spilled to disk.
   */
  public long sizeOfFileOnDiskInBytes() {
    return filePosition.get();
  }

  @Override
  public int size() {
    return valueMetadataMap.size();
  }

  @Override
  public boolean isEmpty() {
    return valueMetadataMap.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    return valueMetadataMap.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    throw new HoodieNotSupportedException("unable to compare values in map");
  }

  @Override
  public V get(Object key) {
    try {
      readLock.lock();
      ValueMetadata entry = valueMetadataMap.get(key);
      if (entry == null) {
        return null;
      }
      return get(entry);
    } finally {
      readLock.unlock();
    }
  }

  private V get(ValueMetadata entry) {
    return get(entry, getRandomAccessFile());
  }

  public static <R> R get(ValueMetadata entry, RandomAccessFile file) {
    try {
      return SerializationUtils
          .deserialize(SpillableMapUtils.readBytesFromDisk(file, entry.getOffsetOfValue(), entry.getSizeOfValue()));
    } catch (IOException e) {
      throw new HoodieIOException("Unable to readFromDisk Hoodie Record from disk", e);
    }
  }

  private synchronized V put(K key, V value, boolean flush) {
    try {
      if (!writeOnlyFile.exists()) {
        initFile(writeOnlyFile);
      }
      byte[] val = SerializationUtils.serialize(value);
      Integer valueSize = val.length;
      Long timestamp = System.currentTimeMillis();
      Long currentOffset = filePosition.get();
      byte[] serializedKey = SerializationUtils.serialize(key);
      filePosition
          .set(SpillableMapUtils.spillToDisk(writeOnlyFileHandle, new FileEntry(SpillableMapUtils.generateChecksum(val),
              serializedKey.length, valueSize, serializedKey, val, timestamp)));
      // Do not change order to update metadata map after writing to disk
      valueMetadataMap.put(key,
              new DiskBasedMap.ValueMetadata(valueSize, currentOffset, timestamp));
      if (flush) {
        flushToDisk();
      }
    } catch (IOException io) {
      throw new HoodieIOException("Unable to store data in Disk Based map", io);
    }
    return value;
  }

  @Override
  public V put(K key, V value) {
    return put(key, value, true);
  }

  @Override
  public V remove(Object key) {
    V value = get(key);
    valueMetadataMap.remove(key);
    return value;
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    for (Map.Entry<? extends K, ? extends V> entry : m.entrySet()) {
      put(entry.getKey(), entry.getValue(), false);
    }
    flushToDisk();
  }

  private void cleanup() {
    // Clearing valueMetadataMap first would ensure none of the future gets would attempt to read the file/file-handles
    valueMetadataMap.clear();
    try {
      writeLock.lock();
      if (writeOnlyFileHandle != null) {
        writeOnlyFileHandle.flush();
        fileOutputStream.getChannel().force(false);
        writeOnlyFileHandle.close();
      }

      while (!openedAccessFiles.isEmpty()) {
        BufferedRandomAccessFile file = openedAccessFiles.poll();
        if (null != file) {
          try {
            file.close();
          } catch (IOException ioe) {
            // skip exception
          }
        }
      }
      writeOnlyFile.delete();
      randomAccessFile = new ThreadLocal<>();
    } catch (Exception e) {
      // delete the file for any sort of exception
      writeOnlyFile.delete();
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public synchronized void clear() throws HoodieException {
    // Clear is synchronized alongside put
    // Get and clear have a readwrite lock to maintain synchronization
    cleanup();
  }

  @Override
  public Set<K> keySet() {
    return valueMetadataMap.keySet();
  }

  @Override
  public Collection<V> values() {
    throw new HoodieException("Unsupported Operation Exception");
  }

  public Stream<V> valueStream() {
    try {
      readLock.lock();
      return valueMetadataMap.values().stream().sorted().sequential().map(valueMetaData -> (V) get(valueMetaData));
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    Set<Entry<K, V>> entrySet = new HashSet<>();
    for (K key : valueMetadataMap.keySet()) {
      entrySet.add(new AbstractMap.SimpleEntry<>(key, get(key)));
    }
    return entrySet;
  }

  /**
   * The file metadata that should be spilled to disk.
   */
  public static final class FileEntry {

    // Checksum of the value written to disk, compared during every readFromDisk to make sure no corruption
    private Long crc;
    // Size (numberOfBytes) of the key written to disk
    private Integer sizeOfKey;
    // Size (numberOfBytes) of the value written to disk
    private Integer sizeOfValue;
    // Actual key
    private byte[] key;
    // Actual value
    private byte[] value;
    // Current timestamp when the value was written to disk
    private Long timestamp;

    public FileEntry(long crc, int sizeOfKey, int sizeOfValue, byte[] key, byte[] value, long timestamp) {
      this.crc = crc;
      this.sizeOfKey = sizeOfKey;
      this.sizeOfValue = sizeOfValue;
      this.key = key;
      this.value = value;
      this.timestamp = timestamp;
    }

    public long getCrc() {
      return crc;
    }

    public int getSizeOfKey() {
      return sizeOfKey;
    }

    public int getSizeOfValue() {
      return sizeOfValue;
    }

    public byte[] getKey() {
      return key;
    }

    public byte[] getValue() {
      return value;
    }

    public long getTimestamp() {
      return timestamp;
    }
  }

  /**
   * The value relevant metadata.
   */
  public static final class ValueMetadata implements Comparable<ValueMetadata> {
    // Size (numberOfBytes) of the value written to disk
    private Integer sizeOfValue;
    // FilePosition of the value written to disk
    private Long offsetOfValue;
    // Current timestamp when the value was written to disk
    private Long timestamp;

    protected ValueMetadata(int sizeOfValue, long offsetOfValue, long timestamp) {
      this.sizeOfValue = sizeOfValue;
      this.offsetOfValue = offsetOfValue;
      this.timestamp = timestamp;
    }

    public int getSizeOfValue() {
      return sizeOfValue;
    }

    public Long getOffsetOfValue() {
      return offsetOfValue;
    }

    public long getTimestamp() {
      return timestamp;
    }

    @Override
    public int compareTo(ValueMetadata o) {
      return Long.compare(offsetOfValue, o.offsetOfValue);
    }
  }
}
