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

import org.apache.hudi.common.util.SerializationUtils;
import org.apache.hudi.common.util.SpillableMapUtils;
import org.apache.hudi.common.util.collection.io.storage.SizeAwareDataOutputStream;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieNotSupportedException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.stream.Stream;

/**
 * This class provides a disk spillable only map implementation. All of the data is currenly written to one file,
 * without any rollover support. It uses the following : 1) An in-memory map that tracks the key-> latest ValueMetadata.
 * 2) Current position in the file NOTE : Only String.class type supported for Key
 */
public final class DiskBasedMap<T extends Serializable, R extends Serializable> implements Map<T, R>, Iterable<R> {

  private static final Logger LOG = LoggerFactory.getLogger(DiskBasedMap.class);
  // Stores the key and corresponding value's latest metadata spilled to disk
  private final Map<T, ValueMetadata> valueMetadataMap;
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
  private ThreadLocal<RandomAccessFile> randomAccessFile = new ThreadLocal<>();
  private Queue<RandomAccessFile> openedAccessFiles = new ConcurrentLinkedQueue<>();

  public DiskBasedMap(String baseFilePath) throws IOException {
    this.valueMetadataMap = new ConcurrentHashMap<>();
    this.writeOnlyFile = new File(baseFilePath, UUID.randomUUID().toString());
    this.filePath = writeOnlyFile.getPath();
    initFile(writeOnlyFile);
    this.fileOutputStream = new FileOutputStream(writeOnlyFile, true);
    this.writeOnlyFileHandle = new SizeAwareDataOutputStream(fileOutputStream);
    this.filePosition = new AtomicLong(0L);
  }

  /**
   * RandomAcessFile is not thread-safe. This API opens a new file handle per thread and returns.
   * 
   * @return
   */
  private RandomAccessFile getRandomAccessFile() {
    try {
      RandomAccessFile readHandle = randomAccessFile.get();
      if (readHandle == null) {
        readHandle = new RandomAccessFile(filePath, "r");
        readHandle.seek(0);
        randomAccessFile.set(readHandle);
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
    LOG.info("Spilling to file location {} in host ({}) with hostname ({})", writeOnlyFile.getAbsolutePath(), InetAddress.getLocalHost().getHostAddress(), InetAddress.getLocalHost().getHostName());
    // Make sure file is deleted when JVM exits
    writeOnlyFile.deleteOnExit();
    addShutDownHook();
  }

  /**
   * Register shutdown hook to force flush contents of the data written to FileOutputStream from OS page cache
   * (typically 4 KB) to disk.
   */
  private void addShutDownHook() {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          if (writeOnlyFileHandle != null) {
            writeOnlyFileHandle.flush();
            fileOutputStream.getChannel().force(false);
            writeOnlyFileHandle.close();
          }

          while (!openedAccessFiles.isEmpty()) {
            RandomAccessFile file = openedAccessFiles.poll();
            if (null != file) {
              try {
                file.close();
              } catch (IOException ioe) {
                // skip exception
              }
            }
          }
          writeOnlyFile.delete();
        } catch (Exception e) {
          // delete the file for any sort of exception
          writeOnlyFile.delete();
        }
      }
    });
  }

  /**
   * Custom iterator to iterate over values written to disk.
   */
  @Override
  public Iterator<R> iterator() {
    return new LazyFileIterable(filePath, valueMetadataMap).iterator();
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
  public R get(Object key) {
    ValueMetadata entry = valueMetadataMap.get(key);
    if (entry == null) {
      return null;
    }
    return get(entry);
  }

  private R get(ValueMetadata entry) {
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

  @Override
  public synchronized R put(T key, R value) {
    try {
      byte[] val = SerializationUtils.serialize(value);
      Integer valueSize = val.length;
      Long timestamp = System.currentTimeMillis();
      this.valueMetadataMap.put(key,
          new DiskBasedMap.ValueMetadata(this.filePath, valueSize, filePosition.get(), timestamp));
      byte[] serializedKey = SerializationUtils.serialize(key);
      filePosition
          .set(SpillableMapUtils.spillToDisk(writeOnlyFileHandle, new FileEntry(SpillableMapUtils.generateChecksum(val),
              serializedKey.length, valueSize, serializedKey, val, timestamp)));
    } catch (IOException io) {
      throw new HoodieIOException("Unable to store data in Disk Based map", io);
    }
    return value;
  }

  @Override
  public R remove(Object key) {
    R value = get(key);
    valueMetadataMap.remove(key);
    return value;
  }

  @Override
  public void putAll(Map<? extends T, ? extends R> m) {
    for (Map.Entry<? extends T, ? extends R> entry : m.entrySet()) {
      put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public void clear() {
    valueMetadataMap.clear();
    // Do not delete file-handles & file as there is no way to do it without synchronizing get/put(and
    // reducing concurrency). Instead, just clear the pointer map. The file will be removed on exit.
  }

  @Override
  public Set<T> keySet() {
    return valueMetadataMap.keySet();
  }

  @Override
  public Collection<R> values() {
    throw new HoodieException("Unsupported Operation Exception");
  }

  public Stream<R> valueStream() {
    final RandomAccessFile file = getRandomAccessFile();
    return valueMetadataMap.values().stream().sorted().sequential().map(valueMetaData -> (R) get(valueMetaData, file));
  }

  @Override
  public Set<Entry<T, R>> entrySet() {
    Set<Entry<T, R>> entrySet = new HashSet<>();
    for (T key : valueMetadataMap.keySet()) {
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

    // FilePath to store the spilled data
    private String filePath;
    // Size (numberOfBytes) of the value written to disk
    private Integer sizeOfValue;
    // FilePosition of the value written to disk
    private Long offsetOfValue;
    // Current timestamp when the value was written to disk
    private Long timestamp;

    protected ValueMetadata(String filePath, int sizeOfValue, long offsetOfValue, long timestamp) {
      this.filePath = filePath;
      this.sizeOfValue = sizeOfValue;
      this.offsetOfValue = offsetOfValue;
      this.timestamp = timestamp;
    }

    public String getFilePath() {
      return filePath;
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
      return Long.compare(this.offsetOfValue, o.offsetOfValue);
    }
  }
}
