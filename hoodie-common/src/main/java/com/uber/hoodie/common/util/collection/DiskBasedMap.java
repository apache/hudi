/*
 *  Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.uber.hoodie.common.util.collection;

import com.uber.hoodie.common.util.SpillableMapUtils;
import com.uber.hoodie.common.util.collection.converter.Converter;
import com.uber.hoodie.common.util.collection.io.storage.SizeAwareDataOutputStream;
import com.uber.hoodie.exception.HoodieException;
import com.uber.hoodie.exception.HoodieIOException;
import com.uber.hoodie.exception.HoodieNotSupportedException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * This class provides a disk spillable only map implementation. All of the data is currenly written to one file,
 * without any rollover support. It uses the following : 1) An in-memory map that tracks the key-> latest ValueMetadata.
 * 2) Current position in the file NOTE : Only String.class type supported for Key
 */
public final class DiskBasedMap<T, R> implements Map<T, R> {

  private static final Logger log = LogManager.getLogger(DiskBasedMap.class);
  // Stores the key and corresponding value's latest metadata spilled to disk
  private final Map<T, ValueMetadata> valueMetadataMap;
  // Key converter to convert key type to bytes
  private final Converter<T> keyConverter;
  // Value converter to convert value type to bytes
  private final Converter<R> valueConverter;
  // Read only file access to be able to seek to random positions to readFromDisk values
  private RandomAccessFile readOnlyFileHandle;
  // Write only OutputStream to be able to ONLY append to the file
  private SizeAwareDataOutputStream writeOnlyFileHandle;
  // FileOutputStream for the file handle to be able to force fsync
  // since FileOutputStream's flush() does not force flush to disk
  private FileOutputStream fileOutputStream;
  // Current position in the file
  private AtomicLong filePosition;
  // FilePath to store the spilled data
  private String filePath;


  protected DiskBasedMap(String baseFilePath,
      Converter<T> keyConverter, Converter<R> valueConverter) throws IOException {
    this.valueMetadataMap = new HashMap<>();
    File writeOnlyFileHandle = new File(baseFilePath, UUID.randomUUID().toString());
    this.filePath = writeOnlyFileHandle.getPath();
    initFile(writeOnlyFileHandle);
    this.fileOutputStream = new FileOutputStream(writeOnlyFileHandle, true);
    this.writeOnlyFileHandle = new SizeAwareDataOutputStream(fileOutputStream);
    this.filePosition = new AtomicLong(0L);
    this.keyConverter = keyConverter;
    this.valueConverter = valueConverter;
  }

  private void initFile(File writeOnlyFileHandle) throws IOException {
    // delete the file if it exists
    if (writeOnlyFileHandle.exists()) {
      writeOnlyFileHandle.delete();
    }
    if (!writeOnlyFileHandle.getParentFile().exists()) {
      writeOnlyFileHandle.getParentFile().mkdir();
    }
    writeOnlyFileHandle.createNewFile();
    log.info(
        "Spilling to file location " + writeOnlyFileHandle.getAbsolutePath() + " in host (" + InetAddress.getLocalHost()
            .getHostAddress() + ") with hostname (" + InetAddress.getLocalHost().getHostName() + ")");
    // Open file in readFromDisk-only mode
    readOnlyFileHandle = new RandomAccessFile(filePath, "r");
    readOnlyFileHandle.seek(0);
    // Make sure file is deleted when JVM exits
    writeOnlyFileHandle.deleteOnExit();
    addShutDownHook();
  }

  /**
   * Register shutdown hook to force flush contents of the data written to FileOutputStream from OS page cache
   * (typically 4 KB) to disk
   */
  private void addShutDownHook() {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        try {
          if (writeOnlyFileHandle != null) {
            writeOnlyFileHandle.flush();
            fileOutputStream.getChannel().force(false);
            writeOnlyFileHandle.close();
          }
        } catch (Exception e) {
          // fail silently for any sort of exception
        }
      }
    });
  }

  /**
   * Custom iterator to iterate over values written to disk
   */
  public Iterator<R> iterator() {
    return new LazyFileIterable(readOnlyFileHandle,
        valueMetadataMap, valueConverter).iterator();
  }

  /**
   * Number of bytes spilled to disk
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
    try {
      return this.valueConverter.getData(SpillableMapUtils.readBytesFromDisk(readOnlyFileHandle,
          entry.getOffsetOfValue(), entry.getSizeOfValue()));
    } catch (IOException e) {
      throw new HoodieIOException("Unable to readFromDisk Hoodie Record from disk", e);
    }
  }

  @Override
  public R put(T key, R value) {
    try {
      byte[] val = this.valueConverter.getBytes(value);
      Integer valueSize = val.length;
      Long timestamp = new Date().getTime();
      this.valueMetadataMap.put(key,
          new DiskBasedMap.ValueMetadata(this.filePath, valueSize, filePosition.get(), timestamp));
      byte[] serializedKey = keyConverter.getBytes(key);
      filePosition.set(SpillableMapUtils.spillToDisk(writeOnlyFileHandle,
          new FileEntry(SpillableMapUtils.generateChecksum(val),
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
    // close input/output streams
    try {
      writeOnlyFileHandle.flush();
      writeOnlyFileHandle.close();
      new File(filePath).delete();
    } catch (IOException e) {
      throw new HoodieIOException("unable to clear map or delete file on disk", e);
    }
  }

  @Override
  public Set<T> keySet() {
    return valueMetadataMap.keySet();
  }

  @Override
  public Collection<R> values() {
    throw new HoodieException("Unsupported Operation Exception");
  }

  @Override
  public Set<Entry<T, R>> entrySet() {
    Set<Entry<T, R>> entrySet = new HashSet<>();
    for (T key : valueMetadataMap.keySet()) {
      entrySet.add(new AbstractMap.SimpleEntry<>(key, get(key)));
    }
    return entrySet;
  }

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

    public FileEntry(long crc, int sizeOfKey, int sizeOfValue, byte[] key, byte[] value,
        long timestamp) {
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

  public final class ValueMetadata {

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
  }
}