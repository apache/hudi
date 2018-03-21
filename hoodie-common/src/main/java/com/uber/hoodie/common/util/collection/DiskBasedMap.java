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

import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.util.HoodieAvroUtils;
import com.uber.hoodie.common.util.SpillableMapUtils;
import com.uber.hoodie.common.util.collection.io.storage.SizeAwareDataOutputStream;
import com.uber.hoodie.exception.HoodieException;
import com.uber.hoodie.exception.HoodieIOException;
import com.uber.hoodie.exception.HoodieNotSupportedException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

/**
 * This class provides a disk spillable only map implementation. All of the data is currenly written
 * to one file, without any rollover support. It uses the following : 1) An in-memory map that
 * tracks the key-> latest ValueMetadata. 2) Current position in the file NOTE : Only String.class
 * type supported for Key
 */
public final class DiskBasedMap<T, R> implements Map<T, R> {

  // Stores the key and corresponding value's latest metadata spilled to disk
  private final Map<T, ValueMetadata> inMemoryMetadataOfSpilledData;
  // Read only file access to be able to seek to random positions to readFromDisk values
  private RandomAccessFile readOnlyFileHandle;
  // Write only OutputStream to be able to ONLY append to the file
  private SizeAwareDataOutputStream writeOnlyFileHandle;
  // FileOutputStream for the file handle to be able to force fsync
  // since FileOutputStream's flush() does not force flush to disk
  private FileOutputStream fileOutputStream;
  // Current position in the file
  private AtomicLong filePosition;
  // Schema used to de-serialize payload written to disk
  private Schema schema;
  // Class used to de-serialize/realize payload written to disk
  private String payloadClazz;
  // FilePath to store the spilled data
  private String filePath;
  // Default file path prefix to put the spillable file
  private static String DEFAULT_BASE_FILE_PATH = "/tmp/";

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

  public static final class FileEntry {

    // Checksum of the value written to disk, compared during every readFromDisk to make sure no
    // corruption
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

  protected DiskBasedMap(Schema schema, String payloadClazz, Optional<String> baseFilePath)
      throws IOException {
    this.inMemoryMetadataOfSpilledData = new HashMap<>();

    if (!baseFilePath.isPresent()) {
      baseFilePath = Optional.of(DEFAULT_BASE_FILE_PATH);
    }
    this.filePath = baseFilePath.get() + UUID.randomUUID().toString();
    File writeOnlyFileHandle = new File(filePath);
    initFile(writeOnlyFileHandle);

    this.fileOutputStream = new FileOutputStream(writeOnlyFileHandle, true);
    this.writeOnlyFileHandle = new SizeAwareDataOutputStream(fileOutputStream);
    this.filePosition = new AtomicLong(0L);
    this.schema = schema;
    this.payloadClazz = payloadClazz;
  }

  private void initFile(File writeOnlyFileHandle) throws IOException {
    // delete the file if it exists
    if (writeOnlyFileHandle.exists()) {
      writeOnlyFileHandle.delete();
    }
    writeOnlyFileHandle.createNewFile();
    // Open file in readFromDisk-only mode
    readOnlyFileHandle = new RandomAccessFile(filePath, "r");
    readOnlyFileHandle.seek(0);
    // Make sure file is deleted when JVM exits
    writeOnlyFileHandle.deleteOnExit();
    addShutDownHook();
  }

  /**
   * Register shutdown hook to force flush contents of the data written to FileOutputStream from OS
   * page cache (typically 4 KB) to disk
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
        inMemoryMetadataOfSpilledData, schema, payloadClazz).iterator();
  }

  /**
   * Number of bytes spilled to disk
   */
  public long sizeOfFileOnDiskInBytes() {
    return filePosition.get();
  }

  @Override
  public int size() {
    return inMemoryMetadataOfSpilledData.size();
  }

  @Override
  public boolean isEmpty() {
    return inMemoryMetadataOfSpilledData.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    return inMemoryMetadataOfSpilledData.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    throw new HoodieNotSupportedException("unable to compare values in map");
  }

  @Override
  public R get(Object key) {
    ValueMetadata entry = inMemoryMetadataOfSpilledData.get(key);
    if (entry == null) {
      return null;
    }
    try {
      return SpillableMapUtils.readFromDisk(readOnlyFileHandle, schema,
          payloadClazz, entry.getOffsetOfValue(), entry.getSizeOfValue());
    } catch (IOException e) {
      throw new HoodieIOException("Unable to readFromDisk Hoodie Record from disk", e);
    }
  }

  @Override
  public R put(T key, R value) {
    //TODO (na) : check value instanceof HoodieRecordPayload, now assume every payload is
    // HoodieRecord
    HoodieRecord payload = (HoodieRecord) value;
    try {
      byte[] val = HoodieAvroUtils
          .avroToBytes((GenericRecord) payload.getData().getInsertValue(this.schema).get());
      Integer valueSize = val.length;
      Long timestamp = new Date().getTime();
      this.inMemoryMetadataOfSpilledData
          .put(key, new DiskBasedMap.ValueMetadata(this.filePath, valueSize,
              filePosition.get(), timestamp));
      // TODO(na) : Test serializer performance for generic types
      String serializedKey = SpillableMapUtils.objectMapper.writeValueAsString(key);
      filePosition.set(SpillableMapUtils.spillToDisk(writeOnlyFileHandle,
          new FileEntry(SpillableMapUtils.generateChecksum(val),
              serializedKey.getBytes().length, valueSize, serializedKey.getBytes(), val,
              timestamp)));
    } catch (IOException io) {
      throw new HoodieIOException("Unable to store data in Disk Based map", io);
    }
    return value;
  }

  @Override
  public R remove(Object key) {
    R value = get(key);
    inMemoryMetadataOfSpilledData.remove(key);
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
    inMemoryMetadataOfSpilledData.clear();
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
    return inMemoryMetadataOfSpilledData.keySet();
  }

  @Override
  public Collection<R> values() {
    throw new HoodieException("Unsupported Operation Exception");
  }

  @Override
  public Set<Entry<T, R>> entrySet() {
    Set<Entry<T, R>> entrySet = new HashSet<>();
    for (T key : inMemoryMetadataOfSpilledData.keySet()) {
      entrySet.add(new AbstractMap.SimpleEntry<>(key, get(key)));
    }
    return entrySet;
  }
}