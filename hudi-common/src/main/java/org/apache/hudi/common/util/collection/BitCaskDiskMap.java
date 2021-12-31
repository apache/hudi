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
import org.apache.hudi.common.util.ClosableIterator;
import org.apache.hudi.common.util.SerializationUtils;
import org.apache.hudi.common.util.SpillableMapUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieNotSupportedException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

/**
 * This class provides a disk spillable only map implementation. All of the data is currenly written to one file,
 * without any rollover support. It uses the following : 1) An in-memory map that tracks the key-> latest ValueMetadata.
 * 2) Current position in the file NOTE : Only String.class type supported for Key
 *
 * Inspired by https://github.com/basho/bitcask
 */
public final class BitCaskDiskMap<T extends Serializable, R extends Serializable> extends DiskMap<T, R> {

  public static final int BUFFER_SIZE = 128 * 1024;  // 128 KB
  private static final Logger LOG = LogManager.getLogger(BitCaskDiskMap.class);
  // Caching byte compression/decompression to avoid creating instances for every operation
  private static final ThreadLocal<CompressionHandler> DISK_COMPRESSION_REF =
      ThreadLocal.withInitial(CompressionHandler::new);

  // Stores the key and corresponding value's latest metadata spilled to disk
  private final Map<T, ValueMetadata> valueMetadataMap;
  // Enables compression for all values stored in the disk map
  private final boolean isCompressionEnabled;
  // Write only file
  private final File writeOnlyFile;
  // Write only OutputStream to be able to ONLY append to the file
  private final SizeAwareDataOutputStream writeOnlyFileHandle;
  // FileOutputStream for the file handle to be able to force fsync
  // since FileOutputStream's flush() does not force flush to disk
  private final FileOutputStream fileOutputStream;
  // Current position in the file
  private final AtomicLong filePosition;
  // FilePath to store the spilled data
  private final String filePath;
  // Thread-safe random access file
  private final ThreadLocal<BufferedRandomAccessFile> randomAccessFile = new ThreadLocal<>();
  private final Queue<BufferedRandomAccessFile> openedAccessFiles = new ConcurrentLinkedQueue<>();

  private final List<ClosableIterator<R>> iterators = new ArrayList<>();

  public BitCaskDiskMap(String baseFilePath, boolean isCompressionEnabled) throws IOException {
    super(baseFilePath, ExternalSpillableMap.DiskMapType.BITCASK.name());
    this.valueMetadataMap = new ConcurrentHashMap<>();
    this.isCompressionEnabled = isCompressionEnabled;
    this.writeOnlyFile = new File(diskMapPath, UUID.randomUUID().toString());
    this.filePath = writeOnlyFile.getPath();
    initFile(writeOnlyFile);
    this.fileOutputStream = new FileOutputStream(writeOnlyFile, true);
    this.writeOnlyFileHandle = new SizeAwareDataOutputStream(fileOutputStream, BUFFER_SIZE);
    this.filePosition = new AtomicLong(0L);
  }

  public BitCaskDiskMap(String baseFilePath) throws IOException {
    this(baseFilePath, false);
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
    LOG.debug("Spilling to file location " + writeOnlyFile.getAbsolutePath());
    // Make sure file is deleted when JVM exits
    writeOnlyFile.deleteOnExit();
  }

  private void flushToDisk() {
    try {
      writeOnlyFileHandle.flush();
    } catch (IOException e) {
      throw new HoodieIOException("Failed to flush to BitCaskDiskMap file", e);
    }
  }

  /**
   * Custom iterator to iterate over values written to disk.
   */
  @Override
  public Iterator<R> iterator() {
    ClosableIterator<R> iterator = new LazyFileIterable(filePath, valueMetadataMap, isCompressionEnabled).iterator();
    this.iterators.add(iterator);
    return iterator;
  }

  /**
   * Number of bytes spilled to disk.
   */
  @Override
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
    return get(entry, getRandomAccessFile(), isCompressionEnabled);
  }

  public static <R> R get(ValueMetadata entry, RandomAccessFile file, boolean isCompressionEnabled) {
    try {
      byte[] bytesFromDisk = SpillableMapUtils.readBytesFromDisk(file, entry.getOffsetOfValue(), entry.getSizeOfValue());
      if (isCompressionEnabled) {
        return SerializationUtils.deserialize(DISK_COMPRESSION_REF.get().decompressBytes(bytesFromDisk));
      }
      return SerializationUtils.deserialize(bytesFromDisk);
    } catch (IOException e) {
      throw new HoodieIOException("Unable to readFromDisk Hoodie Record from disk", e);
    }
  }

  private synchronized R put(T key, R value, boolean flush) {
    try {
      byte[] val = isCompressionEnabled ? DISK_COMPRESSION_REF.get().compressBytes(SerializationUtils.serialize(value)) :
          SerializationUtils.serialize(value);
      Integer valueSize = val.length;
      Long timestamp = System.currentTimeMillis();
      this.valueMetadataMap.put(key,
          new BitCaskDiskMap.ValueMetadata(this.filePath, valueSize, filePosition.get(), timestamp));
      byte[] serializedKey = SerializationUtils.serialize(key);
      filePosition
          .set(SpillableMapUtils.spillToDisk(writeOnlyFileHandle, new FileEntry(SpillableMapUtils.generateChecksum(val),
              serializedKey.length, valueSize, serializedKey, val, timestamp)));
      if (flush) {
        flushToDisk();
      }
    } catch (IOException io) {
      throw new HoodieIOException("Unable to store data in Disk Based map", io);
    }
    return value;
  }

  @Override
  public R put(T key, R value) {
    return put(key, value, true);
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
      put(entry.getKey(), entry.getValue(), false);
    }
    flushToDisk();
  }

  @Override
  public void clear() {
    valueMetadataMap.clear();
    // Do not delete file-handles & file as there is no way to do it without synchronizing get/put(and
    // reducing concurrency). Instead, just clear the pointer map. The file will be removed on exit.
  }

  @Override
  public void close() {
    valueMetadataMap.clear();
    try {
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
      this.iterators.forEach(ClosableIterator::close);
    } catch (Exception e) {
      // delete the file for any sort of exception
      writeOnlyFile.delete();
    } finally {
      super.close();
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
  public Stream<R> valueStream() {
    final BufferedRandomAccessFile file = getRandomAccessFile();
    return valueMetadataMap.values().stream().sorted().sequential().map(valueMetaData -> (R) get(valueMetaData, file, isCompressionEnabled));
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

  private static class CompressionHandler implements Serializable {
    private static final int DISK_COMPRESSION_INITIAL_BUFFER_SIZE = 1048576;
    private static final int DECOMPRESS_INTERMEDIATE_BUFFER_SIZE = 8192;

    // Caching ByteArrayOutputStreams to avoid recreating it for every operation
    private final ByteArrayOutputStream compressBaos;
    private final ByteArrayOutputStream decompressBaos;
    private final byte[] decompressIntermediateBuffer;

    CompressionHandler() {
      compressBaos = new ByteArrayOutputStream(DISK_COMPRESSION_INITIAL_BUFFER_SIZE);
      decompressBaos = new ByteArrayOutputStream(DISK_COMPRESSION_INITIAL_BUFFER_SIZE);
      decompressIntermediateBuffer = new byte[DECOMPRESS_INTERMEDIATE_BUFFER_SIZE];
    }

    private byte[] compressBytes(final byte[] value) throws IOException {
      compressBaos.reset();
      Deflater deflater = new Deflater(Deflater.BEST_COMPRESSION);
      DeflaterOutputStream dos = new DeflaterOutputStream(compressBaos, deflater);
      try {
        dos.write(value);
      } finally {
        dos.close();
        deflater.end();
      }
      return compressBaos.toByteArray();
    }

    private byte[] decompressBytes(final byte[] bytes) throws IOException {
      decompressBaos.reset();
      InputStream in = new InflaterInputStream(new ByteArrayInputStream(bytes));
      try {
        int len;
        while ((len = in.read(decompressIntermediateBuffer)) > 0) {
          decompressBaos.write(decompressIntermediateBuffer, 0, len);
        }
        return decompressBaos.toByteArray();
      } catch (IOException e) {
        throw new HoodieIOException("IOException while decompressing bytes", e);
      }
    }
  }
}
