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
import org.apache.hudi.common.serialization.CustomSerializer;
import org.apache.hudi.common.util.BufferedRandomAccessFile;
import org.apache.hudi.common.util.SerializationUtils;
import org.apache.hudi.common.util.SpillableMapUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieNotSupportedException;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import static org.apache.hudi.common.util.BinaryUtil.generateChecksum;

/**
 * This class provides a disk spillable only map implementation. All of the data is currently written to one file,
 * without any rollover support. It uses the following : 1) An in-memory map that tracks the key-> latest ValueMetadata.
 * 2) Current position in the file NOTE : Only String.class type supported for Key
 * <p>
 * Inspired by https://github.com/basho/bitcask
 */
@Slf4j
public final class BitCaskDiskMap<T extends Serializable, R> extends DiskMap<T, R> {

  public static final int BUFFER_SIZE = 128 * 1024;
  // Caching byte compression/decompression to avoid creating instances for every operation
  private static final ThreadLocal<CompressionHandler> DISK_COMPRESSION_REF =
      ThreadLocal.withInitial(CompressionHandler::new);

  // Stores the key and corresponding value's latest metadata spilled to disk.
  // LinkedHashMap preserves insertion order, which equals disk offset order because the file is
  // append-only and all writes hold mapWriteLock. LazyFileIterator relies on this to iterate in
  // offset order without an extra sort or ArrayList copy (see ENG-43078).
  //
  // LinkedHashMap invariants that must be maintained internally to this class
  // (callers of put/get/remove see only the Map contract; these are NOT caller-facing rules):
  //   (a) accessOrder is false (default constructor) — get() must NOT move entries; only put()
  //       appends to the tail. Never use LinkedHashMap(capacity, loadFactor, accessOrder=true).
  //   (b) Re-insertion: LinkedHashMap.put(k, v) does NOT move an existing key to the tail.
  //       BitCaskDiskMap.put() therefore calls valueMetadataMap.remove(key) before the put()
  //       (under mapWriteLock) so the new, higher disk offset lands at the tail and the
  //       insertion-order == disk-offset-order invariant is preserved. Internal modifications
  //       to valueMetadataMap must do the same — never use compute(), merge(), replace(), or
  //       putIfAbsent() directly on valueMetadataMap; they share this re-insertion pitfall.
  //
  // Thread-safety model: a ReentrantReadWriteLock guards valueMetadataMap.
  //   mapWriteLock (exclusive): held by put(), remove(), clear(), close() for the entire
  //     operation (map update + disk write must be atomic together).
  //   mapReadLock (shared): held by get(), containsKey(), size(), isEmpty(), and snapshot
  //     operations (iterator, valueStream, keySet, entrySet). Multiple concurrent readers are
  //     allowed; disk I/O in get() is performed after releasing mapReadLock to avoid blocking
  //     other readers while waiting on a seek/read.
  // This restores the concurrent-read behaviour that ConcurrentHashMap provided while keeping
  // LinkedHashMap's insertion-order guarantee for lock-free iteration (ENG-43078).
  private final Map<T, ValueMetadata> valueMetadataMap;
  private final ReentrantReadWriteLock mapLock = new ReentrantReadWriteLock();
  private final ReentrantReadWriteLock.ReadLock mapReadLock = mapLock.readLock();
  private final ReentrantReadWriteLock.WriteLock mapWriteLock = mapLock.writeLock();
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
  private final CustomSerializer<R> valueSerializer;

  public BitCaskDiskMap(String baseFilePath, CustomSerializer<R> valueSerializer, boolean isCompressionEnabled) throws IOException {
    super(baseFilePath, ExternalSpillableMap.DiskMapType.BITCASK.name());
    this.valueMetadataMap = new LinkedHashMap<>();
    this.isCompressionEnabled = isCompressionEnabled;
    this.writeOnlyFile = new File(diskMapPath, UUID.randomUUID().toString());
    this.filePath = writeOnlyFile.getPath();
    initFile(writeOnlyFile);
    this.fileOutputStream = new FileOutputStream(writeOnlyFile, true);
    this.writeOnlyFileHandle = new SizeAwareDataOutputStream(fileOutputStream, BUFFER_SIZE);
    this.filePosition = new AtomicLong(0L);
    this.valueSerializer = valueSerializer;
  }

  /**
   * RandomAccessFile is not thread-safe. This API opens a new file handle per thread and returns.
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
    log.debug("Spilling to file location {}", writeOnlyFile.getAbsolutePath());
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
    // Snapshot under mapReadLock so that concurrent puts cannot cause
    // ConcurrentModificationException when the iterator is consumed.
    final List<Map.Entry<T, ValueMetadata>> snapshot;
    mapReadLock.lock();
    try {
      snapshot = new ArrayList<>(valueMetadataMap.entrySet());
    } finally {
      mapReadLock.unlock();
    }
    ClosableIterator<R> iterator = new LazyFileIterable<>(filePath, snapshot, valueSerializer, isCompressionEnabled).iterator();
    this.iterators.add(iterator);
    return iterator;
  }

  /**
   * Custom iterator to iterate over values written to disk with a key filter.
   */
  @Override
  public Iterator<R> iterator(Predicate<T> filter) {
    final List<Map.Entry<T, ValueMetadata>> snapshot;
    mapReadLock.lock();
    try {
      snapshot = valueMetadataMap.entrySet().stream()
          .filter(e -> filter.test(e.getKey()))
          .collect(Collectors.toList());
    } finally {
      mapReadLock.unlock();
    }
    ClosableIterator<R> iterator = new LazyFileIterable<>(filePath, snapshot, valueSerializer, isCompressionEnabled).iterator();
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
    mapReadLock.lock();
    try {
      return valueMetadataMap.size();
    } finally {
      mapReadLock.unlock();
    }
  }

  @Override
  public boolean isEmpty() {
    mapReadLock.lock();
    try {
      return valueMetadataMap.isEmpty();
    } finally {
      mapReadLock.unlock();
    }
  }

  @Override
  public boolean containsKey(Object key) {
    mapReadLock.lock();
    try {
      return valueMetadataMap.containsKey(key);
    } finally {
      mapReadLock.unlock();
    }
  }

  @Override
  public boolean containsValue(Object value) {
    throw new HoodieNotSupportedException("unable to compare values in map");
  }

  @Override
  public R get(Object key) {
    // Hold mapReadLock only for the metadata lookup; release before disk I/O so that
    // concurrent get() calls on different keys are not serialized by the I/O wait.
    ValueMetadata entry;
    mapReadLock.lock();
    try {
      entry = valueMetadataMap.get(key);
    } finally {
      mapReadLock.unlock();
    }
    if (entry == null) {
      return null;
    }
    return get(entry);
  }

  private R get(ValueMetadata entry) {
    return get(entry, getRandomAccessFile(), valueSerializer, isCompressionEnabled);
  }

  public static <V> V get(ValueMetadata entry, RandomAccessFile file, CustomSerializer<V> serializer, boolean isCompressionEnabled) {
    try {
      byte[] bytesFromDisk = SpillableMapUtils.readBytesFromDisk(file, entry.getOffsetOfValue(), entry.getSizeOfValue());
      if (isCompressionEnabled) {
        return serializer.deserialize(DISK_COMPRESSION_REF.get().decompressBytes(bytesFromDisk));
      }
      return serializer.deserialize(bytesFromDisk);
    } catch (IOException e) {
      throw new HoodieIOException("Unable to readFromDisk Hoodie Record from disk", e);
    }
  }

  private R put(T key, R value, boolean flush) {
    try {
      byte[] val = isCompressionEnabled ? DISK_COMPRESSION_REF.get().compressBytes(valueSerializer.serialize(value)) :
          valueSerializer.serialize(value);
      int valueSize = val.length;
      long timestamp = System.currentTimeMillis();
      byte[] serializedKey = SerializationUtils.serialize(key);
      // Hold mapWriteLock for the entire map-update + disk-write to keep them atomic:
      // a concurrent get() must not see a metadata entry whose bytes haven't been flushed yet.
      mapWriteLock.lock();
      try {
        // Remove before re-inserting so the key always lands at the LinkedHashMap tail.
        // LinkedHashMap does not move an existing key on put() — without this remove, a
        // re-inserted key would stay at its original position while its new offset is higher
        // than all entries that follow it, breaking the insertion-order == offset-order invariant.
        // The old bytes remain in the disk file as dead bytes (existing behaviour for remove()).
        valueMetadataMap.remove(key);
        this.valueMetadataMap.put(key,
            new BitCaskDiskMap.ValueMetadata(this.filePath, valueSize, filePosition.get(), timestamp));
        filePosition
            .set(SpillableMapUtils.spillToDisk(writeOnlyFileHandle, new FileEntry(generateChecksum(val),
                serializedKey.length, valueSize, serializedKey, val, timestamp)));
      } finally {
        mapWriteLock.unlock();
      }
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
    ValueMetadata entry;
    mapWriteLock.lock();
    try {
      entry = valueMetadataMap.remove(key);
    } finally {
      mapWriteLock.unlock();
    }
    if (entry == null) {
      return null;
    }
    return get(entry);
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
    // Acquire mapWriteLock so a concurrent reader under mapReadLock (e.g. via
    // SpillableMapBasedFileSystemView's shared readLock) can't observe a half-cleared map.
    mapWriteLock.lock();
    try {
      valueMetadataMap.clear();
    } finally {
      mapWriteLock.unlock();
    }
    // Do not delete file-handles & file as there is no way to do it without synchronizing get/put(and
    // reducing concurrency). Instead, just clear the pointer map. The file will be removed on exit.
  }

  @Override
  public void close() {
    // See clear() for why this needs mapWriteLock.
    mapWriteLock.lock();
    try {
      valueMetadataMap.clear();
    } finally {
      mapWriteLock.unlock();
    }
    try {
      if (writeOnlyFileHandle != null) {
        writeOnlyFileHandle.flush();
        fileOutputStream.getChannel().force(false);
        writeOnlyFileHandle.close();
      }
      fileOutputStream.close();

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
    mapReadLock.lock();
    try {
      return new HashSet<>(valueMetadataMap.keySet());
    } finally {
      mapReadLock.unlock();
    }
  }

  @Override
  public Collection<R> values() {
    throw new HoodieException("Unsupported Operation Exception");
  }

  @Override
  public Stream<R> valueStream() {
    final BufferedRandomAccessFile file = getRandomAccessFile();
    // Snapshot under mapReadLock. LinkedHashMap insertion order equals disk offset order
    // (see put()), so no sort is needed — values are already in forward-read order.
    final List<ValueMetadata> snapshot;
    mapReadLock.lock();
    try {
      snapshot = new ArrayList<>(valueMetadataMap.values());
    } finally {
      mapReadLock.unlock();
    }
    return snapshot.stream().sequential().map(valueMetaData -> (R) get(valueMetaData, file, valueSerializer, isCompressionEnabled));
  }

  @Override
  public Set<Entry<T, R>> entrySet() {
    // Snapshot keys under mapReadLock; disk I/O per-key happens outside the lock.
    final Set<T> keySnapshot;
    mapReadLock.lock();
    try {
      keySnapshot = new HashSet<>(valueMetadataMap.keySet());
    } finally {
      mapReadLock.unlock();
    }
    Set<Entry<T, R>> entrySet = new HashSet<>();
    for (T key : keySnapshot) {
      entrySet.add(new AbstractMap.SimpleEntry<>(key, get(key)));
    }
    return entrySet;
  }

  /**
   * The file metadata that should be spilled to disk.
   */
  public static final class FileEntry {

    // Checksum of the value written to disk, compared during every readFromDisk to make sure no corruption
    private final Long crc;
    // Size (numberOfBytes) of the key written to disk
    private final Integer sizeOfKey;
    // Size (numberOfBytes) of the value written to disk
    private final Integer sizeOfValue;
    // Actual key
    @Getter
    private final byte[] key;
    // Actual value
    @Getter
    private final byte[] value;
    // Current timestamp when the value was written to disk
    private final Long timestamp;

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

    public long getTimestamp() {
      return timestamp;
    }
  }

  /**
   * The value relevant metadata.
   */
  @AllArgsConstructor(access = AccessLevel.PROTECTED)
  @Getter
  public static final class ValueMetadata implements Comparable<ValueMetadata> {

    // FilePath to store the spilled data
    private final String filePath;
    // Size (numberOfBytes) of the value written to disk
    // ENG-43078: primitive fields avoid ~64 B/entry of boxing overhead (Integer + 2 Long wrappers)
    // across the millions of spilled entries held in valueMetadataMap.
    private final int sizeOfValue;
    // FilePosition of the value written to disk
    private final long offsetOfValue;
    // Current timestamp when the value was written to disk
    private final long timestamp;

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
      try (InputStream in = new InflaterInputStream(new ByteArrayInputStream(bytes))) {
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
