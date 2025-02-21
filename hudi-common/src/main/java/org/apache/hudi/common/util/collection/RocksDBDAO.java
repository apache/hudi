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

import org.apache.hudi.common.serialization.CustomSerializer;
import org.apache.hudi.common.serialization.DefaultSerializer;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.SerializationUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;

import org.rocksdb.AbstractImmutableNativeReference;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Statistics;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;

/**
 * Data access objects for storing and retrieving objects in Rocks DB.
 */
public class RocksDBDAO {

  private static final Logger LOG = LoggerFactory.getLogger(RocksDBDAO.class);

  private transient ConcurrentHashMap<String, ColumnFamilyHandle> managedHandlesMap;
  private transient ConcurrentHashMap<String, ColumnFamilyDescriptor> managedDescriptorMap;
  private transient RocksDB rocksDB;
  private boolean closed = false;
  private final String rocksDBBasePath;
  private final transient Map<String, CustomSerializer<?>> columnFamilySerializers;
  private long totalBytesWritten;

  public RocksDBDAO(String basePath, String rocksDBBasePath) {
    this(basePath, rocksDBBasePath, new HashMap<>());
  }

  public RocksDBDAO(String basePath, String rocksDBBasePath, Map<String, CustomSerializer<?>> columnFamilySerializers) {
    this.rocksDBBasePath =
        String.format("%s/%s/%s", rocksDBBasePath, URI.create(basePath).getPath().replace(":","").replace("/", "_"), UUID.randomUUID());
    this.columnFamilySerializers = columnFamilySerializers;
    init();
    totalBytesWritten = 0L;
  }

  /**
   * Create RocksDB if not initialized.
   */
  private RocksDB getRocksDB() {
    return rocksDB;
  }

  /**
   * Initialized Rocks DB instance.
   */
  private void init() {
    try {
      LOG.info("DELETING RocksDB persisted at " + rocksDBBasePath);
      FileIOUtils.deleteDirectory(new File(rocksDBBasePath));

      managedHandlesMap = new ConcurrentHashMap<>();
      managedDescriptorMap = new ConcurrentHashMap<>();

      // If already present, loads the existing column-family handles

      final DBOptions dbOptions = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true)
          .setWalDir(rocksDBBasePath).setStatsDumpPeriodSec(300).setStatistics(new Statistics());
      dbOptions.setLogger(new org.rocksdb.Logger(dbOptions) {
        @Override
        protected void log(InfoLogLevel infoLogLevel, String logMsg) {
          switch (infoLogLevel) {
            case DEBUG_LEVEL:
              LOG.debug("From Rocks DB : {}", logMsg);
              break;
            case WARN_LEVEL:
              LOG.warn("From Rocks DB : {}", logMsg);
              break;
            case ERROR_LEVEL:
            case FATAL_LEVEL:
              LOG.error("From Rocks DB : {}", logMsg);
              break;
            case HEADER_LEVEL:
            case NUM_INFO_LOG_LEVELS:
            case INFO_LEVEL:
            default:
              LOG.info("From Rocks DB : {}", logMsg);
              break;
          }
        }
      });
      final List<ColumnFamilyDescriptor> managedColumnFamilies = loadManagedColumnFamilies(dbOptions);
      final List<ColumnFamilyHandle> managedHandles = new ArrayList<>(managedColumnFamilies.size());
      FileIOUtils.mkdir(new File(rocksDBBasePath));
      rocksDB = RocksDB.open(dbOptions, rocksDBBasePath, managedColumnFamilies, managedHandles);

      ValidationUtils.checkArgument(managedHandles.size() == managedColumnFamilies.size(),
          "Unexpected number of handles are returned");
      for (int index = 0; index < managedHandles.size(); index++) {
        ColumnFamilyHandle handle = managedHandles.get(index);
        ColumnFamilyDescriptor descriptor = managedColumnFamilies.get(index);
        String familyNameFromHandle = new String(handle.getName());
        String familyNameFromDescriptor = new String(descriptor.getName());

        ValidationUtils.checkArgument(familyNameFromDescriptor.equals(familyNameFromHandle),
            "Family Handles not in order with descriptors");
        managedHandlesMap.put(familyNameFromHandle, handle);
        managedDescriptorMap.put(familyNameFromDescriptor, descriptor);
      }
    } catch (RocksDBException | IOException re) {
      LOG.error("Got exception opening Rocks DB instance ", re);
      throw new HoodieException(re);
    }
  }

  /**
   * Helper to load managed column family descriptors.
   */
  private List<ColumnFamilyDescriptor> loadManagedColumnFamilies(DBOptions dbOptions) throws RocksDBException {
    final List<ColumnFamilyDescriptor> managedColumnFamilies = new ArrayList<>();
    final Options options = new Options(dbOptions, new ColumnFamilyOptions());
    List<byte[]> existing = RocksDB.listColumnFamilies(options, rocksDBBasePath);

    if (existing.isEmpty()) {
      LOG.info("No column family found. Loading default");
      managedColumnFamilies.add(getColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
    } else {
      LOG.info("Loading column families :" + existing.stream().map(String::new).collect(Collectors.toList()));
      managedColumnFamilies
          .addAll(existing.stream().map(RocksDBDAO::getColumnFamilyDescriptor).collect(Collectors.toList()));
    }
    return managedColumnFamilies;
  }

  private static ColumnFamilyDescriptor getColumnFamilyDescriptor(byte[] columnFamilyName) {
    return new ColumnFamilyDescriptor(columnFamilyName, new ColumnFamilyOptions());
  }

  /**
   * Perform a batch write operation.
   */
  public void writeBatch(BatchHandler handler) {
    try (WriteBatch batch = new WriteBatch()) {
      handler.apply(batch);
      getRocksDB().write(new WriteOptions(), batch);
    } catch (RocksDBException re) {
      throw new HoodieException(re);
    }
  }

  /**
   * Helper to add put operation in batch.
   *
   * @param batch Batch Handle
   * @param columnFamilyName Column Family
   * @param key Key
   * @param value Payload
   * @param <T> Type of payload
   */
  public <T extends Serializable> void putInBatch(WriteBatch batch, String columnFamilyName, String key, T value) {
    try {
      byte[] payload = serializePayload(columnFamilyName, value);
      batch.put(managedHandlesMap.get(columnFamilyName), getUTF8Bytes(key), payload);
    } catch (Exception e) {
      throw new HoodieException(e);
    }
  }

  /**
   * Helper to add put operation in batch.
   *
   * @param batch Batch Handle
   * @param columnFamilyName Column Family
   * @param key Key
   * @param value Payload
   * @param <T> Type of payload
   */
  public <K extends Serializable, T> void putInBatch(WriteBatch batch, String columnFamilyName,
      K key, T value) {
    try {
      byte[] keyBytes = SerializationUtils.serialize(key);
      byte[] payload = serializePayload(columnFamilyName, value);
      batch.put(managedHandlesMap.get(columnFamilyName), keyBytes, payload);
    } catch (Exception e) {
      throw new HoodieException(e);
    }
  }

  /**
   * Perform single PUT on a column-family.
   *
   * @param columnFamilyName Column family name
   * @param key Key
   * @param value Payload
   * @param <T> Type of Payload
   */
  public <T extends Serializable> void put(String columnFamilyName, String key, T value) {
    try {
      byte[] payload = serializePayload(columnFamilyName, value);
      getRocksDB().put(managedHandlesMap.get(columnFamilyName), getUTF8Bytes(key), payload);
    } catch (Exception e) {
      throw new HoodieException(e);
    }
  }

  /**
   * Perform single PUT on a column-family.
   *
   * @param columnFamilyName Column family name
   * @param key Key
   * @param value Payload
   * @param <T> Type of Payload
   */
  public <K extends Serializable, T> void put(String columnFamilyName, K key, T value) {
    try {
      byte[] payload = serializePayload(columnFamilyName, value);
      getRocksDB().put(managedHandlesMap.get(columnFamilyName), SerializationUtils.serialize(key), payload);
    } catch (Exception e) {
      throw new HoodieException(e);
    }
  }

  /**
   * Helper to add delete operation in batch.
   *
   * @param batch Batch Handle
   * @param columnFamilyName Column Family
   * @param key Key
   */
  public void deleteInBatch(WriteBatch batch, String columnFamilyName, String key) {
    try {
      batch.delete(managedHandlesMap.get(columnFamilyName), getUTF8Bytes(key));
    } catch (RocksDBException e) {
      throw new HoodieException(e);
    }
  }

  /**
   * Helper to add delete operation in batch.
   *
   * @param batch Batch Handle
   * @param columnFamilyName Column Family
   * @param key Key
   */
  public <K extends Serializable> void deleteInBatch(WriteBatch batch, String columnFamilyName, K key) {
    try {
      batch.delete(managedHandlesMap.get(columnFamilyName), SerializationUtils.serialize(key));
    } catch (Exception e) {
      throw new HoodieException(e);
    }
  }

  /**
   * Perform a single Delete operation.
   *
   * @param columnFamilyName Column Family name
   * @param key Key to be deleted
   */
  public void delete(String columnFamilyName, String key) {
    try {
      getRocksDB().delete(managedHandlesMap.get(columnFamilyName), getUTF8Bytes(key));
    } catch (RocksDBException e) {
      throw new HoodieException(e);
    }
  }

  /**
   * Perform a single Delete operation.
   *
   * @param columnFamilyName Column Family name
   * @param key Key to be deleted
   */
  public <K extends Serializable> void delete(String columnFamilyName, K key) {
    try {
      getRocksDB().delete(managedHandlesMap.get(columnFamilyName), SerializationUtils.serialize(key));
    } catch (Exception e) {
      throw new HoodieException(e);
    }
  }

  /**
   * Retrieve a value for a given key in a column family.
   *
   * @param columnFamilyName Column Family Name
   * @param key Key to be retrieved
   * @param <T> Type of object stored.
   */
  public <T extends Serializable> T get(String columnFamilyName, String key) {
    return get(columnFamilyName, getKeyBytes(key));
  }

  /**
   * Retrieve a value for a given key in a column family.
   *
   * @param columnFamilyName Column Family Name
   * @param key Key to be retrieved
   * @param <T> Type of object stored.
   */
  public <K extends Serializable, T extends Serializable> T get(String columnFamilyName, K key) {
    return get(columnFamilyName, getKeyBytes(key));
  }

  /**
   * Retrieve a value for a given key in a column family.
   *
   * @param columnFamilyName Column Family Name
   * @param key Key to be retrieved
   * @param <T> Type of object stored.
   */
  public <K extends Serializable, T extends Serializable> T get(String columnFamilyName, byte[] key) {
    ValidationUtils.checkArgument(!closed);
    try {
      byte[] val = getRocksDB().get(managedHandlesMap.get(columnFamilyName), key);
      return deserializePayload(columnFamilyName, val);
    } catch (Exception e) {
      throw new HoodieException(e);
    }
  }

  /**
   * Perform a prefix search and return stream of key-value pairs retrieved.
   *
   * @param columnFamilyName Column Family Name
   * @param prefix Prefix Key
   * @param <T> Type of value stored
   */
  public <T extends Serializable> Stream<Pair<String, T>> prefixSearch(String columnFamilyName, String prefix) {
    ValidationUtils.checkArgument(!closed);
    final HoodieTimer timer = HoodieTimer.start();
    long timeTakenMicro = 0;
    List<Pair<String, T>> results = new LinkedList<>();
    try (final RocksIterator it = getRocksDB().newIterator(managedHandlesMap.get(columnFamilyName))) {
      it.seek(getUTF8Bytes(prefix));
      while (it.isValid() && new String(it.key()).startsWith(prefix)) {
        long beginTs = System.nanoTime();
        T val = deserializePayload(columnFamilyName, it.value());
        timeTakenMicro += ((System.nanoTime() - beginTs) / 1000);
        results.add(Pair.of(new String(it.key()), val));
        it.next();
      }
    }

    LOG.info("Prefix Search for (query=" + prefix + ") on " + columnFamilyName + ". Total Time Taken (msec)="
        + timer.endTimer() + ". Serialization Time taken(micro)=" + timeTakenMicro + ", num entries=" + results.size());
    return results.stream();
  }

  /**
   * Return Iterator of key-value pairs from RocksIterator.
   *
   * @param columnFamilyName Column Family Name
   * @param <T>              Type of key stored
   * @param <R>              Type of value stored
   */
  public <T extends Serializable, R> Iterator<Pair<T, R>> iterator(String columnFamilyName) {
    return new IteratorWrapper<>(getRocksDB().newIterator(managedHandlesMap.get(columnFamilyName)), getSerializerForColumnFamily(columnFamilyName));
  }

  /**
   * Perform a prefix delete and return stream of key-value pairs retrieved.
   *
   * @param columnFamilyName Column Family Name
   * @param prefix Prefix Key
   * @param <T> Type of value stored
   */
  public <T extends Serializable> void prefixDelete(String columnFamilyName, String prefix) {
    ValidationUtils.checkArgument(!closed);
    LOG.info("Prefix DELETE (query=" + prefix + ") on " + columnFamilyName);
    final RocksIterator it = getRocksDB().newIterator(managedHandlesMap.get(columnFamilyName));
    it.seek(getUTF8Bytes(prefix));
    // Find first and last keys to be deleted
    String firstEntry = null;
    String lastEntry = null;
    while (it.isValid() && new String(it.key()).startsWith(prefix)) {
      String result = new String(it.key());
      it.next();
      if (firstEntry == null) {
        firstEntry = result;
      }
      lastEntry = result;
    }
    it.close();

    if (null != firstEntry) {
      try {
        // This will not delete the last entry
        getRocksDB().deleteRange(managedHandlesMap.get(columnFamilyName), getUTF8Bytes(firstEntry), getUTF8Bytes(lastEntry));
        // Delete the last entry
        getRocksDB().delete(managedHandlesMap.get(columnFamilyName), getUTF8Bytes(lastEntry));
      } catch (RocksDBException e) {
        LOG.error("Got exception performing range delete");
        throw new HoodieException(e);
      }
    }
  }

  /**
   * Add a new column family to store.
   *
   * @param columnFamilyName Column family name
   */
  public void addColumnFamily(String columnFamilyName) {
    ValidationUtils.checkArgument(!closed);

    managedDescriptorMap.computeIfAbsent(columnFamilyName, colFamilyName -> {
      try {
        ColumnFamilyDescriptor descriptor = getColumnFamilyDescriptor(StringUtils.getUTF8Bytes(colFamilyName));
        ColumnFamilyHandle handle = getRocksDB().createColumnFamily(descriptor);
        managedHandlesMap.put(colFamilyName, handle);
        return descriptor;
      } catch (RocksDBException e) {
        throw new HoodieException(e);
      }
    });
  }

  /**
   * Note : Does not delete from underlying DB. Just closes the handle.
   *
   * @param columnFamilyName Column Family Name
   */
  public void dropColumnFamily(String columnFamilyName) {
    ValidationUtils.checkArgument(!closed);

    managedDescriptorMap.computeIfPresent(columnFamilyName, (colFamilyName, descriptor) -> {
      ColumnFamilyHandle handle = managedHandlesMap.get(colFamilyName);
      try {
        getRocksDB().dropColumnFamily(handle);
        handle.close();
      } catch (RocksDBException e) {
        throw new HoodieException(e);
      }
      managedHandlesMap.remove(columnFamilyName);
      return null;
    });
  }

  /**
   * Close the DAO object.
   */
  public synchronized void close() {
    if (!closed) {
      closed = true;
      managedHandlesMap.values().forEach(AbstractImmutableNativeReference::close);
      managedHandlesMap.clear();
      managedDescriptorMap.clear();
      getRocksDB().close();
      try {
        FileIOUtils.deleteDirectory(new File(rocksDBBasePath));
      } catch (IOException e) {
        throw new HoodieIOException(e.getMessage(), e);
      }
    }
  }

  public long getTotalBytesWritten() {
    return totalBytesWritten;
  }

  private <T> byte[] serializePayload(String columnFamily, T value) throws IOException {
    CustomSerializer<T> serializer = getSerializerForColumnFamily(columnFamily);
    byte[] payload = serializer.serialize(value);
    totalBytesWritten += payload.length;
    return payload;
  }

  private <T> T deserializePayload(String columnFamily, byte[] value) {
    CustomSerializer<T> serializer = getSerializerForColumnFamily(columnFamily);
    if (value == null) {
      return null;
    }
    return serializer.deserialize(value);
  }

  private <T> CustomSerializer<T> getSerializerForColumnFamily(String columnFamily) {
    return (CustomSerializer<T>) columnFamilySerializers.computeIfAbsent(columnFamily, cf -> new DefaultSerializer<>());
  }

  private byte[] getKeyBytes(String key) {
    return getUTF8Bytes(key);
  }

  private <K extends Serializable> byte[] getKeyBytes(K key) {
    try {
      return SerializationUtils.serialize(key);
    } catch (IOException e) {
      throw new HoodieException(e);
    }
  }

  String getRocksDBBasePath() {
    return rocksDBBasePath;
  }

  /**
   * {@link Iterator} wrapper for RocksDb Iterator {@link RocksIterator}.
   */
  private static class IteratorWrapper<T, R> implements Iterator<Pair<T, R>> {

    private final RocksIterator iterator;
    private final CustomSerializer<R> deserializer;

    public IteratorWrapper(final RocksIterator iterator, final CustomSerializer<R> deserializer) {
      this.iterator = iterator;
      this.deserializer = deserializer;
      iterator.seekToFirst();
    }

    @Override
    public boolean hasNext() {
      return iterator.isValid();
    }

    @Override
    public Pair<T, R> next() {
      if (!hasNext()) {
        throw new IllegalStateException("next() called on rocksDB with no more valid entries");
      }
      T key = SerializationUtils.deserialize(iterator.key());
      R val = deserializer.deserialize(iterator.value());
      iterator.next();
      return Pair.of(key, val);
    }
  }

  /**
   * Functional interface for stacking operation to Write batch.
   */
  public interface BatchHandler {

    void apply(WriteBatch batch);
  }
}
