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

package org.apache.hudi.common.util;

import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Data access objects for storing and retrieving objects in Rocks DB.
 */
public class RocksDBDAO {

  private static final Logger LOG = LoggerFactory.getLogger(RocksDBDAO.class);

  private transient ConcurrentHashMap<String, ColumnFamilyHandle> managedHandlesMap;
  private transient ConcurrentHashMap<String, ColumnFamilyDescriptor> managedDescriptorMap;
  private transient RocksDB rocksDB;
  private boolean closed = false;
  private final String basePath;
  private final String rocksDBBasePath;

  public RocksDBDAO(String basePath, String rocksDBBasePath) {
    this.basePath = basePath;
    this.rocksDBBasePath =
        String.format("%s/%s/%s", rocksDBBasePath, this.basePath.replace("/", "_"), UUID.randomUUID().toString());
    init();
  }

  /**
   * Create RocksDB if not initialized.
   */
  private RocksDB getRocksDB() {
    if (null == rocksDB) {
      init();
    }
    return rocksDB;
  }

  /**
   * Initialized Rocks DB instance.
   */
  private void init() throws HoodieException {
    try {
      LOG.info("DELETING RocksDB persisted at {}", rocksDBBasePath);
      FileIOUtils.deleteDirectory(new File(rocksDBBasePath));

      managedHandlesMap = new ConcurrentHashMap<>();
      managedDescriptorMap = new ConcurrentHashMap<>();

      // If already present, loads the existing column-family handles

      final DBOptions dbOptions = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true)
          .setWalDir(rocksDBBasePath).setStatsDumpPeriodSec(300).setStatistics(new Statistics());
      dbOptions.setLogger(new org.rocksdb.Logger(dbOptions) {
        @Override
        protected void log(InfoLogLevel infoLogLevel, String logMsg) {
          LOG.info("From Rocks DB : {}", logMsg);
        }
      });
      final List<ColumnFamilyDescriptor> managedColumnFamilies = loadManagedColumnFamilies(dbOptions);
      final List<ColumnFamilyHandle> managedHandles = new ArrayList<>();
      FileIOUtils.mkdir(new File(rocksDBBasePath));
      rocksDB = RocksDB.open(dbOptions, rocksDBBasePath, managedColumnFamilies, managedHandles);

      Preconditions.checkArgument(managedHandles.size() == managedColumnFamilies.size(),
          "Unexpected number of handles are returned");
      for (int index = 0; index < managedHandles.size(); index++) {
        ColumnFamilyHandle handle = managedHandles.get(index);
        ColumnFamilyDescriptor descriptor = managedColumnFamilies.get(index);
        String familyNameFromHandle = new String(handle.getName());
        String familyNameFromDescriptor = new String(descriptor.getName());

        Preconditions.checkArgument(familyNameFromDescriptor.equals(familyNameFromHandle),
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
      LOG.info("Loading column families :{}", existing.stream().map(String::new).collect(Collectors.toList()));
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
    WriteBatch batch = new WriteBatch();
    try {
      handler.apply(batch);
      getRocksDB().write(new WriteOptions(), batch);
    } catch (RocksDBException re) {
      throw new HoodieException(re);
    } finally {
      batch.close();
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
      byte[] payload = SerializationUtils.serialize(value);
      batch.put(managedHandlesMap.get(columnFamilyName), key.getBytes(), payload);
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
  public <K extends Serializable, T extends Serializable> void putInBatch(WriteBatch batch, String columnFamilyName,
      K key, T value) {
    try {
      byte[] keyBytes = SerializationUtils.serialize(key);
      byte[] payload = SerializationUtils.serialize(value);
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
      byte[] payload = SerializationUtils.serialize(value);
      getRocksDB().put(managedHandlesMap.get(columnFamilyName), key.getBytes(), payload);
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
  public <K extends Serializable, T extends Serializable> void put(String columnFamilyName, K key, T value) {
    try {
      byte[] payload = SerializationUtils.serialize(value);
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
      batch.delete(managedHandlesMap.get(columnFamilyName), key.getBytes());
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
      getRocksDB().delete(managedHandlesMap.get(columnFamilyName), key.getBytes());
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
    Preconditions.checkArgument(!closed);
    try {
      byte[] val = getRocksDB().get(managedHandlesMap.get(columnFamilyName), key.getBytes());
      return val == null ? null : SerializationUtils.deserialize(val);
    } catch (RocksDBException e) {
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
  public <K extends Serializable, T extends Serializable> T get(String columnFamilyName, K key) {
    Preconditions.checkArgument(!closed);
    try {
      byte[] val = getRocksDB().get(managedHandlesMap.get(columnFamilyName), SerializationUtils.serialize(key));
      return val == null ? null : SerializationUtils.deserialize(val);
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
    Preconditions.checkArgument(!closed);
    final HoodieTimer timer = new HoodieTimer();
    timer.startTimer();
    long timeTakenMicro = 0;
    List<Pair<String, T>> results = new LinkedList<>();
    try (final RocksIterator it = getRocksDB().newIterator(managedHandlesMap.get(columnFamilyName))) {
      it.seek(prefix.getBytes());
      while (it.isValid() && new String(it.key()).startsWith(prefix)) {
        long beginTs = System.nanoTime();
        T val = SerializationUtils.deserialize(it.value());
        timeTakenMicro += ((System.nanoTime() - beginTs) / 1000);
        results.add(Pair.of(new String(it.key()), val));
        it.next();
      }
    }

    LOG.info("Prefix Search for (query={}) on {}. Total Time Taken (msec)={}. Serialization Time taken(micro)={}, num entries={}", prefix, columnFamilyName, timer.endTimer(), timeTakenMicro,
            results.size());
    return results.stream();
  }

  /**
   * Perform a prefix delete and return stream of key-value pairs retrieved.
   *
   * @param columnFamilyName Column Family Name
   * @param prefix Prefix Key
   * @param <T> Type of value stored
   */
  public <T extends Serializable> void prefixDelete(String columnFamilyName, String prefix) {
    Preconditions.checkArgument(!closed);
    LOG.info("Prefix DELETE (query={}) on {}", prefix, columnFamilyName);
    final RocksIterator it = getRocksDB().newIterator(managedHandlesMap.get(columnFamilyName));
    it.seek(prefix.getBytes());
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
        getRocksDB().deleteRange(managedHandlesMap.get(columnFamilyName), firstEntry.getBytes(), lastEntry.getBytes());
        // Delete the last entry
        getRocksDB().delete(lastEntry.getBytes());
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
    Preconditions.checkArgument(!closed);

    managedDescriptorMap.computeIfAbsent(columnFamilyName, colFamilyName -> {
      try {
        ColumnFamilyDescriptor descriptor = getColumnFamilyDescriptor(colFamilyName.getBytes());
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
    Preconditions.checkArgument(!closed);

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
      managedHandlesMap.values().forEach(columnFamilyHandle -> {
        columnFamilyHandle.close();
      });
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

  @VisibleForTesting
  String getRocksDBBasePath() {
    return rocksDBBasePath;
  }

  /**
   * Functional interface for stacking operation to Write batch.
   */
  public interface BatchHandler {

    void apply(WriteBatch batch);
  }
}
