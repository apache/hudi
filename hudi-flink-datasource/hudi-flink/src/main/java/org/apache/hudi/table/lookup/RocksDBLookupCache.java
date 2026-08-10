/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table.lookup;

import org.apache.hudi.common.serialization.CustomSerializer;
import org.apache.hudi.common.util.collection.RocksDBDAO;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Off-heap {@link LookupCache} backed by RocksDB.
 *
 * <p>Data is stored on disk / in the OS page cache, keeping JVM heap usage minimal. Each lookup
 * key maps to one or more full dimension-table rows. The compound RocksDB key encodes the lookup
 * key bytes in hexadecimal plus a monotonic counter so that multiple rows sharing the same lookup
 * key can coexist in the store without overwriting each other. Retrieval is a prefix scan on the
 * hex-encoded lookup key.
 *
 * <p>Keys are serialized with Flink's {@link TypeSerializer} for the key {@link
 * org.apache.flink.table.types.logical.RowType}, and values (full rows) are serialized with the
 * row-level {@link TypeSerializer}. The raw bytes are stored as-is in RocksDB via a
 * pass-through {@link CustomSerializer} — no additional Java-serialization overhead.
 */
@Slf4j
public class RocksDBLookupCache implements LookupCache {

  private static final String COLUMN_FAMILY = "lookup_cache";

  /** Separator between the hex-encoded key and the row counter in the compound RocksDB key. */
  private static final String KEY_SEPARATOR = "_";

  private final TypeSerializer<RowData> keySerializer;
  private final TypeSerializer<RowData> rowSerializer;
  private final String rocksDbBasePath;

  /** Reusable serialization buffer — single-threaded use only. */
  private final DataOutputSerializer keyOutputBuffer = new DataOutputSerializer(64);
  private final DataOutputSerializer rowOutputBuffer = new DataOutputSerializer(256);
  private final DataInputDeserializer rowInputBuffer = new DataInputDeserializer();

  private RocksDBDAO rocksDBDAO;
  private long rowCounter;

  public RocksDBLookupCache(
      TypeSerializer<RowData> keySerializer,
      TypeSerializer<RowData> rowSerializer,
      String rocksDbBasePath) {
    this.keySerializer = keySerializer;
    this.rowSerializer = rowSerializer;
    this.rocksDbBasePath = rocksDbBasePath;
    this.rocksDBDAO = createDAO();
    this.rowCounter = 0L;
  }

  @Override
  public void addRow(RowData key, RowData row) throws IOException {
    String keyHex = serializeKeyToHex(key);
    String compoundKey = keyHex + KEY_SEPARATOR + rowCounter++;
    byte[] valueBytes = serializeRow(row);
    rocksDBDAO.put(COLUMN_FAMILY, compoundKey, valueBytes);
  }

  @Override
  @Nullable
  public List<RowData> getRows(RowData key) throws IOException {
    String prefix = serializeKeyToHex(key) + KEY_SEPARATOR;
    List<byte[]> rawValues = rocksDBDAO.<byte[]>prefixSearch(COLUMN_FAMILY, prefix)
        .map(pair -> pair.getValue())
        .collect(Collectors.toList());
    if (rawValues.isEmpty()) {
      return null;
    }
    List<RowData> result = new ArrayList<>(rawValues.size());
    for (byte[] bytes : rawValues) {
      result.add(deserializeRow(bytes));
    }
    return result;
  }

  @Override
  public void clear() {
    if (rocksDBDAO != null) {
      rocksDBDAO.close();
    }
    rocksDBDAO = createDAO();
    rowCounter = 0L;
    log.debug("RocksDB lookup cache cleared and reinitialized at {}", rocksDbBasePath);
  }

  @Override
  public void close() {
    if (rocksDBDAO != null) {
      rocksDBDAO.close();
      rocksDBDAO = null;
    }
  }

  // -------------------------------------------------------------------------
  //  Internals
  // -------------------------------------------------------------------------

  private RocksDBDAO createDAO() {
    ConcurrentHashMap<String, CustomSerializer<?>> serializers = new ConcurrentHashMap<>();
    serializers.put(COLUMN_FAMILY, new PassThroughSerializer());
    RocksDBDAO dao = new RocksDBDAO("hudi-lookup", rocksDbBasePath, serializers);
    dao.addColumnFamily(COLUMN_FAMILY);
    return dao;
  }

  private String serializeKeyToHex(RowData key) throws IOException {
    keyOutputBuffer.clear();
    keySerializer.serialize(key, keyOutputBuffer);
    return bytesToHex(keyOutputBuffer.getCopyOfBuffer());
  }

  private byte[] serializeRow(RowData row) throws IOException {
    rowOutputBuffer.clear();
    rowSerializer.serialize(row, rowOutputBuffer);
    return rowOutputBuffer.getCopyOfBuffer();
  }

  private RowData deserializeRow(byte[] bytes) throws IOException {
    rowInputBuffer.setBuffer(bytes);
    return rowSerializer.deserialize(rowInputBuffer);
  }

  private static String bytesToHex(byte[] bytes) {
    StringBuilder sb = new StringBuilder(bytes.length * 2);
    for (byte b : bytes) {
      sb.append(String.format("%02x", b));
    }
    return sb.toString();
  }

  /**
   * A {@link CustomSerializer} that treats the serialized form as the raw byte array itself,
   * bypassing Java object serialization overhead. Used to store pre-serialized Flink
   * {@link RowData} bytes directly in RocksDB.
   */
  private static class PassThroughSerializer implements CustomSerializer<byte[]> {
    private static final long serialVersionUID = 1L;

    @Override
    public byte[] serialize(byte[] input) {
      return input;
    }

    @Override
    public byte[] deserialize(byte[] bytes) {
      return bytes;
    }
  }
}
