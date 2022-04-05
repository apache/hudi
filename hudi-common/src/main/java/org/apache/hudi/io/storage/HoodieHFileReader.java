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

package org.apache.hudi.io.storage;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileInfo;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.ClosableIterator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.io.ByteBufferBackedInputStream;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.CollectionUtils.toStream;

public class HoodieHFileReader<R extends IndexedRecord> implements HoodieFileReader<R> {
  public static final String SCHEMA_KEY = "schema";

  // TODO HoodieHFileReader right now tightly coupled to MT, we should break that coupling
  public static final String KEY_FIELD_NAME = "key";
  public static final String KEY_BLOOM_FILTER_META_BLOCK = "bloomFilter";
  public static final String KEY_BLOOM_FILTER_TYPE_CODE = "bloomFilterTypeCode";
  public static final String KEY_MIN_RECORD = "minRecordKey";
  public static final String KEY_MAX_RECORD = "maxRecordKey";

  private static final Logger LOG = LogManager.getLogger(HoodieHFileReader.class);

  private final Path path;
  private final Configuration conf;
  private final HFile.Reader reader;
  // TODO make final
  private Schema schema;

  public HoodieHFileReader(Configuration configuration, Path path, CacheConfig cacheConfig) throws IOException {
    this.conf = configuration;
    this.path = path;
    this.reader = HoodieHFileUtils.createHFileReader(FSUtils.getFs(path.toString(), configuration), path, cacheConfig, conf);
  }

  public HoodieHFileReader(Configuration configuration, Path path, CacheConfig cacheConfig, FileSystem fs) throws IOException {
    this.conf = configuration;
    this.path = path;
    this.reader = HoodieHFileUtils.createHFileReader(fs, path, cacheConfig, configuration);
  }

  public HoodieHFileReader(FileSystem fs, Path dummyPath, byte[] content) throws IOException {
    this.reader = HoodieHFileUtils.createHFileReader(fs, dummyPath, content);
    this.path = null;
    this.conf = null;
  }

  @Override
  public String[] readMinMaxRecordKeys() {
    HFileInfo fileInfo = reader.getHFileInfo();
    return new String[] {new String(fileInfo.get(KEY_MIN_RECORD.getBytes())),
        new String(fileInfo.get(KEY_MAX_RECORD.getBytes()))};
  }

  @Override
  public Schema getSchema() {
    if (schema == null) {
      HFileInfo fileInfo = reader.getHFileInfo();
      schema = new Schema.Parser().parse(new String(fileInfo.get(SCHEMA_KEY.getBytes())));
    }

    return schema;
  }

  /**
   * Sets up the writer schema explicitly.
   */
  public void withSchema(Schema schema) {
    this.schema = schema;
  }

  @Override
  public BloomFilter readBloomFilter() {
    HFileInfo fileInfo;
    try {
      fileInfo = reader.getHFileInfo();
      ByteBuff serializedFilter = reader.getMetaBlock(KEY_BLOOM_FILTER_META_BLOCK, false).getBufferWithoutHeader();
      byte[] filterBytes = new byte[serializedFilter.remaining()];
      serializedFilter.get(filterBytes); // read the bytes that were written
      return BloomFilterFactory.fromString(new String(filterBytes),
          new String(fileInfo.get(KEY_BLOOM_FILTER_TYPE_CODE.getBytes())));
    } catch (IOException e) {
      throw new HoodieException("Could not read bloom filter from " + path, e);
    }
  }

  /**
   * Filter keys by availability.
   * <p>
   * Note: This method is performant when the caller passes in a sorted candidate keys.
   *
   * @param candidateRowKeys - Keys to check for the availability
   * @return Subset of candidate keys that are available
   */
  @Override
  public Set<String> filterRowKeys(Set<String> candidateRowKeys) {
    HFileScanner hFileScanner = getHFileScannerUnchecked();
    return candidateRowKeys.stream().filter(k -> {
      try {
        return isKeyAvailable(k, hFileScanner);
      } catch (IOException e) {
        LOG.error("Failed to check key availability: " + k);
        return false;
      }
    }).collect(Collectors.toSet());
  }

  @Override
  public ClosableIterator<R> getRecordsByKeyPrefixIterator(List<String> keyPrefixes, Schema readerSchema) throws IOException {
    this.schema = readerSchema;
    return new RecordByKeyPrefixIterator(keyPrefixes, readerSchema);
  }

  @Override
  public ClosableIterator<R> getRecordsByKeysIterator(List<String> keys, Schema schema) throws IOException {
    return new RecordByKeyIterator(keys, schema);
  }

  @Override
  public ClosableIterator<R> getRecordIterator(Schema readerSchema) throws IOException {
    return new RecordIterator(readerSchema);
  }

  @Override
  public Option<R> getRecordByKey(String key, Schema readerSchema) throws IOException {
    HFileScanner hFileScanner = getHFileScanner();
    return getRecordByKeyInternal(key, readerSchema, hFileScanner);
  }

  private Map<String, R> getRecordsByKeyPrefixesInternal(List<String> keyPrefixes, HFileScanner hFileScanner, Schema readerSchema) throws IOException {
    // NOTE: It's always beneficial to sort keys being sought to by HFile reader
    //       to avoid seeking back and forth
    Collections.sort(keyPrefixes);

    List<Pair<byte[], byte[]>> keyRecordsBytes = new ArrayList<>(keyPrefixes.size());
    for (String keyPrefix : keyPrefixes) {
      KeyValue kv = new KeyValue(keyPrefix.getBytes(), null, null, null);

      // NOTE: HFile persists both keys/values as bytes, therefore lexicographical sorted is
      //       essentially employed
      //
      // For the HFile containing list of cells c[0], c[1], ..., c[N], `seekTo(cell)` would return
      // following:
      //    a) -1, if cell < c[0], no position;
      //    b) 0, such that c[i] = cell and scanner is left in position i;
      //    c) and 1, such that c[i] < cell, and scanner is left in position i.
      //
      // Consider entries w/ the following keys in HFile: [key01, key02, key03, key04,..., key20];
      // In case looked up key-prefix is
      //    - "key", `seekTo()` will return -1 and place the cursor just before "key01",
      //    `getCell()` will return "key01" entry
      //    - "key03", `seekTo()` will return 0 (exact match) and place the cursor just before "key03",
      //    `getCell()` will return "key03" entry
      //    - "key1", `seekTo()` will return 1 (first not lower than) and place the cursor just before
      //    "key10" (i.e. on "key09");
      //
      int val = hFileScanner.seekTo(kv);
      if (val == 1) {
        // Try moving to next entry, matching the prefix key; if we're at the EOF,
        // `next()` will return false
        if (!hFileScanner.next()) {
          break;
        }
      }

      do {
        Cell c = hFileScanner.getCell();
        byte[] keyBytes = copyKeyFromCell(c);
        String key = new String(keyBytes);
        // Check whether we're still reading records corresponding to the key-prefix
        if (!key.startsWith(keyPrefix)) {
          break;
        }

        // Extract the byte value before releasing the lock since we cannot hold on to the returned cell afterwards
        byte[] valueBytes = copyValueFromCell(c);
        keyRecordsBytes.add(Pair.newPair(keyBytes, valueBytes));
      } while (hFileScanner.next());
    }

    // Use `TreeMap` so that returned entries are sorted
    Map<String, R> values = new TreeMap<>();
    for (Pair<byte[], byte[]> kv : keyRecordsBytes) {
      R record = deserialize(kv.getFirst(), kv.getSecond(), getSchema(), readerSchema);
      values.put(new String(kv.getFirst()), record);
    }

    return values;
  }

  private boolean isKeyAvailable(String key, HFileScanner keyScanner) throws IOException {
    final KeyValue kv = new KeyValue(key.getBytes(), null, null, null);
    return keyScanner.seekTo(kv) == 0;
  }

  private Option<R> getRecordByKeyInternal(String key, Schema readerSchema, HFileScanner hFileScanner) throws IOException {
    KeyValue kv = new KeyValue(key.getBytes(), null, null, null);
    if (hFileScanner.seekTo(kv) != 0) {
      return Option.empty();
    }

    Cell c = hFileScanner.getCell();
    byte[] valueBytes = copyValueFromCell(c);
    R record = deserialize(key.getBytes(), valueBytes, getSchema(), readerSchema);

    return Option.of(record);
  }

  private Pair<String, R> getRecordFromCell(Cell cell, Schema writerSchema, Schema readerSchema) throws IOException {
    final byte[] keyBytes = copyKeyFromCell(cell);
    final byte[] valueBytes = copyValueFromCell(cell);
    R record = deserialize(keyBytes, valueBytes, writerSchema, readerSchema);
    return new Pair<>(new String(keyBytes), record);
  }

  private static byte[] copyKeyFromCell(Cell cell) {
    return Arrays.copyOfRange(cell.getRowArray(), cell.getRowOffset(), cell.getRowOffset() + cell.getRowLength());
  }

  private static byte[] copyValueFromCell(Cell c) {
    return Arrays.copyOfRange(c.getValueArray(), c.getValueOffset(), c.getValueOffset() + c.getValueLength());
  }

  /**
   * Deserialize the record byte array contents to record object.
   *
   * @param keyBytes       - Record key as byte array
   * @param valueBytes     - Record content as byte array
   * @param writerSchema   - Writer schema
   * @param readerSchema   - Reader schema
   * @return Deserialized record object
   */
  private R deserialize(final byte[] keyBytes, final byte[] valueBytes, Schema writerSchema, Schema readerSchema) throws IOException {
    R record = (R) HoodieAvroUtils.bytesToAvro(valueBytes, writerSchema, readerSchema);

    getKeySchema(readerSchema).ifPresent(keyFieldSchema -> {
      final Object keyObject = record.get(keyFieldSchema.pos());
      if (keyObject != null && keyObject.toString().isEmpty()) {
        record.put(keyFieldSchema.pos(), new String(keyBytes));
      }
    });

    return record;
  }

  @Override
  public long getTotalRecords() {
    return reader.getEntries();
  }

  @Override
  public synchronized void close() {
    try {
      reader.close();
    } catch (IOException e) {
      throw new HoodieIOException("Error closing the hfile reader", e);
    }
  }

  /**
   * NOTE: THIS SHOULD ONLY BE USED FOR TESTING, RECORDS ARE MATERIALIZED EAGERLY
   *
   * Reads all the records with given schema
   */
  public static <R extends IndexedRecord> List<R> readAllRecords(HoodieHFileReader<R> reader) throws IOException {
    Schema schema = reader.getSchema();
    return toStream(reader.getRecordIterator(schema))
        .collect(Collectors.toList());
  }

  /**
   * NOTE: THIS SHOULD ONLY BE USED FOR TESTING, RECORDS ARE MATERIALIZED EAGERLY
   *
   * Reads all the records with given schema and filtering keys.
   */
  public static <R extends IndexedRecord> List<R> readRecords(HoodieHFileReader<R> reader,
                                                              List<String> keys) throws IOException {
    return readRecords(reader, keys, reader.getSchema());
  }

  /**
   * NOTE: THIS SHOULD ONLY BE USED FOR TESTING, RECORDS ARE MATERIALIZED EAGERLY
   *
   * Reads all the records with given schema and filtering keys.
   */
  public static <R extends IndexedRecord> List<R> readRecords(HoodieHFileReader<R> reader,
                                                              List<String> keys,
                                                              Schema schema) throws IOException {
    Collections.sort(keys);
    return toStream(reader.getRecordsByKeysIterator(keys, schema))
        .collect(Collectors.toList());
  }

  @Nonnull
  private HFileScanner getHFileScanner() throws IOException {
    HFileScanner hFileScanner = reader.getScanner(false, false);
    hFileScanner.seekTo(); // seek to beginning of file.
    return hFileScanner;
  }

  @Nonnull
  private HFileScanner getHFileScannerUnchecked() {
    try {
      return getHFileScanner();
    } catch (IOException e) {
      throw new HoodieIOException("Cannot seek to beginning of HFile " + reader.getHFileInfo().toString(), e);
    }
  }

  private static Option<Schema.Field> getKeySchema(Schema schema) {
    return Option.ofNullable(schema.getField(KEY_FIELD_NAME));
  }

  class RecordByKeyPrefixIterator implements ClosableIterator<R> {
    private final Iterator<String> keyPrefixesIterator;
    private Iterator<R> recordsIterator;
    private R next = null;

    private final HFileScanner hFileScanner;
    private final Schema readerSchema;

    RecordByKeyPrefixIterator(List<String> keyPrefixes, Schema readerSchema) throws IOException {
      this.keyPrefixesIterator = keyPrefixes.iterator();
      this.readerSchema = readerSchema;
      this.hFileScanner = reader.getScanner(false, false);
      this.hFileScanner.seekTo(); // position at the beginning of the file
    }

    @Override
    public boolean hasNext() {
      try {
        while (true) {
          // NOTE: This is required for idempotency
          if (next != null) {
            return true;
          } else if (recordsIterator != null && recordsIterator.hasNext()) {
            next = recordsIterator.next();
            return true;
          } else if (keyPrefixesIterator.hasNext()) {
            recordsIterator = getRecordsByKeyPrefixesInternal(Collections.singletonList(keyPrefixesIterator.next()), hFileScanner, readerSchema)
                .values().iterator();
          } else {
            return false;
          }
        }
      } catch (IOException e) {
        throw new HoodieIOException("Unable to read next record from HFile", e);
      }
    }

    @Override
    public R next() {
      R next = this.next;
      this.next = null;
      return next;
    }

    @Override
    public void close() {
      hFileScanner.close();
    }
  }

  public class RecordByKeyIterator implements ClosableIterator<R> {
    private final Iterator<String> keyIterator;
    private final HFileScanner hFileScanner;
    private final Schema schema;

    private R next = null;

    RecordByKeyIterator(List<String> keys, Schema schema) throws IOException {
      this.keyIterator = keys.iterator();
      this.schema = schema;
      this.hFileScanner = reader.getScanner(false, false);
      this.hFileScanner.seekTo(); // position at the beginning of the file
    }

    @Override
    public void close() {
      hFileScanner.close();
    }

    @Override
    public boolean hasNext() {
      try {
        // NOTE: This is required for idempotency
        if (next != null) {
          return true;
        }

        while (keyIterator.hasNext()) {
          Option<R> value = getRecordByKeyInternal(keyIterator.next(), schema, hFileScanner);
          if (value.isPresent()) {
            next = value.get();
            return true;
          }
        }
        return false;
      } catch (IOException e) {
        throw new HoodieIOException("unable to read next record from hfile ", e);
      }
    }

    @Override
    public R next() {
      R next = this.next;
      this.next = null;
      return next;
    }
  }

  class RecordIterator implements ClosableIterator<R> {
    private final HFileScanner hFileScanner;
    private final Schema readerSchema;

    private R next = null;

    RecordIterator(Schema readerSchema) {
      this.hFileScanner = reader.getScanner(false, false);
      this.readerSchema = readerSchema;
    }

    @Override
    public boolean hasNext() {
      try {
        // NOTE: This is required for idempotency
        if (next != null) {
          return true;
        }

        boolean hasRecords;
        if (!hFileScanner.isSeeked()) {
          hasRecords = hFileScanner.seekTo();
        } else {
          hasRecords = hFileScanner.next();
        }

        if (!hasRecords) {
          return false;
        }

        Pair<String, R> keyRecordPair = getRecordFromCell(hFileScanner.getCell(), getSchema(), readerSchema);
        this.next = keyRecordPair.getSecond();
        return true;
      } catch (IOException io) {
        throw new HoodieIOException("unable to read next record from hfile ", io);
      }
    }

    @Override
    public R next() {
      R next = this.next;
      this.next = null;
      return next;
    }

    @Override
    public void close() {
      hFileScanner.close();
    }
  }

  static class SeekableByteArrayInputStream extends ByteBufferBackedInputStream implements Seekable, PositionedReadable {
    public SeekableByteArrayInputStream(byte[] buf) {
      super(buf);
    }

    @Override
    public long getPos() throws IOException {
      return getPosition();
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      return false;
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length) throws IOException {
      return copyFrom(position, buffer, offset, length);
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
      read(position, buffer, 0, buffer.length);
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
      read(position, buffer, offset, length);
    }
  }
}
