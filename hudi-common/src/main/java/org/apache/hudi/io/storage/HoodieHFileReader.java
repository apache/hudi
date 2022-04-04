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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.ClosableIterator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.io.ByteBufferBackedInputStream;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;

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
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class HoodieHFileReader<R extends IndexedRecord> implements HoodieFileReader<R> {
  public static final String KEY_FIELD_NAME = "key";
  public static final String KEY_SCHEMA = "schema";
  public static final String KEY_BLOOM_FILTER_META_BLOCK = "bloomFilter";
  public static final String KEY_BLOOM_FILTER_TYPE_CODE = "bloomFilterTypeCode";
  public static final String KEY_MIN_RECORD = "minRecordKey";
  public static final String KEY_MAX_RECORD = "maxRecordKey";

  private static final Logger LOG = LogManager.getLogger(HoodieHFileReader.class);

  private final Path path;
  private final Configuration conf;
  private final HFile.Reader reader;
  private final FSDataInputStream fsDataInputStream;
  // TODO make final
  private Schema schema;

  public HoodieHFileReader(Configuration configuration, Path path, CacheConfig cacheConfig) throws IOException {
    this.conf = configuration;
    this.path = path;
    this.reader = HoodieHFileUtils.createHFileReader(FSUtils.getFs(path.toString(), configuration), path, cacheConfig, conf);
    this.fsDataInputStream = null;
  }

  public HoodieHFileReader(Configuration configuration, Path path, CacheConfig cacheConfig, FileSystem fs) throws IOException {
    this.conf = configuration;
    this.path = path;
    this.fsDataInputStream = fs.open(path);
    this.reader = HoodieHFileUtils.createHFileReader(fs, path, cacheConfig, configuration);
  }

  public HoodieHFileReader(FileSystem fs, Path dummyPath, byte[] content) throws IOException {
    this.reader = HoodieHFileUtils.createHFileReader(fs, dummyPath, content);
    this.path = null;
    this.conf = null;
    this.fsDataInputStream = null;
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
      schema = new Schema.Parser().parse(new String(fileInfo.get(KEY_SCHEMA.getBytes())));
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
    HFileScanner hFileScanner = reader.getScanner(false, false);
    try {
      hFileScanner.seekTo();
    } catch (IOException e) {
      throw new HoodieIOException("Cannot seek to beginning of HFile " + reader.getHFileInfo().toString(), e);
    }
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
  public Map<String, R> getRecordsByKeys(List<String> rowKeys) throws IOException {
    return filterRecordsImpl(new TreeSet<>(rowKeys));
  }

  /**
   * Filter records by sorted keys.
   * <p>
   * TODO: Implement single seek and sequential scan till the last candidate key
   * instead of repeated seeks.
   *
   * @param sortedCandidateRowKeys - Sorted set of keys to fetch records for
   * @return Map of keys to fetched records
   * @throws IOException When the deserialization of records fail
   */
  private synchronized Map<String, R> filterRecordsImpl(TreeSet<String> sortedCandidateRowKeys) throws IOException {
    HFileScanner hFileScanner = reader.getScanner(false, false);
    hFileScanner.seekTo();
    Schema readerSchema = getSchema();
    Option<Schema.Field> keyFieldSchema = Option.ofNullable(readerSchema.getField(KEY_FIELD_NAME));
    HashMap<String, R> filteredRecords = new HashMap<>();
    for (String key : sortedCandidateRowKeys) {
      Option<R> record = getRecordByKey(key, readerSchema, hFileScanner, keyFieldSchema);
      if (record.isPresent()) {
        filteredRecords.put(key, record.get());
      }
    }
    return filteredRecords;
  }

  /**
   * Reads all the records with given schema.
   *
   * <p>NOTE: This should only be used for testing,
   * the records are materialized eagerly into a list and returned,
   * use {@code getRecordIterator} where possible.
   */
  private List<Pair<String, R>> readAllRecords(Schema writerSchema, Schema readerSchema) {
    final Option<Schema.Field> keyFieldSchema = Option.ofNullable(readerSchema.getField(KEY_FIELD_NAME));
    List<Pair<String, R>> recordList = new LinkedList<>();
    try {
      final HFileScanner scanner = reader.getScanner(false, false);
      if (scanner.seekTo()) {
        do {
          Cell c = scanner.getCell();
          final Pair<String, R> keyAndRecordPair = getRecordFromCell(c, writerSchema, readerSchema, keyFieldSchema);
          recordList.add(keyAndRecordPair);
        } while (scanner.next());
      }

      return recordList;
    } catch (IOException e) {
      throw new HoodieException("Error reading hfile " + path + " as a dataframe", e);
    }
  }

  /**
   * Reads all the records with current schema.
   *
   * <p>NOTE: This should only be used for testing,
   * the records are materialized eagerly into a list and returned,
   * use {@code getRecordIterator} where possible.
   */
  public List<Pair<String, R>> readAllRecords() {
    Schema schema = getSchema();
    return readAllRecords(schema, schema);
  }

  /**
   * Reads all the records with current schema and filtering keys.
   *
   * <p>NOTE: This should only be used for testing,
   * the records are materialized eagerly into a list and returned,
   * use {@code getRecordIterator} where possible.
   */
  public List<Pair<String, R>> readRecords(List<String> keys) throws IOException {
    return readRecords(keys, getSchema());
  }

  /**
   * Reads all the records with given schema and filtering keys.
   *
   * <p>NOTE: This should only be used for testing,
   * the records are materialized eagerly into a list and returned,
   * use {@code getRecordIterator} where possible.
   */
  public List<Pair<String, R>> readRecords(List<String> keys, Schema schema) throws IOException {
    this.schema = schema;
    HFileScanner hFileScanner = reader.getScanner(false, false);
    hFileScanner.seekTo();
    Option<Schema.Field> keyFieldSchema = Option.ofNullable(schema.getField(KEY_FIELD_NAME));
    List<Pair<String, R>> records = new ArrayList<>();
    for (String key : keys) {
      Option<R> value = getRecordByKey(key, schema, hFileScanner, keyFieldSchema);
      if (value.isPresent()) {
        records.add(new Pair(key, value.get()));
      }
    }
    return records;
  }

  public ClosableIterator<R> getRecordIteratorByKeyPrefix(List<String> keyPrefixes, Schema schema) throws IOException {
    this.schema = schema;
    return new InternalClosableKeyPrefixIterator(keyPrefixes);
  }

  class InternalClosableKeyPrefixIterator implements ClosableIterator<R> {

    private Iterator<String> keyPrefixesIterator;
    private Iterator<R> recordsIterator;
    private R next;
    private final HFileScanner hFileScanner;
    private final Schema readerSchema;
    private final Option<Schema.Field> keyFieldSchema;

    InternalClosableKeyPrefixIterator(List<String> keyPrefixes) throws IOException {
      keyPrefixesIterator = keyPrefixes.iterator();
      // Instantiate a KeyScanner locally and use it for the lifecycle of this interator and not re-use the class instance variable.
      hFileScanner = reader.getScanner(false, false);
      hFileScanner.seekTo();
      readerSchema = getSchema();
      keyFieldSchema = Option.ofNullable(readerSchema.getField(KEY_FIELD_NAME));
    }

    @Override
    public boolean hasNext() {
      try {
        while (true) {
          if (recordsIterator != null && recordsIterator.hasNext()) {
            next = recordsIterator.next();
            return true;
          } else if (keyPrefixesIterator.hasNext()) {
            recordsIterator = getRecordsByKeyPrefixes(Collections.singletonList(keyPrefixesIterator.next()), hFileScanner, readerSchema, keyFieldSchema)
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
      return next;
    }

    @Override
    public void close() {
      hFileScanner.close();
    }
  }

  public ClosableIterator<R> getRecordIterator(List<String> keys, Schema schema) throws IOException {
    return new InternalClosableIterator(keys, schema);
  }

  public class InternalClosableIterator implements ClosableIterator<R> {
    private Schema schema;
    private Iterator<String> iterator;
    private HFileScanner hFileScanner;
    private R next;
    private Option<Schema.Field> keyFieldSchema;

    InternalClosableIterator(List<String> keys, Schema schema) throws IOException {
      this.iterator = keys.iterator();
      this.schema = schema;
      this.hFileScanner = reader.getScanner(false, false);
      hFileScanner.seekTo();
      keyFieldSchema = Option.ofNullable(schema.getField(KEY_FIELD_NAME));
    }

    @Override
    public void close() {
      hFileScanner.close();
    }

    @Override
    public boolean hasNext() {
      try {
        while (iterator.hasNext()) {
          Option<R> value = getRecordByKey(iterator.next(), schema, hFileScanner, keyFieldSchema);
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
      return next;
    }
  }

  @Override
  public Iterator getRecordIterator(Schema readerSchema) throws IOException {
    final HFileScanner hFileScanner = reader.getScanner(false, false);
    final Option<Schema.Field> keyFieldSchema = Option.ofNullable(readerSchema.getField(KEY_FIELD_NAME));
    return new ClosableIterator() {
      private R next = null;
      private boolean eof = false;

      @Override
      public boolean hasNext() {
        try {
          // To handle when hasNext() is called multiple times for idempotency and/or the first time
          if (this.next == null && !this.eof) {
            if (!hFileScanner.isSeeked() && hFileScanner.seekTo()) {
              final Pair<String, R> keyAndRecordPair = getRecordFromCell(hFileScanner.getCell(), getSchema(), readerSchema, keyFieldSchema);
              this.next = keyAndRecordPair.getSecond();
            }
          }
          return this.next != null;
        } catch (IOException io) {
          throw new HoodieIOException("unable to read next record from hfile ", io);
        }
      }

      @Override
      public R next() {
        try {
          // To handle case when next() is called before hasNext()
          if (this.next == null) {
            if (!hasNext()) {
              throw new HoodieIOException("No more records left to read from hfile");
            }
          }
          R retVal = this.next;
          if (hFileScanner.next()) {
            final Pair<String, R> keyAndRecordPair = getRecordFromCell(hFileScanner.getCell(), getSchema(), readerSchema, keyFieldSchema);
            this.next = keyAndRecordPair.getSecond();
          } else {
            this.next = null;
            this.eof = true;
          }
          return retVal;
        } catch (IOException io) {
          throw new HoodieIOException("unable to read next record from parquet file ", io);
        }
      }

      @Override
      public void close() {
        hFileScanner.close();
      }
    };
  }

  private boolean isKeyAvailable(String key, HFileScanner keyScanner) throws IOException {
    final KeyValue kv = new KeyValue(key.getBytes(), null, null, null);
    return keyScanner.seekTo(kv) == 0;
  }

  @Override
  public Map<String, R> getRecordsByKeyPrefixes(List<String> keyPrefixes) throws IOException {
    Schema readerSchema = getSchema();
    Option<Schema.Field> keyFieldSchema = Option.ofNullable(readerSchema.getField(KEY_FIELD_NAME));
    synchronized (this) {
      HFileScanner hFileScanner = reader.getScanner(false, false);
      hFileScanner.seekTo(); // seek to beginning of file.
      return getRecordsByKeyPrefixes(keyPrefixes, hFileScanner, readerSchema, keyFieldSchema);
    }
  }

  @Override
  public Map<String, R> getRecordsByKeyPrefixes(List<String> keyPrefixes, HFileScanner hFileScanner, Schema readerSchema, Option<Schema.Field> keyFieldSchema) throws IOException {
    // NOTE: It's always beneficial to sort keys being sought to by HFile reader
    //       to avoid seeking back and forth
    Collections.sort(keyPrefixes);
    List<Pair<byte[], byte[]>> keyRecordsBytes = new ArrayList<>(keyPrefixes.size());
    for (String keyPrefix : keyPrefixes) {
      KeyValue kv = new KeyValue(keyPrefix.getBytes(), null, null, null);
      int val = hFileScanner.seekTo(kv);
      // what does seekTo() does:
      // eg entries in file. [key01, key02, key03, key04,..., key20]
      // when keyPrefix is "key", seekTo will return -1 and place the cursor just before key01. getCel() will return key01 entry
      // when keyPrefix is ""key03", seekTo will return 0 and place the cursor just before key01. getCell() will return key03 entry
      // when keyPrefix is ""key1", seekTo will return 1 and place the cursor just before key10(i.e. key09). call next() and then call getCell() to see key10 entry
      // when keyPrefix is "key99", seekTo will return 1 and place the cursor just before last entry, ie. key04. getCell() will return key04 entry.

      if (val == 1) { // move to next entry if return value is 1
        if (!hFileScanner.next()) {
          // we have reached the end of file. we can skip proceeding further
          break;
        }
      }
      do {
        Cell c = hFileScanner.getCell();
        byte[] keyBytes = Arrays.copyOfRange(c.getRowArray(), c.getRowOffset(), c.getRowOffset() + c.getRowLength());
        String key = new String(keyBytes);
        // Check whether we're still reading records corresponding to the key-prefix
        if (!key.startsWith(keyPrefix)) {
          break;
        }

        // Extract the byte value before releasing the lock since we cannot hold on to the returned cell afterwards
        byte[] valueBytes = Arrays.copyOfRange(c.getValueArray(), c.getValueOffset(), c.getValueOffset() + c.getValueLength());
        keyRecordsBytes.add(Pair.newPair(keyBytes, valueBytes));
      } while (hFileScanner.next());
    }

    // Use  tree map so that entries are in sorted in the map being returned.
    Map<String, R> values = new TreeMap<String, R>();
    for (Pair<byte[], byte[]> kv : keyRecordsBytes) {
      R record = deserialize(kv.getFirst(), kv.getSecond(), readerSchema, readerSchema, keyFieldSchema);
      values.put(new String(kv.getFirst()), record);
    }
    return values;
  }

  @Override
  public Option<R> getRecordByKey(String key, Schema readerSchema, HFileScanner hFileScanner, Option<Schema.Field> keyFieldSchema) throws IOException {
    byte[] value = null;
    KeyValue kv = new KeyValue(key.getBytes(), null, null, null);

    if (hFileScanner.seekTo(kv) == 0) {
      Cell c = hFileScanner.getCell();
      // Extract the byte value before releasing the lock since we cannot hold on to the returned cell afterwards
      value = Arrays.copyOfRange(c.getValueArray(), c.getValueOffset(), c.getValueOffset() + c.getValueLength());
    }

    if (value != null) {
      R record = deserialize(key.getBytes(), value, getSchema(), readerSchema, keyFieldSchema);
      return Option.of(record);
    }

    return Option.empty();
  }

  private Pair<String, R> getRecordFromCell(Cell cell, Schema writerSchema, Schema readerSchema, Option<Schema.Field> keyFieldSchema) throws IOException {
    final byte[] keyBytes = Arrays.copyOfRange(cell.getRowArray(), cell.getRowOffset(), cell.getRowOffset() + cell.getRowLength());
    final byte[] valueBytes = Arrays.copyOfRange(cell.getValueArray(), cell.getValueOffset(), cell.getValueOffset() + cell.getValueLength());
    R record = deserialize(keyBytes, valueBytes, writerSchema, readerSchema, keyFieldSchema);
    return new Pair<>(new String(keyBytes), record);
  }

  /**
   * Deserialize the record byte array contents to record object.
   *
   * @param keyBytes       - Record key as byte array
   * @param valueBytes     - Record content as byte array
   * @param writerSchema   - Writer schema
   * @param readerSchema   - Reader schema
   * @param keyFieldSchema - Key field id in the schema
   * @return Deserialized record object
   */
  private R deserialize(final byte[] keyBytes, final byte[] valueBytes, Schema writerSchema, Schema readerSchema,
                        Option<Schema.Field> keyFieldSchema) throws IOException {
    R record = (R) HoodieAvroUtils.bytesToAvro(valueBytes, writerSchema, readerSchema);
    materializeRecordIfNeeded(keyBytes, record, keyFieldSchema);
    return record;
  }

  /**
   * Materialize the record for any missing fields, if needed.
   *
   * @param keyBytes       - Key byte array
   * @param record         - Record object to materialize
   * @param keyFieldSchema - Key field id in the schema
   */
  private void materializeRecordIfNeeded(final byte[] keyBytes, R record, Option<Schema.Field> keyFieldSchema) {
    if (keyFieldSchema.isPresent()) {
      final Object keyObject = record.get(keyFieldSchema.get().pos());
      if (keyObject != null && keyObject.toString().isEmpty()) {
        record.put(keyFieldSchema.get().pos(), new String(keyBytes));
      }
    }
  }

  @Override
  public long getTotalRecords() {
    return reader.getEntries();
  }

  @Override
  public synchronized void close() {
    try {
      reader.close();
      if (fsDataInputStream != null) {
        fsDataInputStream.close();
      }
    } catch (IOException e) {
      throw new HoodieIOException("Error closing the hfile reader", e);
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
