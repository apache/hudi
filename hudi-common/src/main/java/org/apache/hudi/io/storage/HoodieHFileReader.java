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
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.ClosableIterator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.io.ByteBufferBackedInputStream;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.util.Lazy;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.CollectionUtils.toStream;
import static org.apache.hudi.common.util.ValidationUtils.checkState;

/**
 * NOTE: PLEASE READ DOCS & COMMENTS CAREFULLY BEFORE MAKING CHANGES
 * <p>
 * {@link HoodieFileReader} implementation allowing to read from {@link HFile}.
 */
public class HoodieHFileReader<R extends IndexedRecord> implements HoodieFileReader<R> {

  // TODO HoodieHFileReader right now tightly coupled to MT, we should break that coupling
  public static final String SCHEMA_KEY = "schema";
  public static final String KEY_BLOOM_FILTER_META_BLOCK = "bloomFilter";
  public static final String KEY_BLOOM_FILTER_TYPE_CODE = "bloomFilterTypeCode";

  public static final String KEY_FIELD_NAME = "key";
  public static final String KEY_MIN_RECORD = "minRecordKey";
  public static final String KEY_MAX_RECORD = "maxRecordKey";

  private static final Logger LOG = LogManager.getLogger(HoodieHFileReader.class);

  private final Path path;

  private final Lazy<Schema> schema;

  // NOTE: Reader is ONLY THREAD-SAFE for {@code Scanner} operating in Positional Read ("pread")
  //       mode (ie created w/ "pread = true")
  private final HFile.Reader reader;
  // NOTE: Scanner caches read blocks, therefore it's important to re-use scanner
  //       wherever possible
  private final HFileScanner sharedScanner;

  private final Object sharedScannerLock = new Object();

  public HoodieHFileReader(Configuration hadoopConf, Path path, CacheConfig cacheConfig) throws IOException {
    this(path,
        HoodieHFileUtils.createHFileReader(FSUtils.getFs(path.toString(), hadoopConf), path, cacheConfig, hadoopConf),
        Option.empty());
  }

  public HoodieHFileReader(Configuration hadoopConf, Path path, CacheConfig cacheConfig, FileSystem fs) throws IOException {
    this(path, HoodieHFileUtils.createHFileReader(fs, path, cacheConfig, hadoopConf), Option.empty());
  }

  public HoodieHFileReader(FileSystem fs, Path dummyPath, byte[] content, Option<Schema> schemaOpt) throws IOException {
    this(null, HoodieHFileUtils.createHFileReader(fs, dummyPath, content), schemaOpt);
  }

  public HoodieHFileReader(Path path, HFile.Reader reader, Option<Schema> schemaOpt) throws IOException {
    this.path = path;
    this.reader = reader;
    // For shared scanner, which is primarily used for point-lookups, we're caching blocks
    // by default, to minimize amount of traffic to the underlying storage
    this.sharedScanner = getHFileScanner(reader, true);
    this.schema = schemaOpt.map(Lazy::eagerly)
        .orElseGet(() -> Lazy.lazily(() -> fetchSchema(reader)));
  }

  @Override
  public String[] readMinMaxRecordKeys() {
    // NOTE: This access to reader is thread-safe
    HFileInfo fileInfo = reader.getHFileInfo();
    return new String[]{new String(fileInfo.get(KEY_MIN_RECORD.getBytes())),
        new String(fileInfo.get(KEY_MAX_RECORD.getBytes()))};
  }

  @Override
  public BloomFilter readBloomFilter() {
    try {
      // NOTE: This access to reader is thread-safe
      HFileInfo fileInfo = reader.getHFileInfo();
      ByteBuff buf = reader.getMetaBlock(KEY_BLOOM_FILTER_META_BLOCK, false).getBufferWithoutHeader();
      // We have to copy bytes here, since we can't reuse buffer's underlying
      // array as is, since it contains additional metadata (header)
      byte[] bytes = new byte[buf.remaining()];
      buf.get(bytes);
      return BloomFilterFactory.fromString(new String(bytes),
          new String(fileInfo.get(KEY_BLOOM_FILTER_TYPE_CODE.getBytes())));
    } catch (IOException e) {
      throw new HoodieException("Could not read bloom filter from " + path, e);
    }
  }

  @Override
  public Schema getSchema() {
    return schema.get();
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
    checkState(candidateRowKeys instanceof TreeSet,
        String.format("HFile reader expects a TreeSet as iterating over ordered keys is more performant, got (%s)", candidateRowKeys.getClass().getSimpleName()));

    synchronized (sharedScannerLock) {
      return candidateRowKeys.stream().filter(k -> {
        try {
          return isKeyAvailable(k, sharedScanner);
        } catch (IOException e) {
          LOG.error("Failed to check key availability: " + k);
          return false;
        }
      }).collect(Collectors.toSet());
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public Option<R> getRecordByKey(String key, Schema readerSchema) throws IOException {
    synchronized (sharedScannerLock) {
      return (Option<R>) fetchRecordByKeyInternal(sharedScanner, key, getSchema(), readerSchema);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public ClosableIterator<R> getRecordIterator(Schema readerSchema) throws IOException {
    // TODO eval whether seeking scanner would be faster than pread
    HFileScanner scanner = getHFileScanner(reader, false);
    return (ClosableIterator<R>) new RecordIterator(scanner, getSchema(), readerSchema);
  }

  @SuppressWarnings("unchecked")
  @Override
  public ClosableIterator<R> getRecordsByKeysIterator(List<String> keys, Schema readerSchema) throws IOException {
    // We're caching blocks for this scanner to minimize amount of traffic
    // to the underlying storage as we fetched (potentially) sparsely distributed
    // keys
    HFileScanner scanner = getHFileScanner(reader, true);
    return (ClosableIterator<R>) new RecordByKeyIterator(scanner, keys, getSchema(), readerSchema);
  }

  @SuppressWarnings("unchecked")
  @Override
  public ClosableIterator<R> getRecordsByKeyPrefixIterator(List<String> keyPrefixes, Schema readerSchema) throws IOException {
    // We're caching blocks for this scanner to minimize amount of traffic
    // to the underlying storage as we fetched (potentially) sparsely distributed
    // keys
    HFileScanner scanner = getHFileScanner(reader, true);
    return (ClosableIterator<R>) new RecordByKeyPrefixIterator(scanner, keyPrefixes, getSchema(), readerSchema);
  }

  @Override
  public long getTotalRecords() {
    // NOTE: This access to reader is thread-safe
    return reader.getEntries();
  }

  @Override
  public void close() {
    try {
      synchronized (this) {
        reader.close();
      }
    } catch (IOException e) {
      throw new HoodieIOException("Error closing the hfile reader", e);
    }
  }

  private boolean isKeyAvailable(String key, HFileScanner keyScanner) throws IOException {
    final KeyValue kv = new KeyValue(key.getBytes(), null, null, null);
    return keyScanner.seekTo(kv) == 0;
  }

  private static Iterator<GenericRecord> getRecordByKeyPrefixIteratorInternal(HFileScanner scanner,
                                                                              String keyPrefix,
                                                                              Schema writerSchema,
                                                                              Schema readerSchema) throws IOException {
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
    int val = scanner.seekTo(kv);
    if (val == 1) {
      // Try moving to next entry, matching the prefix key; if we're at the EOF,
      // `next()` will return false
      if (!scanner.next()) {
        return Collections.emptyIterator();
      }
    }

    class KeyPrefixIterator implements Iterator<GenericRecord> {
      private GenericRecord next = null;
      private boolean eof = false;

      @Override
      public boolean hasNext() {
        if (next != null) {
          return true;
        } else if (eof) {
          return false;
        }

        Cell c = Objects.requireNonNull(scanner.getCell());
        byte[] keyBytes = copyKeyFromCell(c);
        String key = new String(keyBytes);
        // Check whether we're still reading records corresponding to the key-prefix
        if (!key.startsWith(keyPrefix)) {
          return false;
        }

        // Extract the byte value before releasing the lock since we cannot hold on to the returned cell afterwards
        byte[] valueBytes = copyValueFromCell(c);
        try {
          next = deserialize(keyBytes, valueBytes, writerSchema, readerSchema);
          // In case scanner is not able to advance, it means we reached EOF
          eof = !scanner.next();
        } catch (IOException e) {
          throw new HoodieIOException("Failed to deserialize payload", e);
        }

        return true;
      }

      @Override
      public GenericRecord next() {
        GenericRecord next = this.next;
        this.next = null;
        return next;
      }
    }

    return new KeyPrefixIterator();
  }

  private static Option<GenericRecord> fetchRecordByKeyInternal(HFileScanner scanner, String key, Schema writerSchema, Schema readerSchema) throws IOException {
    KeyValue kv = new KeyValue(key.getBytes(), null, null, null);
    if (scanner.seekTo(kv) != 0) {
      return Option.empty();
    }

    Cell c = scanner.getCell();
    byte[] valueBytes = copyValueFromCell(c);
    GenericRecord record = deserialize(key.getBytes(), valueBytes, writerSchema, readerSchema);

    return Option.of(record);
  }

  private static GenericRecord getRecordFromCell(Cell cell, Schema writerSchema, Schema readerSchema) throws IOException {
    final byte[] keyBytes = copyKeyFromCell(cell);
    final byte[] valueBytes = copyValueFromCell(cell);
    return deserialize(keyBytes, valueBytes, writerSchema, readerSchema);
  }

  private static GenericRecord deserializeUnchecked(final byte[] keyBytes,
                                                    final byte[] valueBytes,
                                                    Schema writerSchema,
                                                    Schema readerSchema) {
    try {
      return deserialize(keyBytes, valueBytes, writerSchema, readerSchema);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to deserialize payload", e);
    }
  }

  private static GenericRecord deserialize(final byte[] keyBytes,
                                           final byte[] valueBytes,
                                           Schema writerSchema,
                                           Schema readerSchema) throws IOException {
    GenericRecord record = HoodieAvroUtils.bytesToAvro(valueBytes, writerSchema, readerSchema);

    getKeySchema(readerSchema).ifPresent(keyFieldSchema -> {
      final Object keyObject = record.get(keyFieldSchema.pos());
      if (keyObject != null && keyObject.toString().isEmpty()) {
        record.put(keyFieldSchema.pos(), new String(keyBytes));
      }
    });

    return record;
  }

  private static Schema fetchSchema(HFile.Reader reader) {
    HFileInfo fileInfo = reader.getHFileInfo();
    return new Schema.Parser().parse(new String(fileInfo.get(SCHEMA_KEY.getBytes())));
  }

  private static byte[] copyKeyFromCell(Cell cell) {
    return Arrays.copyOfRange(cell.getRowArray(), cell.getRowOffset(), cell.getRowOffset() + cell.getRowLength());
  }

  private static byte[] copyValueFromCell(Cell c) {
    return Arrays.copyOfRange(c.getValueArray(), c.getValueOffset(), c.getValueOffset() + c.getValueLength());
  }

  /**
   * NOTE: THIS SHOULD ONLY BE USED FOR TESTING, RECORDS ARE MATERIALIZED EAGERLY
   * <p>
   * Reads all the records with given schema
   */
  public static <R extends IndexedRecord> List<R> readAllRecords(HoodieHFileReader<R> reader) throws IOException {
    Schema schema = reader.getSchema();
    return toStream(reader.getRecordIterator(schema))
        .collect(Collectors.toList());
  }

  /**
   * NOTE: THIS SHOULD ONLY BE USED FOR TESTING, RECORDS ARE MATERIALIZED EAGERLY
   * <p>
   * Reads all the records with given schema and filtering keys.
   */
  public static <R extends IndexedRecord> List<R> readRecords(HoodieHFileReader<R> reader,
                                                              List<String> keys) throws IOException {
    return readRecords(reader, keys, reader.getSchema());
  }

  /**
   * NOTE: THIS SHOULD ONLY BE USED FOR TESTING, RECORDS ARE MATERIALIZED EAGERLY
   * <p>
   * Reads all the records with given schema and filtering keys.
   */
  public static <R extends IndexedRecord> List<R> readRecords(HoodieHFileReader<R> reader,
                                                              List<String> keys,
                                                              Schema schema) throws IOException {
    Collections.sort(keys);
    return toStream(reader.getRecordsByKeysIterator(keys, schema))
        .collect(Collectors.toList());
  }

  private static HFileScanner getHFileScanner(HFile.Reader reader, boolean cacheBlocks) {
    // NOTE: Only scanners created in Positional Read ("pread") mode could share the same reader,
    //       since scanners in default mode will be seeking w/in the underlying stream
    return reader.getScanner(cacheBlocks, true);
  }

  private static Option<Schema.Field> getKeySchema(Schema schema) {
    return Option.ofNullable(schema.getField(KEY_FIELD_NAME));
  }

  private static class RecordByKeyPrefixIterator implements ClosableIterator<GenericRecord> {
    private final Iterator<String> keyPrefixesIterator;
    private Iterator<GenericRecord> recordsIterator;

    private final HFileScanner scanner;

    private final Schema writerSchema;
    private final Schema readerSchema;

    private GenericRecord next = null;

    RecordByKeyPrefixIterator(HFileScanner scanner, List<String> keyPrefixes, Schema writerSchema, Schema readerSchema) throws IOException {
      this.keyPrefixesIterator = keyPrefixes.iterator();

      this.scanner = scanner;
      this.scanner.seekTo(); // position at the beginning of the file

      this.writerSchema = writerSchema;
      this.readerSchema = readerSchema;
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
            String currentKeyPrefix = keyPrefixesIterator.next();
            recordsIterator =
                getRecordByKeyPrefixIteratorInternal(scanner, currentKeyPrefix, writerSchema, readerSchema);
          } else {
            return false;
          }
        }
      } catch (IOException e) {
        throw new HoodieIOException("Unable to read next record from HFile", e);
      }
    }

    @Override
    public GenericRecord next() {
      GenericRecord next = this.next;
      this.next = null;
      return next;
    }

    @Override
    public void close() {
      scanner.close();
    }
  }

  private static class RecordByKeyIterator implements ClosableIterator<GenericRecord> {
    private final Iterator<String> keyIterator;

    private final HFileScanner scanner;

    private final Schema readerSchema;
    private final Schema writerSchema;

    private GenericRecord next = null;

    RecordByKeyIterator(HFileScanner scanner, List<String> keys, Schema writerSchema, Schema readerSchema) throws IOException {
      this.keyIterator = keys.iterator();

      this.scanner = scanner;
      this.scanner.seekTo(); // position at the beginning of the file

      this.writerSchema = writerSchema;
      this.readerSchema = readerSchema;
    }

    @Override
    public boolean hasNext() {
      try {
        // NOTE: This is required for idempotency
        if (next != null) {
          return true;
        }

        while (keyIterator.hasNext()) {
          Option<GenericRecord> value = fetchRecordByKeyInternal(scanner, keyIterator.next(), writerSchema, readerSchema);
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
    public GenericRecord next() {
      GenericRecord next = this.next;
      this.next = null;
      return next;
    }

    @Override
    public void close() {
      scanner.close();
    }
  }

  private static class RecordIterator implements ClosableIterator<GenericRecord> {
    private final HFileScanner scanner;

    private final Schema writerSchema;
    private final Schema readerSchema;

    private GenericRecord next = null;

    RecordIterator(HFileScanner scanner, Schema writerSchema, Schema readerSchema) {
      this.scanner = scanner;
      this.writerSchema = writerSchema;
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
        if (!scanner.isSeeked()) {
          hasRecords = scanner.seekTo();
        } else {
          hasRecords = scanner.next();
        }

        if (!hasRecords) {
          return false;
        }

        this.next = getRecordFromCell(scanner.getCell(), writerSchema, readerSchema);
        return true;
      } catch (IOException io) {
        throw new HoodieIOException("unable to read next record from hfile ", io);
      }
    }

    @Override
    public GenericRecord next() {
      GenericRecord next = this.next;
      this.next = null;
      return next;
    }

    @Override
    public void close() {
      scanner.close();
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
