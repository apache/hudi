/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.io.storage;

import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.io.ByteBufferBackedInputStream;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.expression.Expression;
import org.apache.hudi.expression.Predicate;
import org.apache.hudi.expression.Predicates;
import org.apache.hudi.io.ByteArraySeekableDataInputStream;
import org.apache.hudi.io.SeekableDataInputStream;
import org.apache.hudi.io.hfile.HFileReader;
import org.apache.hudi.io.hfile.HFileReaderImpl;
import org.apache.hudi.io.hfile.KeyValue;
import org.apache.hudi.io.hfile.UTF8StringKey;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.util.Lazy;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.StringUtils.fromUTF8Bytes;
import static org.apache.hudi.common.util.TypeUtils.unsafeCast;
import static org.apache.hudi.io.hfile.HFileUtils.isPrefixOfKey;

/**
 * An implementation of {@link HoodieAvroHFileReaderImplBase} using native {@link HFileReader}.
 */
public class HoodieNativeAvroHFileReader extends HoodieAvroHFileReaderImplBase {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieNativeAvroHFileReader.class);
  // Keys of the meta info that should be preloaded on demand from the HFile
  private static final Set<String> PRELOADED_META_INFO_KEYS = new HashSet<>(
      Arrays.asList(KEY_MIN_RECORD, KEY_MAX_RECORD, SCHEMA_KEY));

  private final HoodieStorage storage;
  private final Option<StoragePath> path;
  private final Option<byte[]> bytesContent;
  // In-memory cache for meta info
  private final Map<String, byte[]> metaInfoMap;
  private final Lazy<Schema> schema;
  private boolean isMetaInfoLoaded = false;
  private long numKeyValueEntries = -1L;

  public HoodieNativeAvroHFileReader(HoodieStorage storage, StoragePath path, Option<Schema> schemaOption) {
    this.storage = storage;
    this.path = Option.of(path);
    this.bytesContent = Option.empty();
    this.metaInfoMap = new HashMap<>();
    this.schema = schemaOption.map(Lazy::eagerly).orElseGet(() -> Lazy.lazily(this::fetchSchema));
  }

  public HoodieNativeAvroHFileReader(HoodieStorage storage, byte[] content, Option<Schema> schemaOption) {
    this.storage = storage;
    this.path = Option.empty();
    this.bytesContent = Option.of(content);
    this.metaInfoMap = new HashMap<>();
    this.schema = schemaOption.map(Lazy::eagerly).orElseGet(() -> Lazy.lazily(this::fetchSchema));
  }

  @Override
  public ClosableIterator<IndexedRecord> getIndexedRecordIterator(Schema readerSchema,
                                                                  Schema requestedSchema)
      throws IOException {
    if (!Objects.equals(readerSchema, requestedSchema)) {
      throw new UnsupportedOperationException(
          "Schema projections are not supported in HFile reader");
    }

    HFileReader reader = newHFileReader();
    return new RecordIterator(reader, getSchema(), readerSchema);
  }

  @Override
  public String[] readMinMaxRecordKeys() {
    try {
      return new String[] {
          fromUTF8Bytes(getHFileMetaInfoFromCache(KEY_MIN_RECORD)),
          fromUTF8Bytes(getHFileMetaInfoFromCache(KEY_MAX_RECORD))};
    } catch (IOException e) {
      throw new HoodieIOException("Cannot read min and max record keys from HFile.", e);
    }
  }

  @Override
  public BloomFilter readBloomFilter() {
    try (HFileReader reader = newHFileReader()) {
      ByteBuffer byteBuffer = reader.getMetaBlock(KEY_BLOOM_FILTER_META_BLOCK).get();
      return BloomFilterFactory.fromByteBuffer(byteBuffer,
          fromUTF8Bytes(reader.getMetaInfo(new UTF8StringKey(KEY_BLOOM_FILTER_TYPE_CODE)).get()));
    } catch (IOException e) {
      throw new HoodieException("Could not read bloom filter from " + path, e);
    }
  }

  @Override
  public Set<Pair<String, Long>> filterRowKeys(Set<String> candidateRowKeys) {
    try (HFileReader reader = newHFileReader()) {
      reader.seekTo();
      // candidateRowKeys must be sorted
      return new TreeSet<>(candidateRowKeys).stream()
          .filter(k -> {
            try {
              return reader.seekTo(new UTF8StringKey(k)) == HFileReader.SEEK_TO_FOUND;
            } catch (IOException e) {
              LOG.error("Failed to check key availability: " + k);
              return false;
            }
          })
          // Record position is not supported for HFile
          .map(key -> Pair.of(key, HoodieRecordLocation.INVALID_POSITION))
          .collect(Collectors.toSet());
    } catch (IOException e) {
      throw new HoodieIOException("Unable to filter row keys in HFiles", e);
    }
  }

  @Override
  public ClosableIterator<String> getRecordKeyIterator() throws IOException {
    HFileReader reader = newHFileReader();
    return new ClosableIterator<String>() {
      @Override
      public boolean hasNext() {
        try {
          return reader.next();
        } catch (IOException e) {
          throw new HoodieException("Error while scanning for keys", e);
        }
      }

      @Override
      public String next() {
        try {
          return reader.getKeyValue().get().getKey().getContentInString();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public void close() {
        try {
          reader.close();
        } catch (IOException e) {
          throw new HoodieIOException("Error closing the HFile reader", e);
        }
      }
    };
  }

  @Override
  public Schema getSchema() {
    return schema.get();
  }

  @Override
  public void close() {
    isMetaInfoLoaded = false;
    metaInfoMap.clear();
  }

  @Override
  public long getTotalRecords() {
    try {
      loadAllMetaInfoIntoCacheIfNeeded();
    } catch (IOException e) {
      throw new HoodieIOException("Cannot get the number of entries from HFile", e);
    }
    ValidationUtils.checkArgument(
        numKeyValueEntries >= 0, "Number of entries in HFile must be >= 0");
    return numKeyValueEntries;
  }

  @Override
  public ClosableIterator<HoodieRecord<IndexedRecord>> getRecordsByKeysIterator(
      List<String> sortedKeys, Schema schema) throws IOException {
    HFileReader reader = newHFileReader();
    ClosableIterator<IndexedRecord> iterator =
        new RecordByKeyIterator(reader, sortedKeys, getSchema(), schema);
    return new CloseableMappingIterator<>(
        iterator, data -> unsafeCast(new HoodieAvroIndexedRecord(data)));
  }

  @Override
  public ClosableIterator<HoodieRecord<IndexedRecord>> getRecordsByKeyPrefixIterator(
      List<String> sortedKeyPrefixes, Schema schema) throws IOException {
    HFileReader reader = newHFileReader();
    ClosableIterator<IndexedRecord> iterator =
        new RecordByKeyPrefixIterator(reader, sortedKeyPrefixes, getSchema(), schema);
    return new CloseableMappingIterator<>(
        iterator, data -> unsafeCast(new HoodieAvroIndexedRecord(data)));
  }

  @Override
  public boolean supportKeyPredicate() {
    return true;
  }

  @Override
  public boolean supportKeyPrefixPredicate() {
    return true;
  }

  @Override
  public List<String> extractKeys(Option<Predicate> keyFilterOpt) {
    List<String> keys = new ArrayList<>();
    if (keyFilterOpt.isPresent()
        && keyFilterOpt.get().getOperator().equals(Expression.Operator.IN)) {
      List<Expression> children = ((Predicates.In) keyFilterOpt.get()).getRightChildren();
      keys = children.stream().map(e -> (String) e.eval(null)).collect(Collectors.toList());
    }

    if (keys.isEmpty() && keyFilterOpt.isPresent()) {
      LOG.warn("Cannot extract valid keys from predicate: {}", keyFilterOpt.get());
    }
    return keys;
  }

  @Override
  public List<String> extractKeyPrefixes(Option<Predicate> keyFilterOpt) {
    List<String> keyPrefixes = new ArrayList<>();
    if (keyFilterOpt.isPresent()
        && keyFilterOpt.get().getOperator().equals(Expression.Operator.STARTS_WITH)) {
      List<Expression> children = ((Predicates.StringStartsWithAny) keyFilterOpt.get()).getRightChildren();
      keyPrefixes = children.stream()
          .map(e -> (String) e.eval(null)).collect(Collectors.toList());
    }
    if (keyPrefixes.isEmpty() && keyFilterOpt.isPresent()) {
      LOG.warn("Cannot extract valid key prefixes from predicate: {}", keyFilterOpt.get());
    }
    return keyPrefixes;
  }

  private Schema fetchSchema() {
    try {
      return new Schema.Parser().parse(
          fromUTF8Bytes(getHFileMetaInfoFromCache(SCHEMA_KEY)));
    } catch (IOException e) {
      throw new HoodieIOException("Unable to read schema from HFile", e);
    }
  }

  private static GenericRecord getRecordFromKeyValue(KeyValue keyValue,
                                                     Schema writerSchema,
                                                     Schema readerSchema) throws IOException {
    byte[] bytes = keyValue.getBytes();
    return deserialize(
        bytes, keyValue.getKeyContentOffset(), keyValue.getKeyContentLength(),
        bytes, keyValue.getValueOffset(), keyValue.getValueLength(),
        writerSchema,
        readerSchema);
  }

  private byte[] getHFileMetaInfoFromCache(String key) throws IOException {
    if (!PRELOADED_META_INFO_KEYS.contains(key)) {
      throw new IllegalStateException("HoodieNativeAvroHFileReader#getHFileMetaInfoFromCache"
          + " should only be called on supported meta info keys; this key is not supported: "
          + key);
    }
    loadAllMetaInfoIntoCacheIfNeeded();
    return metaInfoMap.get(key);
  }

  private synchronized void loadAllMetaInfoIntoCacheIfNeeded() throws IOException {
    if (!isMetaInfoLoaded) {
      // Load all meta info that are small into cache
      try (HFileReader reader = newHFileReader()) {
        this.numKeyValueEntries = reader.getNumKeyValueEntries();
        for (String metaInfoKey : PRELOADED_META_INFO_KEYS) {
          Option<byte[]> metaInfo = reader.getMetaInfo(new UTF8StringKey(metaInfoKey));
          if (metaInfo.isPresent()) {
            metaInfoMap.put(metaInfoKey, metaInfo.get());
          }
        }
        isMetaInfoLoaded = true;
      } catch (Exception e) {
        throw new IOException("Unable to construct HFile reader", e);
      }
    }
  }

  private HFileReader newHFileReader() throws IOException {
    SeekableDataInputStream inputStream;
    long fileSize;
    if (path.isPresent()) {
      fileSize = storage.getPathInfo(path.get()).getLength();
      inputStream = storage.openSeekable(path.get(), false);
    } else {
      fileSize = bytesContent.get().length;
      inputStream = new ByteArraySeekableDataInputStream(new ByteBufferBackedInputStream(bytesContent.get()));
    }
    return new HFileReaderImpl(inputStream, fileSize);
  }

  public ClosableIterator<IndexedRecord> getIndexedRecordsByKeysIterator(List<String> sortedKeys,
                                                                         Schema readerSchema) throws IOException {
    HFileReader reader = newHFileReader();
    return new RecordByKeyIterator(reader, sortedKeys, getSchema(), schema.get());
  }

  @Override
  public ClosableIterator<IndexedRecord> getIndexedRecordsByKeyPrefixIterator(List<String> sortedKeyPrefixes,
                                                                              Schema readerSchema) throws IOException {
    HFileReader reader = newHFileReader();
    return new RecordByKeyPrefixIterator(reader, sortedKeyPrefixes, getSchema(), readerSchema);
  }

  private static class RecordIterator implements ClosableIterator<IndexedRecord> {
    private final HFileReader reader;

    private final Schema writerSchema;
    private final Schema readerSchema;

    private IndexedRecord next = null;
    private boolean eof = false;

    RecordIterator(HFileReader reader, Schema writerSchema, Schema readerSchema) {
      this.reader = reader;
      this.writerSchema = writerSchema;
      this.readerSchema = readerSchema;
    }

    @Override
    public boolean hasNext() {
      try {
        // NOTE: This is required for idempotency
        if (eof) {
          return false;
        }

        if (next != null) {
          return true;
        }

        boolean hasRecords;
        if (!reader.isSeeked()) {
          hasRecords = reader.seekTo();
        } else {
          hasRecords = reader.next();
        }

        if (!hasRecords) {
          eof = true;
          return false;
        }

        this.next = getRecordFromKeyValue(reader.getKeyValue().get(), writerSchema, readerSchema);
        return true;
      } catch (IOException io) {
        throw new HoodieIOException("unable to read next record from hfile ", io);
      }
    }

    @Override
    public IndexedRecord next() {
      IndexedRecord next = this.next;
      this.next = null;
      return next;
    }

    @Override
    public void close() {
      try {
        reader.close();
      } catch (IOException e) {
        throw new HoodieIOException("Error closing the HFile reader", e);
      }
    }
  }

  private static class RecordByKeyIterator implements ClosableIterator<IndexedRecord> {
    private final Iterator<String> sortedKeyIterator;

    private final HFileReader reader;

    private final Schema readerSchema;
    private final Schema writerSchema;

    private IndexedRecord next = null;

    RecordByKeyIterator(HFileReader reader, List<String> sortedKeys, Schema writerSchema,
                        Schema readerSchema) throws IOException {
      this.sortedKeyIterator = sortedKeys.iterator();
      this.reader = reader;
      this.reader.seekTo(); // position at the beginning of the file

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

        while (sortedKeyIterator.hasNext()) {
          UTF8StringKey key = new UTF8StringKey(sortedKeyIterator.next());
          if (reader.seekTo(key) == HFileReader.SEEK_TO_FOUND) {
            // Key is found
            KeyValue keyValue = reader.getKeyValue().get();
            next = deserialize(
                key.getBytes(), key.getContentOffset(), key.getContentLength(),
                keyValue.getBytes(), keyValue.getValueOffset(), keyValue.getValueLength(),
                writerSchema, readerSchema);
            return true;
          }
        }
        return false;
      } catch (IOException e) {
        throw new HoodieIOException("Unable to read next record from HFile ", e);
      }
    }

    @Override
    public IndexedRecord next() {
      IndexedRecord next = this.next;
      this.next = null;
      return next;
    }

    @Override
    public void close() {
      try {
        reader.close();
      } catch (IOException e) {
        throw new HoodieIOException("Error closing the HFile reader", e);
      }
    }
  }

  private static class RecordByKeyPrefixIterator implements ClosableIterator<IndexedRecord> {
    private final Iterator<String> sortedKeyPrefixesIterator;
    private Iterator<IndexedRecord> recordsIterator;

    private final HFileReader reader;

    private final Schema writerSchema;
    private final Schema readerSchema;

    private IndexedRecord next = null;
    private boolean isFirstKeyPrefix = true;

    RecordByKeyPrefixIterator(HFileReader reader, List<String> sortedKeyPrefixes,
                              Schema writerSchema, Schema readerSchema) throws IOException {
      this.sortedKeyPrefixesIterator = sortedKeyPrefixes.iterator();
      this.reader = reader;
      this.reader.seekTo(); // position at the beginning of the file

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
          } else if (sortedKeyPrefixesIterator.hasNext()) {
            recordsIterator = getRecordByKeyPrefixIteratorInternal(
                reader, isFirstKeyPrefix, sortedKeyPrefixesIterator.next(), writerSchema, readerSchema);
            isFirstKeyPrefix = false;
          } else {
            return false;
          }
        }
      } catch (IOException e) {
        throw new HoodieIOException("Unable to read next record from HFile", e);
      }
    }

    @Override
    public IndexedRecord next() {
      IndexedRecord next = this.next;
      this.next = null;
      return next;
    }

    @Override
    public void close() {
      try {
        reader.close();
      } catch (IOException e) {
        throw new HoodieIOException("Error closing the HFile reader and scanner", e);
      }
    }

    private static Iterator<IndexedRecord> getRecordByKeyPrefixIteratorInternal(HFileReader reader,
                                                                                boolean isFirstKeyPrefix,
                                                                                String keyPrefix,
                                                                                Schema writerSchema,
                                                                                Schema readerSchema)
        throws IOException {
      UTF8StringKey lookUpKeyPrefix = new UTF8StringKey(keyPrefix);
      if (!isFirstKeyPrefix) {
        // For the subsequent key prefixes after the first, do special handling to
        // avoid potential backward seeks.
        Option<KeyValue> keyValue = reader.getKeyValue();
        if (!keyValue.isPresent()) {
          return Collections.emptyIterator();
        }
        if (!isPrefixOfKey(lookUpKeyPrefix, keyValue.get().getKey())) {
          // If the key at current cursor does not start with the lookup prefix.
          if (lookUpKeyPrefix.compareTo(keyValue.get().getKey()) < 0) {
            // Prefix is less than the current key, no key found for the prefix.
            return Collections.emptyIterator();
          } else {
            // Prefix is greater than the current key. Call seekTo to move the cursor.
            if (reader.seekTo(lookUpKeyPrefix) >= HFileReader.SEEK_TO_IN_RANGE) {
              // Try moving to next entry, matching the prefix key; if we're at the EOF,
              // `next()` will return false
              if (!reader.next()) {
                return Collections.emptyIterator();
              }
            }
          }
        }
        // If the key current cursor starts with the lookup prefix,
        // do not call seekTo. Continue with reading the keys with the prefix.
      } else {
        // For the first key prefix, directly do seekTo.
        if (reader.seekTo(lookUpKeyPrefix) >= HFileReader.SEEK_TO_IN_RANGE) {
          // Try moving to next entry, matching the prefix key; if we're at the EOF,
          // `next()` will return false
          if (!reader.next()) {
            return Collections.emptyIterator();
          }
        }
      }

      class KeyPrefixIterator implements Iterator<IndexedRecord> {
        private IndexedRecord next = null;
        private boolean eof = false;

        @Override
        public boolean hasNext() {
          if (next != null) {
            return true;
          } else if (eof) {
            return false;
          }

          // Extract the byte value before releasing the lock since we cannot hold on to the returned cell afterwards
          try {
            KeyValue keyValue = reader.getKeyValue().get();
            // Check whether we're still reading records corresponding to the key-prefix
            if (!isPrefixOfKey(lookUpKeyPrefix, keyValue.getKey())) {
              return false;
            }
            byte[] bytes = keyValue.getBytes();
            next =
                deserialize(
                    bytes, keyValue.getKeyContentOffset(), keyValue.getKeyContentLength(),
                    bytes, keyValue.getValueOffset(), keyValue.getValueLength(),
                    writerSchema, readerSchema);
            // In case scanner is not able to advance, it means we reached EOF
            eof = !reader.next();
          } catch (IOException e) {
            throw new HoodieIOException("Failed to deserialize payload", e);
          }

          return true;
        }

        @Override
        public IndexedRecord next() {
          IndexedRecord next = this.next;
          this.next = null;
          return next;
        }
      }

      return new KeyPrefixIterator();
    }
  }
}
