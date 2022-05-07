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

package org.apache.hudi.common.table.log.block;

import org.apache.hudi.common.util.ClosableIterator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hudi.internal.schema.InternalSchema;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.apache.hudi.common.util.ValidationUtils.checkState;

/**
 * DataBlock contains a list of records serialized using formats compatible with the base file format.
 * For each base file format there is a corresponding DataBlock format.
 * <p>
 * The Datablock contains:
 *   1. Data Block version
 *   2. Total number of records in the block
 *   3. Actual serialized content of the records
 */
public abstract class HoodieDataBlock extends HoodieLogBlock {

  // TODO rebase records/content to leverage Either to warrant
  //      that they are mutex (used by read/write flows respectively)
  private final Option<List<IndexedRecord>> records;

  /**
   * Key field's name w/in the record's schema
   */
  private final String keyFieldName;

  private final boolean enablePointLookups;

  protected final Schema readerSchema;

  protected InternalSchema internalSchema = InternalSchema.getEmptyInternalSchema();

  /**
   * NOTE: This ctor is used on the write-path (ie when records ought to be written into the log)
   */
  public HoodieDataBlock(List<IndexedRecord> records,
                         Map<HeaderMetadataType, String> header,
                         Map<HeaderMetadataType, String> footer,
                         String keyFieldName) {
    super(header, footer, Option.empty(), Option.empty(), null, false);
    this.records = Option.of(records);
    this.keyFieldName = keyFieldName;
    // If no reader-schema has been provided assume writer-schema as one
    this.readerSchema = getWriterSchema(super.getLogBlockHeader());
    this.enablePointLookups = false;
  }

  /**
   * NOTE: This ctor is used on the write-path (ie when records ought to be written into the log)
   */
  protected HoodieDataBlock(Option<byte[]> content,
                            FSDataInputStream inputStream,
                            boolean readBlockLazily,
                            Option<HoodieLogBlockContentLocation> blockContentLocation,
                            Option<Schema> readerSchema,
                            Map<HeaderMetadataType, String> headers,
                            Map<HeaderMetadataType, String> footer,
                            String keyFieldName,
                            boolean enablePointLookups) {
    super(headers, footer, blockContentLocation, content, inputStream, readBlockLazily);
    this.records = Option.empty();
    this.keyFieldName = keyFieldName;
    // If no reader-schema has been provided assume writer-schema as one
    this.readerSchema = readerSchema.orElseGet(() -> getWriterSchema(super.getLogBlockHeader()));
    this.enablePointLookups = enablePointLookups;
  }

  protected HoodieDataBlock(Option<byte[]> content,
                            FSDataInputStream inputStream,
                            boolean readBlockLazily,
                            Option<HoodieLogBlockContentLocation> blockContentLocation,
                            Option<Schema> readerSchema,
                            Map<HeaderMetadataType, String> headers,
                            Map<HeaderMetadataType, String> footer,
                            String keyFieldName,
                            boolean enablePointLookups,
                            InternalSchema internalSchema) {
    super(headers, footer, blockContentLocation, content, inputStream, readBlockLazily);
    this.records = Option.empty();
    this.keyFieldName = keyFieldName;
    // If no reader-schema has been provided assume writer-schema as one
    this.readerSchema = readerSchema.orElseGet(() -> getWriterSchema(super.getLogBlockHeader()));
    this.enablePointLookups = enablePointLookups;
    this.internalSchema = internalSchema == null ? InternalSchema.getEmptyInternalSchema() : internalSchema;
  }

  @Override
  public byte[] getContentBytes() throws IOException {
    // In case this method is called before realizing records from content
    Option<byte[]> content = getContent();

    checkState(content.isPresent() || records.isPresent(), "Block is in invalid state");

    if (content.isPresent()) {
      return content.get();
    }

    return serializeRecords(records.get());
  }

  protected static Schema getWriterSchema(Map<HeaderMetadataType, String> logBlockHeader) {
    return new Schema.Parser().parse(logBlockHeader.get(HeaderMetadataType.SCHEMA));
  }

  /**
   * Returns all the records iterator contained w/in this block.
   */
  public final ClosableIterator<IndexedRecord> getRecordIterator() {
    if (records.isPresent()) {
      return list2Iterator(records.get());
    }
    try {
      // in case records are absent, read content lazily and then convert to IndexedRecords
      return readRecordsFromBlockPayload();
    } catch (IOException io) {
      throw new HoodieIOException("Unable to convert content bytes to records", io);
    }
  }

  public Schema getSchema() {
    return readerSchema;
  }

  /**
   * Batch get of keys of interest. Implementation can choose to either do full scan and return matched entries or
   * do a seek based parsing and return matched entries.
   *
   * @param keys keys of interest.
   * @return List of IndexedRecords for the keys of interest.
   * @throws IOException in case of failures encountered when reading/parsing records
   */
  public final ClosableIterator<IndexedRecord> getRecordIterator(List<String> keys, boolean fullKey) throws IOException {
    boolean fullScan = keys.isEmpty();
    if (enablePointLookups && !fullScan) {
      return lookupRecords(keys, fullKey);
    }

    // Otherwise, we fetch all the records and filter out all the records, but the
    // ones requested
    ClosableIterator<IndexedRecord> allRecords = getRecordIterator();
    if (fullScan) {
      return allRecords;
    }

    HashSet<String> keySet = new HashSet<>(keys);
    return FilteringIterator.getInstance(allRecords, keySet, fullKey, this::getRecordKey);
  }

  protected ClosableIterator<IndexedRecord> readRecordsFromBlockPayload() throws IOException {
    if (readBlockLazily && !getContent().isPresent()) {
      // read log block contents from disk
      inflate();
    }

    try {
      return deserializeRecords(getContent().get());
    } finally {
      // Free up content to be GC'd by deflating the block
      deflate();
    }
  }

  protected ClosableIterator<IndexedRecord> lookupRecords(List<String> keys, boolean fullKey) throws IOException {
    throw new UnsupportedOperationException(
        String.format("Point lookups are not supported by this Data block type (%s)", getBlockType())
    );
  }

  protected abstract byte[] serializeRecords(List<IndexedRecord> records) throws IOException;

  protected abstract ClosableIterator<IndexedRecord> deserializeRecords(byte[] content) throws IOException;

  public abstract HoodieLogBlockType getBlockType();

  protected Option<Schema.Field> getKeyField(Schema schema) {
    return Option.ofNullable(schema.getField(keyFieldName));
  }

  protected Option<String> getRecordKey(IndexedRecord record) {
    return getKeyField(record.getSchema())
        .map(keyField -> record.get(keyField.pos()))
        .map(Object::toString);
  }

  /**
   * Converts the given list to closable iterator.
   */
  static <T> ClosableIterator<T> list2Iterator(List<T> list) {
    Iterator<T> iterator = list.iterator();
    return new ClosableIterator<T>() {
      @Override
      public void close() {
        // ignored
      }

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public T next() {
        return iterator.next();
      }
    };
  }

  // -------------------------------------------------------------------------
  //  Inner Class
  // -------------------------------------------------------------------------

  /**
   * A {@link ClosableIterator} that supports filtering strategy with given keys.
   * User should supply the key extraction function for fetching string format keys.
   *
   * @param <T> the element type
   */
  private static class FilteringIterator<T extends IndexedRecord> implements ClosableIterator<T> {
    private final ClosableIterator<T> nested; // nested iterator

    private final Set<String> keys; // the filtering keys
    private final boolean fullKey;

    private final Function<T, Option<String>> keyExtract; // function to extract the key

    private T next;

    private FilteringIterator(ClosableIterator<T> nested, Set<String> keys, boolean fullKey, Function<T, Option<String>> keyExtract) {
      this.nested = nested;
      this.keys = keys;
      this.fullKey = fullKey;
      this.keyExtract = keyExtract;
    }

    public static <T extends IndexedRecord> FilteringIterator<T> getInstance(
        ClosableIterator<T> nested,
        Set<String> keys,
        boolean fullKey,
        Function<T, Option<String>> keyExtract) {
      return new FilteringIterator<>(nested, keys, fullKey, keyExtract);
    }

    @Override
    public void close() {
      this.nested.close();
    }

    @Override
    public boolean hasNext() {
      while (this.nested.hasNext()) {
        this.next = this.nested.next();
        String key = keyExtract.apply(this.next)
            .orElseGet(() -> {
              throw new IllegalStateException(String.format("Record without a key (%s)", this.next));
            });

        if (fullKey && keys.contains(key)
            || !fullKey && keys.stream().anyMatch(key::startsWith)) {
          return true;
        }
      }
      return false;
    }

    @Override
    public T next() {
      return this.next;
    }
  }
}
