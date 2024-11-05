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

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.SeekableDataInputStream;
import org.apache.hudi.storage.HoodieStorage;

import org.apache.avro.Schema;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.hudi.common.util.TypeUtils.unsafeCast;
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
  private final Option<List<HoodieRecord>> records;

  /**
   * Key field's name w/in the record's schema
   */
  private final String keyFieldName;

  private final boolean enablePointLookups;

  protected Schema readerSchema;

  //  Map of string schema to parsed schema.
  private static ConcurrentHashMap<String, Schema> schemaMap = new ConcurrentHashMap<>();

  /**
   * NOTE: This ctor is used on the write-path (ie when records ought to be written into the log)
   */
  public HoodieDataBlock(List<HoodieRecord> records,
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
                            Supplier<SeekableDataInputStream> inputStreamSupplier,
                            boolean readBlockLazily,
                            Option<HoodieLogBlockContentLocation> blockContentLocation,
                            Option<Schema> readerSchema,
                            Map<HeaderMetadataType, String> headers,
                            Map<HeaderMetadataType, String> footer,
                            String keyFieldName,
                            boolean enablePointLookups) {
    super(headers, footer, blockContentLocation, content, inputStreamSupplier, readBlockLazily);
    this.records = Option.empty();
    this.keyFieldName = keyFieldName;
    // If no reader-schema has been provided assume writer-schema as one
    this.readerSchema = readerSchema.orElseGet(() -> getWriterSchema(super.getLogBlockHeader()));
    this.enablePointLookups = enablePointLookups;
  }

  @Override
  public byte[] getContentBytes(HoodieStorage storage) throws IOException {
    // In case this method is called before realizing records from content
    Option<byte[]> content = getContent();

    checkState(content.isPresent() || records.isPresent(), "Block is in invalid state");

    if (content.isPresent()) {
      return content.get();
    }

    return serializeRecords(records.get(), storage);
  }

  public String getKeyFieldName() {
    return keyFieldName;
  }

  protected static Schema getWriterSchema(Map<HeaderMetadataType, String> logBlockHeader) {
    return new Schema.Parser().parse(logBlockHeader.get(HeaderMetadataType.SCHEMA));
  }

  /**
   * Returns all the records iterator contained w/in this block.
   */
  public final <T> ClosableIterator<HoodieRecord<T>> getRecordIterator(HoodieRecordType type) {
    if (records.isPresent()) {
      // TODO need convert record type
      return list2Iterator(unsafeCast(records.get()));
    }
    try {
      // in case records are absent, read content lazily and then convert to IndexedRecords
      return readRecordsFromBlockPayload(type);
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
  public final <T> ClosableIterator<HoodieRecord<T>> getRecordIterator(List<String> keys, boolean fullKey, HoodieRecordType type) throws IOException {
    boolean fullScan = keys.isEmpty();
    if (enablePointLookups && !fullScan) {
      return lookupRecords(keys, fullKey);
    }

    // Otherwise, we fetch all the records and filter out all the records, but the
    // ones requested
    ClosableIterator<HoodieRecord<T>> allRecords = getRecordIterator(type);
    if (fullScan) {
      return allRecords;
    }

    HashSet<String> keySet = new HashSet<>(keys);
    return FilteringIterator.getInstance(allRecords, keySet, fullKey, this::getRecordKey);
  }

  protected <T> ClosableIterator<HoodieRecord<T>> readRecordsFromBlockPayload(HoodieRecordType type) throws IOException {
    if (readBlockLazily && !getContent().isPresent()) {
      // read log block contents from disk
      inflate();
    }

    try {
      return deserializeRecords(getContent().get(), type);
    } finally {
      // Free up content to be GC'd by deflating the block
      deflate();
    }
  }

  protected <T> ClosableIterator<HoodieRecord<T>> lookupRecords(List<String> keys, boolean fullKey) throws IOException {
    throw new UnsupportedOperationException(
        String.format("Point lookups are not supported by this Data block type (%s)", getBlockType())
    );
  }

  protected abstract byte[] serializeRecords(List<HoodieRecord> records, HoodieStorage storage) throws IOException;

  protected abstract <T> ClosableIterator<HoodieRecord<T>> deserializeRecords(byte[] content, HoodieRecordType type) throws IOException;

  public abstract HoodieLogBlockType getBlockType();

  protected Option<Schema.Field> getKeyField(Schema schema) {
    return Option.ofNullable(schema.getField(keyFieldName));
  }

  protected Option<String> getRecordKey(HoodieRecord record) {
    return Option.ofNullable(record.getRecordKey(readerSchema, keyFieldName));
  }

  protected Schema getSchemaFromHeader() {
    String schemaStr = getLogBlockHeader().get(HeaderMetadataType.SCHEMA);
    schemaMap.computeIfAbsent(schemaStr, (schemaString) -> new Schema.Parser().parse(schemaString));
    return schemaMap.get(schemaStr);
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
   */
  private static class FilteringIterator<T> implements ClosableIterator<HoodieRecord<T>> {
    private final ClosableIterator<HoodieRecord<T>> nested; // nested iterator

    private final Set<String> keys; // the filtering keys
    private final boolean fullKey;

    private final Function<HoodieRecord<T>, Option<String>> keyExtract; // function to extract the key

    private HoodieRecord<T> next;

    private FilteringIterator(ClosableIterator<HoodieRecord<T>> nested, Set<String> keys, boolean fullKey, Function<HoodieRecord<T>, Option<String>> keyExtract) {
      this.nested = nested;
      this.keys = keys;
      this.fullKey = fullKey;
      this.keyExtract = keyExtract;
    }

    public static <T> FilteringIterator<T> getInstance(
        ClosableIterator<HoodieRecord<T>> nested,
        Set<String> keys,
        boolean fullKey,
        Function<HoodieRecord<T>, Option<String>> keyExtract) {
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
    public HoodieRecord<T> next() {
      return this.next;
    }
  }
}
