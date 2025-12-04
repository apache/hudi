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

import org.apache.hudi.avro.AvroSchemaCache;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.exception.HoodieAvroSchemaException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.SeekableDataInputStream;
import org.apache.hudi.storage.HoodieStorage;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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
  private static final Logger LOG = LoggerFactory.getLogger(HoodieDataBlock.class);

  // TODO rebase records/content to leverage Either to warrant
  //      that they are mutex (used by read/write flows respectively)
  protected Option<List<HoodieRecord>> records;

  /**
   * Key field's name w/in the record's schema
   */
  private final String keyFieldName;

  private final boolean enablePointLookups;

  protected HoodieSchema readerSchema;

  //  Map of string schema to parsed schema.
  private static final ConcurrentHashMap<String, HoodieSchema> SCHEMA_MAP = new ConcurrentHashMap<>();

  /**
   * NOTE: This ctor is used on the write-path (ie when records ought to be written into the log)
   */
  public HoodieDataBlock(List<HoodieRecord> records,
                         Map<HeaderMetadataType, String> header,
                         Map<FooterMetadataType, String> footer,
                         String keyFieldName) {
    super(header, footer, Option.empty(), Option.empty(), null, false);
    addRecordPositionsIfRequired(records, HoodieRecord::getCurrentPosition);
    this.records = Option.of(records);
    this.keyFieldName = keyFieldName;
    // If no reader-schema has been provided assume writer-schema as one
    // TODO: Use HoodieSchemaCache after #14374 has been merged
    this.readerSchema = HoodieSchema.fromAvroSchema(AvroSchemaCache.intern(getWriterSchema(super.getLogBlockHeader())));
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
                            Map<FooterMetadataType, String> footer,
                            String keyFieldName,
                            boolean enablePointLookups) {
    super(headers, footer, blockContentLocation, content, inputStreamSupplier, readBlockLazily);
    this.records = Option.empty();
    this.keyFieldName = keyFieldName;
    this.readerSchema = containsPartialUpdates()
        // When the data block contains partial updates, we need to strictly use the writer schema
        // from the log block header, as we need to use the partial schema to indicate which
        // fields are updated during merging.
        // TODO: Use HoodieSchemaCache after #14374 has been merged
        ? HoodieSchema.fromAvroSchema(AvroSchemaCache.intern(getWriterSchema(super.getLogBlockHeader())))
        // If no reader-schema has been provided assume writer-schema as one
        // TODO: Use HoodieSchemaCache after #14374 has been merged
        : HoodieSchema.fromAvroSchema(AvroSchemaCache.intern(readerSchema.orElseGet(() -> getWriterSchema(super.getLogBlockHeader()))));
    this.enablePointLookups = enablePointLookups;
  }

  @Override
  public ByteArrayOutputStream getContentBytes(HoodieStorage storage) throws IOException {
    // In case this method is called before realizing records from content
    Option<byte[]> content = getContent();

    checkState(content.isPresent() || records.isPresent(), "Block is in invalid state");

    Option<ByteArrayOutputStream> baosOpt = getContentAsByteStream();
    if (baosOpt.isPresent()) {
      return baosOpt.get();
    }

    return serializeRecords(records.get(), storage);
  }

  public String getKeyFieldName() {
    return keyFieldName;
  }

  public boolean containsPartialUpdates() {
    return getLogBlockHeader().containsKey(HeaderMetadataType.IS_PARTIAL)
        && Boolean.parseBoolean(getLogBlockHeader().get(HeaderMetadataType.IS_PARTIAL));
  }

  protected static Schema getWriterSchema(Map<HeaderMetadataType, String> logBlockHeader) {
    return new Schema.Parser().parse(logBlockHeader.get(HeaderMetadataType.SCHEMA));
  }

  /**
   * Returns an iterator over all the records contained within this block.
   * This method uses a default buffer size of 0, which means it will read
   * the entire Data Block content at once.
   *
   * @param type The type of HoodieRecord.
   * @param <T>  The type parameter for HoodieRecord.
   * @return A ClosableIterator over HoodieRecord<T>.
   */
  public final <T> ClosableIterator<HoodieRecord<T>> getRecordIterator(HoodieRecordType type) {
    return getRecordIterator(type, 0);
  }

  /**
   * Returns an iterator over all the records contained within this block.
   *
   * @param type       The type of HoodieRecord.
   * @param bufferSize The size of the buffer for streaming read.
   *                   A bufferSize less than or equal to 0 means that streaming read is disabled and
   *                   the entire block content will be read at once.
   *                   A bufferSize greater than 0 enables streaming read with the specified buffer size.
   * @param <T>        The type parameter for HoodieRecord.
   * @return A ClosableIterator over HoodieRecord<T>.
   * @throws HoodieIOException If there is an error reading records from the block payload.
   */
  public final <T> ClosableIterator<HoodieRecord<T>> getRecordIterator(HoodieRecordType type, int bufferSize) {
    if (records.isPresent()) {
      // TODO need convert record type
      return list2Iterator(unsafeCast(records.get()));
    }
    try {
      // in case records are absent, read content lazily and then convert to IndexedRecords
      return readRecordsFromBlockPayload(type, bufferSize);
    } catch (IOException io) {
      throw new HoodieIOException("Unable to convert content bytes to records", io);
    }
  }

  public HoodieSchema getSchema() {
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
    return getRecordIterator(keys, fullKey, type, 0);
  }

  /**
   * Batch get of keys of interest. Implementation can choose to either do full scan and return matched entries or
   * do a seek based parsing and return matched entries.
   *
   * @param keys keys of interest.
   * @param bufferSize The size of the buffer for streaming read.
   *                   A bufferSize less than or equal to 0 means that streaming read is disabled and
   *                   the entire block content will be read at once.
   *                   A bufferSize greater than 0 enables streaming read with the specified buffer size.
   * @param <T>        The type parameter for HoodieRecord.
   * @return List of IndexedRecords for the keys of interest.
   * @throws IOException in case of failures encountered when reading/parsing records
   */
  public final <T> ClosableIterator<HoodieRecord<T>> getRecordIterator(List<String> keys, boolean fullKey, HoodieRecordType type, int bufferSize) throws IOException {
    boolean fullScan = keys.isEmpty();
    if (enablePointLookups && !fullScan) {
      return lookupRecords(keys, fullKey);
    }

    // Otherwise, we fetch all the records and filter out all the records, but the
    // ones requested
    ClosableIterator<HoodieRecord<T>> allRecords = getRecordIterator(type, bufferSize);
    if (fullScan) {
      return allRecords;
    }

    HashSet<String> keySet = new HashSet<>(keys);
    return FilteringIterator.getInstance(allRecords, keySet, fullKey, this::getRecordKey);
  }

  /**
   * Returns all the records in the type of engine-specific record representation contained
   * within this block in an iterator.
   *
   * @param readerContext {@link HoodieReaderContext} instance with type T.
   * @param <T>           The type of engine-specific record representation to return.
   * @return An iterator containing all records in specified type.
   */
  public final <T> ClosableIterator<T> getEngineRecordIterator(HoodieReaderContext<T> readerContext) {
    if (records.isPresent()) {
      return list2Iterator(unsafeCast(
          records.get().stream().map(hoodieRecord -> (T) hoodieRecord.getData())
              .collect(Collectors.toList())));
    }
    try {
      return readRecordsFromBlockPayload(readerContext);
    } catch (IOException io) {
      throw new HoodieIOException("Unable to convert content bytes to records", io);
    }
  }

  /**
   * Batch get of keys of interest. Implementation can choose to either do full scan and return matched entries or
   * do a seek based parsing and return matched entries.
   *
   * @param readerContext {@link HoodieReaderContext} instance with type T.
   * @param keys          Keys of interest.
   * @param fullKey       Whether the key is full or not.
   * @param <T>           The type of engine-specific record representation to return.
   * @return An iterator containing the records of interest in specified type.
   */
  public final <T> ClosableIterator<T> getEngineRecordIterator(HoodieReaderContext<T> readerContext, List<String> keys, boolean fullKey) throws IOException {
    boolean fullScan = keys.isEmpty();
    if (!fullScan) {
      return lookupEngineRecords(keys, fullKey);
    } else {
      throw new IllegalStateException("Unexpected code reached. Expected to be called only with keySpec defined for non FILES partition in Metadata table");
    }
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

  /**
   * Reads records from the block payload using a specified buffer size.
   * This method attempts to read serialized records from the block payload, leveraging a buffer size for streaming reads.
   * If the buffer size is less than or equal to 0, it reads the entire block content at once.
   *
   * @param bufferSize The size of the buffer for streaming read.
   *                   A bufferSize less than or equal to 0 means that streaming read is disabled and
   *                   the entire block content will be read at once.
   *                   A bufferSize greater than 0 enables streaming read with the specified buffer size.
   * @return A ClosableIterator over HoodieRecord<T>.
   * @throws IOException If there is an error reading or deserializing the records.
   */
  protected <T> ClosableIterator<HoodieRecord<T>> readRecordsFromBlockPayload(HoodieRecordType type, int bufferSize) throws IOException {
    if (getContent().isPresent() || bufferSize <= 0) {
      return readRecordsFromBlockPayload(type);
    }

    return deserializeRecords(getInputStreamSupplier().get(), getBlockContentLocation().get(), type, bufferSize);
  }

  protected <T> ClosableIterator<T> readRecordsFromBlockPayload(HoodieReaderContext<T> readerContext) throws IOException {
    if (readBlockLazily && !getContent().isPresent()) {
      // read log block contents from disk
      inflate();
    }

    try {
      return deserializeRecords(readerContext, getContent().get());
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

  protected <T> ClosableIterator<T> lookupEngineRecords(List<String> keys, boolean fullKey) throws IOException {
    throw new UnsupportedOperationException(
        String.format("Point lookups are not supported by this Data block type (%s)", getBlockType())
    );
  }

  protected abstract ByteArrayOutputStream serializeRecords(List<HoodieRecord> records, HoodieStorage storage) throws IOException;

  protected abstract <T> ClosableIterator<HoodieRecord<T>> deserializeRecords(byte[] content, HoodieRecordType type) throws IOException;

  /**
   * Streaming deserialization of records.
   *
   * @param inputStream The input stream from which to read the records.
   * @param contentLocation The location within the input stream where the content starts.
   * @param bufferSize The size of the buffer to use for reading the records.
   * @return A ClosableIterator over HoodieRecord<T>.
   * @throws IOException If there is an error reading or deserializing the records.
   */
  protected abstract <T> ClosableIterator<HoodieRecord<T>> deserializeRecords(
      SeekableDataInputStream inputStream,
      HoodieLogBlockContentLocation contentLocation,
      HoodieRecordType type,
      int bufferSize
  ) throws IOException;

  /**
   * Deserializes the content bytes of the data block to the records in engine-specific representation.
   *
   * @param readerContext Hudi reader context with engine-specific implementation.
   * @param content       Content in byte array.
   * @param <T>           Record type of engine-specific representation.
   * @return {@link ClosableIterator} of records in engine-specific representation.
   * @throws IOException upon deserialization error.
   */
  protected abstract <T> ClosableIterator<T> deserializeRecords(HoodieReaderContext<T> readerContext, byte[] content) throws IOException;

  public abstract HoodieLogBlockType getBlockType();

  protected Option<Schema.Field> getKeyField(Schema schema) {
    return Option.ofNullable(schema.getField(keyFieldName));
  }

  protected Option<String> getRecordKey(HoodieRecord record) {
    return Option.ofNullable(record.getRecordKey(readerSchema.toAvroSchema(), keyFieldName));
  }

  protected HoodieSchema getSchemaFromHeader() {
    String schemaStr = getLogBlockHeader().get(HeaderMetadataType.SCHEMA);
    SCHEMA_MAP.computeIfAbsent(schemaStr,
        (schemaString) -> {
          try {
            return HoodieSchema.parse(schemaStr);
          } catch (HoodieAvroSchemaException e) {
            // Archived commits from earlier hudi versions fail the schema check
            // So we retry in this one specific instance with validation disabled
            if (e.getCause() instanceof AvroTypeException) {
              return HoodieSchema.parse(schemaStr, false);
            }
            throw e;
          }
        });
    return SCHEMA_MAP.get(schemaStr);
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

  /**
   * A {@link ClosableIterator} that supports filtering strategy with given keys on records
   * of engine-specific type.
   * User should supply the key extraction function for fetching string format keys.
   *
   * @param <T> The type of engine-specific record representation.
   */
  private static class FilteringEngineRecordIterator<T> implements ClosableIterator<T> {
    private final ClosableIterator<T> nested; // nested iterator

    private final Set<String> keys; // the filtering keys
    private final boolean fullKey;

    private final Function<T, Option<String>> keyExtract; // function to extract the key

    private T next;

    private FilteringEngineRecordIterator(ClosableIterator<T> nested,
                                          Set<String> keys, boolean fullKey,
                                          Function<T, Option<String>> keyExtract) {
      this.nested = nested;
      this.keys = keys;
      this.fullKey = fullKey;
      this.keyExtract = keyExtract;
    }

    public static <T> FilteringEngineRecordIterator<T> getInstance(
        ClosableIterator<T> nested,
        Set<String> keys,
        boolean fullKey,
        Function<T, Option<String>> keyExtract) {
      return new FilteringEngineRecordIterator<>(nested, keys, fullKey, keyExtract);
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
