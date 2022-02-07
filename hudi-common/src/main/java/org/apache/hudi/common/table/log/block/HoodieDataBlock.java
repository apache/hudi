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

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
  private Option<List<HoodieRecord>> records;

  /**
   * Key field's name w/in the record's schema
   */
  private final String keyFieldName;

  private final boolean enablePointLookups;

  protected final Schema readerSchema;

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
   * Returns all the records contained w/in this block
   */
  public final List<HoodieRecord> getRecords(HoodieRecordMapper mapper) {
    if (!records.isPresent()) {
      try {
        // in case records are absent, read content lazily and then convert to IndexedRecords
        records = Option.of(readRecordsFromBlockPayload(mapper));
      } catch (IOException io) {
        throw new HoodieIOException("Unable to convert content bytes to records", io);
      }
    }
    return records.get();
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
  public final List<HoodieRecord> getRecords(List<String> keys, HoodieRecordMapper mapper) throws IOException {
    boolean fullScan = keys.isEmpty();
    if (enablePointLookups && !fullScan) {
      return lookupRecords(keys, mapper);
    }

    // Otherwise, we fetch all the records and filter out all the records, but the
    // ones requested
    List<HoodieRecord> allRecords = getRecords(mapper);
    if (fullScan) {
      return allRecords;
    }

    HashSet<String> keySet = new HashSet<>(keys);
    return allRecords.stream()
        .filter(record -> keySet.contains(getRecordKey(record).orElse(null)))
        .collect(Collectors.toList());
  }

  protected List<HoodieRecord> readRecordsFromBlockPayload(HoodieRecordMapper mapper) throws IOException {
    if (readBlockLazily && !getContent().isPresent()) {
      // read log block contents from disk
      inflate();
    }

    try {
      return deserializeRecords(getContent().get(), mapper);
    } finally {
      // Free up content to be GC'd by deflating the block
      deflate();
    }
  }

  protected List<HoodieRecord> lookupRecords(List<String> keys, HoodieRecordMapper mapper) throws IOException {
    throw new UnsupportedOperationException(
        String.format("Point lookups are not supported by this Data block type (%s)", getBlockType())
    );
  }

  protected abstract byte[] serializeRecords(List<HoodieRecord> records) throws IOException;

  protected abstract List<HoodieRecord> deserializeRecords(byte[] content, HoodieRecordMapper mapper) throws IOException;

  public abstract HoodieLogBlockType getBlockType();

  protected Option<Schema.Field> getKeyField(Schema schema) {
    return Option.ofNullable(schema.getField(keyFieldName));
  }

  protected Option<String> getRecordKey(HoodieRecord record) {
    return Option.ofNullable(record.getRecordKey());
  }

  // TODO remove
  @FunctionalInterface
  public
  interface HoodieRecordMapper {
    HoodieRecord apply(IndexedRecord avroPayload);
  }
}
