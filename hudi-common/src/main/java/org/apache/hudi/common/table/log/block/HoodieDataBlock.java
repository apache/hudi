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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.FSDataInputStream;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * DataBlock contains a list of records serialized using formats compatible with the base file format.
 * For each base file format there is a corresponding DataBlock format.
 *
 * The Datablock contains:
 *   1. Data Block version
 *   2. Total number of records in the block
 *   3. Actual serialized content of the records
 */
public abstract class HoodieDataBlock extends HoodieLogBlock {

  protected List<IndexedRecord> records;
  protected Schema schema;
  protected String keyField;

  public HoodieDataBlock(@Nonnull Map<HeaderMetadataType, String> logBlockHeader,
      @Nonnull Map<HeaderMetadataType, String> logBlockFooter,
      @Nonnull Option<HoodieLogBlockContentLocation> blockContentLocation, @Nonnull Option<byte[]> content,
      FSDataInputStream inputStream, boolean readBlockLazily) {
    super(logBlockHeader, logBlockFooter, blockContentLocation, content, inputStream, readBlockLazily);
    this.keyField = HoodieRecord.RECORD_KEY_METADATA_FIELD;
  }

  public HoodieDataBlock(@Nonnull List<IndexedRecord> records, @Nonnull Map<HeaderMetadataType, String> header,
                         @Nonnull Map<HeaderMetadataType, String> footer, String keyField) {
    this(header, footer, Option.empty(), Option.empty(), null, false);
    this.records = records;
    this.schema = new Schema.Parser().parse(super.getLogBlockHeader().get(HeaderMetadataType.SCHEMA));
    this.keyField = keyField;
  }

  protected HoodieDataBlock(Option<byte[]> content, @Nonnull FSDataInputStream inputStream, boolean readBlockLazily,
                            Option<HoodieLogBlockContentLocation> blockContentLocation, Schema readerSchema,
                            @Nonnull Map<HeaderMetadataType, String> headers, @Nonnull Map<HeaderMetadataType,
      String> footer, String keyField) {
    this(headers, footer, blockContentLocation, content, inputStream, readBlockLazily);
    this.schema = readerSchema;
    this.keyField = keyField;
  }

  /**
   * Util method to get a data block for the requested type.
   *
   * @param logDataBlockFormat - Data block type
   * @param recordList         - List of records that goes in the data block
   * @param header             - data block header
   * @return Data block of the requested type.
   */
  public static HoodieLogBlock getBlock(HoodieLogBlockType logDataBlockFormat, List<IndexedRecord> recordList,
                                        Map<HeaderMetadataType, String> header) {
    return getBlock(logDataBlockFormat, recordList, header, HoodieRecord.RECORD_KEY_METADATA_FIELD);
  }

  /**
   * Util method to get a data block for the requested type.
   *
   * @param logDataBlockFormat - Data block type
   * @param recordList         - List of records that goes in the data block
   * @param header             - data block header
   * @param keyField           - FieldId to get the key from the records
   * @return Data block of the requested type.
   */
  public static HoodieLogBlock getBlock(HoodieLogBlockType logDataBlockFormat, List<IndexedRecord> recordList,
                                        Map<HeaderMetadataType, String> header, String keyField) {
    switch (logDataBlockFormat) {
      case AVRO_DATA_BLOCK:
        return new HoodieAvroDataBlock(recordList, header, keyField);
      case HFILE_DATA_BLOCK:
        return new HoodieHFileDataBlock(recordList, header, keyField);
      default:
        throw new HoodieException("Data block format " + logDataBlockFormat + " not implemented");
    }
  }

  @Override
  public byte[] getContentBytes() throws IOException {
    // In case this method is called before realizing records from content
    if (getContent().isPresent()) {
      return getContent().get();
    } else if (readBlockLazily && !getContent().isPresent() && records == null) {
      // read block lazily
      createRecordsFromContentBytes();
    }

    return serializeRecords();
  }

  public abstract HoodieLogBlockType getBlockType();

  public List<IndexedRecord> getRecords() {
    if (records == null) {
      try {
        // in case records are absent, read content lazily and then convert to IndexedRecords
        createRecordsFromContentBytes();
      } catch (IOException io) {
        throw new HoodieIOException("Unable to convert content bytes to records", io);
      }
    }
    return records;
  }

  /**
   * Batch get of keys of interest. Implementation can choose to either do full scan and return matched entries or
   * do a seek based parsing and return matched entries.
   * @param keys keys of interest.
   * @return List of IndexedRecords for the keys of interest.
   * @throws IOException
   */
  public List<IndexedRecord> getRecords(List<String> keys) throws IOException {
    throw new UnsupportedOperationException("On demand batch get based on interested keys not supported");
  }

  public Schema getSchema() {
    // if getSchema was invoked before converting byte [] to records
    if (records == null) {
      getRecords();
    }
    return schema;
  }

  protected void createRecordsFromContentBytes() throws IOException {
    if (readBlockLazily && !getContent().isPresent()) {
      // read log block contents from disk
      inflate();
    }

    deserializeRecords();
  }

  protected abstract byte[] serializeRecords() throws IOException;

  protected abstract void deserializeRecords() throws IOException;
}
