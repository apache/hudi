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

package org.apache.hudi.common.table.log.block;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.io.storage.HoodieHFileKeyExcludedReader;
import org.apache.hudi.io.storage.HoodieHFileReader;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * HFile data block for the Metadata table records. Since the backing log format for metadata table
 * is the HFile KeyValue and since the key field in the metadata record payload is a duplicate
 * of the Key in the Cell, the redundant key field in the record can be nullified to save on the
 * cost. Such trimmed metadata records need to re-materialized with the key field during deserialization.
 */
public class HoodieHFileKeyExcludedDataBlock extends HoodieHFileDataBlock {

  private static final Logger LOG = LogManager.getLogger(HoodieHFileKeyExcludedDataBlock.class);

  public HoodieHFileKeyExcludedDataBlock(HoodieLogFile logFile, FSDataInputStream inputStream, Option<byte[]> content,
                                         boolean readBlockLazily, long position, long blockSize, long blockEndPos,
                                         Schema readerSchema, Map<HeaderMetadataType, String> header,
                                         Map<HeaderMetadataType, String> footer, boolean enableInlineReading,
                                         String keyField) {
    super(logFile, inputStream, content, readBlockLazily, position, blockSize, blockEndPos, readerSchema, header,
        footer, enableInlineReading, keyField);
  }

  public HoodieHFileKeyExcludedDataBlock(@NotNull List<IndexedRecord> records,
                                         @NotNull Map<HeaderMetadataType, String> header, String keyField) {
    super(records, header, keyField);
  }

  /**
   * Serialize the metadata table record to byte buffer after any field trimming if needed.
   *
   * @param record         - Record to serialize
   * @param schemaKeyField - Key field in the schema
   * @return Serialized byte array of the metadata trimmed record
   */
  @Override
  protected ByteBuffer serializeRecord(final IndexedRecord record, final Option<Schema.Field> schemaKeyField) {
    if (!schemaKeyField.isPresent()) {
      return super.serializeRecord(record, schemaKeyField);
    }

    ValidationUtils.checkArgument(record.getSchema() != null, "Unknown schema for the record!");
    record.put(schemaKeyField.get().pos(), "");
    return super.serializeRecord(record, schemaKeyField);
  }

  /**
   * Create a HFile reader for the metadata table log block content. Metadata table
   * records could have the key field trimmed from the payload to save on the storage.
   * If so, HoodieMetadataHFileReader does record transformation to materialize the
   * record fully with the key again.
   *
   * @param content  - Metadata table log block serialized content byte array
   * @param keyField - Key field in the schema
   * @return New HFile reader for the serialized metadata table log block content
   * @throws IOException
   */
  @Override
  protected HoodieHFileReader<IndexedRecord> createHFileReader(final byte[] content,
                                                               final String keyField) throws IOException {
    return new HoodieHFileKeyExcludedReader<>(content, keyField);
  }

  /**
   * Create a new HFile reader for the metadata table log file. Metadata table
   * records could have the key field trimmed from the payload to save on the storage.
   * If so, HoodieMetadataHFileReader does record transformation to materialize the
   * record fully with the key again.
   *
   * @param inlineConf - Common configuration
   * @param inlinePath - Log file path
   * @param cacheConf  - Cache configuration
   * @param fileSystem - Filesystem for the log file
   * @param keyField   - Key field in the schema
   * @return New HFile reader instance for the log file
   * @throws IOException
   */
  @Override
  protected HoodieHFileReader<IndexedRecord> createHFileReader(Configuration inlineConf, Path inlinePath,
                                                               CacheConfig cacheConf, FileSystem fileSystem,
                                                               String keyField) throws IOException {
    return new HoodieHFileKeyExcludedReader<>(inlineConf, inlinePath, cacheConf, fileSystem, keyField);
  }

}


