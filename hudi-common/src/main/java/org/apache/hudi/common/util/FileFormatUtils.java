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

package org.apache.hudi.common.util;

import org.apache.hudi.avro.HoodieBloomFilterWriteSupport;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.bloom.BloomFilterTypeCode;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Utils for file format used in Hudi.
 */
public abstract class FileFormatUtils {
  /**
   * Read the rowKey list from the given data file.
   *
   * @param storage  {@link HoodieStorage} instance.
   * @param filePath the data file path.
   * @return set of row keys
   */
  public Set<String> readRowKeys(HoodieStorage storage, StoragePath filePath) {
    return filterRowKeys(storage, filePath, new HashSet<>());
  }

  /**
   * Read the bloom filter from the metadata of the given data file.
   *
   * @param storage  {@link HoodieStorage} instance.
   * @param filePath the data file path.
   * @return a BloomFilter object.
   */
  public BloomFilter readBloomFilterFromMetadata(HoodieStorage storage, StoragePath filePath) {
    Map<String, String> footerVals =
        readFooter(storage, false, filePath,
            HoodieBloomFilterWriteSupport.HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY,
            HoodieBloomFilterWriteSupport.OLD_HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY,
            HoodieBloomFilterWriteSupport.HOODIE_BLOOM_FILTER_TYPE_CODE);
    String footerVal = footerVals.get(HoodieBloomFilterWriteSupport.HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY);
    if (null == footerVal) {
      // We use old style key "com.uber.hoodie.bloomfilter"
      footerVal = footerVals.get(HoodieBloomFilterWriteSupport.OLD_HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY);
    }
    BloomFilter toReturn = null;
    if (footerVal != null) {
      if (footerVals.containsKey(HoodieBloomFilterWriteSupport.HOODIE_BLOOM_FILTER_TYPE_CODE)) {
        toReturn = BloomFilterFactory.fromString(footerVal,
            footerVals.get(HoodieBloomFilterWriteSupport.HOODIE_BLOOM_FILTER_TYPE_CODE));
      } else {
        toReturn = BloomFilterFactory.fromString(footerVal, BloomFilterTypeCode.SIMPLE.name());
      }
    }
    return toReturn;
  }

  /**
   * Read the min and max record key from the metadata of the given data file.
   *
   * @param storage  {@link HoodieStorage} instance.
   * @param filePath the data file path.
   * @return an array of two string where the first is min record key and the second is max record key.
   */
  public String[] readMinMaxRecordKeys(HoodieStorage storage, StoragePath filePath) {
    Map<String, String> minMaxKeys = readFooter(storage, true, filePath,
        HoodieBloomFilterWriteSupport.HOODIE_MIN_RECORD_KEY_FOOTER, HoodieBloomFilterWriteSupport.HOODIE_MAX_RECORD_KEY_FOOTER);
    if (minMaxKeys.size() != 2) {
      throw new HoodieException(
          String.format("Could not read min/max record key out of footer correctly from %s. read) : %s",
              filePath, minMaxKeys));
    }
    return new String[] {minMaxKeys.get(HoodieBloomFilterWriteSupport.HOODIE_MIN_RECORD_KEY_FOOTER),
        minMaxKeys.get(HoodieBloomFilterWriteSupport.HOODIE_MAX_RECORD_KEY_FOOTER)};
  }

  /**
   * Read the data file
   * NOTE: This literally reads the entire file contents, thus should be used with caution.
   *
   * @param storage  {@link HoodieStorage} instance.
   * @param filePath the data file path.
   * @return a list of GenericRecord.
   */
  public abstract List<GenericRecord> readAvroRecords(HoodieStorage storage, StoragePath filePath);

  /**
   * Read the data file using the given schema
   * NOTE: This literally reads the entire file contents, thus should be used with caution.
   *
   * @param storage  {@link HoodieStorage} instance.
   * @param filePath the data file path.
   * @return a list of GenericRecord.
   */
  public abstract List<GenericRecord> readAvroRecords(HoodieStorage storage, StoragePath filePath, Schema schema);

  /**
   * Read the footer data of the given data file.
   *
   * @param storage     {@link HoodieStorage} instance.
   * @param required    require the footer data to be in data file.
   * @param filePath    the data file path.
   * @param footerNames the footer names to read.
   * @return a map where the key is the footer name and the value is the footer value.
   */
  public abstract Map<String, String> readFooter(HoodieStorage storage, boolean required, StoragePath filePath,
                                                 String... footerNames);

  /**
   * Returns the number of records in the data file.
   *
   * @param storage  {@link HoodieStorage} instance.
   * @param filePath the data file path.
   */
  public abstract long getRowCount(HoodieStorage storage, StoragePath filePath);

  /**
   * Read the rowKey list matching the given filter, from the given data file.
   * If the filter is empty, then this will return all the row keys.
   *
   * @param storage  {@link HoodieStorage} instance.
   * @param filePath the data file path.
   * @param filter   record keys filter.
   * @return set of row keys matching candidateRecordKeys.
   */
  public abstract Set<String> filterRowKeys(HoodieStorage storage, StoragePath filePath, Set<String> filter);

  /**
   * Fetch {@link HoodieKey}s from the given data file.
   *
   * @param storage  {@link HoodieStorage} instance.
   * @param filePath the data file path.
   * @return {@link List} of {@link HoodieKey}s fetched from the data file.
   */
  public abstract List<HoodieKey> fetchHoodieKeys(HoodieStorage storage, StoragePath filePath);

  /**
   * Provides a closable iterator for reading the given data file.
   *
   * @param storage         {@link HoodieStorage} instance.
   * @param filePath        the data file path.
   * @param keyGeneratorOpt instance of KeyGenerator.
   * @return {@link ClosableIterator} of {@link HoodieKey}s for reading the file.
   */
  public abstract ClosableIterator<HoodieKey> getHoodieKeyIterator(HoodieStorage storage,
                                                                   StoragePath filePath,
                                                                   Option<BaseKeyGenerator> keyGeneratorOpt);

  /**
   * Provides a closable iterator for reading the given data file.
   *
   * @param storage  {@link HoodieStorage} instance.
   * @param filePath the data file path.
   * @return {@link ClosableIterator} of {@link HoodieKey}s for reading the file.
   */
  public abstract ClosableIterator<HoodieKey> getHoodieKeyIterator(HoodieStorage storage, StoragePath filePath);

  /**
   * Fetch {@link HoodieKey}s from the given data file.
   *
   * @param storage         {@link HoodieStorage} instance.
   * @param filePath        the data file path.
   * @param keyGeneratorOpt instance of KeyGenerator.
   * @return {@link List} of{@link HoodieKey}s fetched from the data file.
   */
  public abstract List<HoodieKey> fetchHoodieKeys(HoodieStorage storage,
                                                                           StoragePath filePath,
                                                                           Option<BaseKeyGenerator> keyGeneratorOpt);

  /**
   * Read the Avro schema of the data file.
   *
   * @param storage  {@link HoodieStorage} instance.
   * @param filePath the data file path.
   * @return the Avro schema of the data file.
   */
  public abstract Schema readAvroSchema(HoodieStorage storage, StoragePath filePath);

  /**
   * Reads column statistics stored in the metadata.
   *
   * @param storage    {@link HoodieStorage} instance.
   * @param filePath   the data file path.
   * @param columnList List of columns to get column statistics.
   * @return {@link List} of {@link HoodieColumnRangeMetadata}.
   */
  @SuppressWarnings("rawtype")
  public abstract List<HoodieColumnRangeMetadata<Comparable>> readColumnStatsFromMetadata(HoodieStorage storage,
                                                                                          StoragePath filePath,
                                                                                          List<String> columnList);

  /**
   * @return The subclass's {@link HoodieFileFormat}.
   */
  public abstract HoodieFileFormat getFormat();

  /**
   * Writes properties to the meta file.
   *
   * @param storage  {@link HoodieStorage} instance.
   * @param filePath file path to write to.
   * @param props    properties to write.
   * @throws IOException upon write error.
   */
  public abstract void writeMetaFile(HoodieStorage storage,
                                     StoragePath filePath,
                                     Properties props) throws IOException;

  /**
   * Serializes Hudi records to the log block.
   *
   * @param storage      {@link HoodieStorage} instance.
   * @param records      a list of {@link HoodieRecord}.
   * @param writerSchema writer schema string from the log block header.
   * @param readerSchema
   * @param keyFieldName
   * @param paramsMap    additional params for serialization.
   * @return byte array after serialization.
   * @throws IOException upon serialization error.
   */
  public abstract byte[] serializeRecordsToLogBlock(HoodieStorage storage,
                                                    List<HoodieRecord> records,
                                                    Schema writerSchema,
                                                    Schema readerSchema, String keyFieldName,
                                                    Map<String, String> paramsMap) throws IOException;
}
