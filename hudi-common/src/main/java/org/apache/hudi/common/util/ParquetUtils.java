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

import org.apache.hudi.avro.HoodieAvroWriteSupport;
import org.apache.hudi.common.BloomFilter;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.MetadataNotFoundException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Utility functions involving with parquet.
 */
public class ParquetUtils {

  /**
   * Read the rowKey list from the given parquet file.
   *
   * @param filePath The parquet file path.
   * @param configuration configuration to build fs object
   * @return Set Set of row keys
   */
  public static Set<String> readRowKeysFromParquet(Configuration configuration, Path filePath) {
    return filterParquetRowKeys(configuration, filePath, new HashSet<>());
  }

  /**
   * Read the rowKey list matching the given filter, from the given parquet file. If the filter is empty, then this will
   * return all the rowkeys.
   *
   * @param filePath The parquet file path.
   * @param configuration configuration to build fs object
   * @param filter record keys filter
   * @return Set Set of row keys matching candidateRecordKeys
   */
  public static Set<String> filterParquetRowKeys(Configuration configuration, Path filePath, Set<String> filter) {
    Option<RecordKeysFilterFunction> filterFunction = Option.empty();
    if (filter != null && !filter.isEmpty()) {
      filterFunction = Option.of(new RecordKeysFilterFunction(filter));
    }
    Configuration conf = new Configuration(configuration);
    conf.addResource(FSUtils.getFs(filePath.toString(), conf).getConf());
    Schema readSchema = HoodieAvroUtils.getRecordKeySchema();
    AvroReadSupport.setAvroReadSchema(conf, readSchema);
    AvroReadSupport.setRequestedProjection(conf, readSchema);
    Set<String> rowKeys = new HashSet<>();
    try (ParquetReader reader = AvroParquetReader.builder(filePath).withConf(conf).build()) {
      Object obj = reader.read();
      while (obj != null) {
        if (obj instanceof GenericRecord) {
          String recordKey = ((GenericRecord) obj).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
          if (!filterFunction.isPresent() || filterFunction.get().apply(recordKey)) {
            rowKeys.add(recordKey);
          }
        }
        obj = reader.read();
      }
    } catch (IOException e) {
      throw new HoodieIOException("Failed to read row keys from Parquet " + filePath, e);

    }
    // ignore
    return rowKeys;
  }

  public static ParquetMetadata readMetadata(Configuration conf, Path parquetFilePath) {
    ParquetMetadata footer;
    try {
      // TODO(vc): Should we use the parallel reading version here?
      footer = ParquetFileReader.readFooter(FSUtils.getFs(parquetFilePath.toString(), conf).getConf(), parquetFilePath);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to read footer for parquet " + parquetFilePath, e);
    }
    return footer;
  }

  /**
   * Get the schema of the given parquet file.
   */
  public static MessageType readSchema(Configuration configuration, Path parquetFilePath) {
    return readMetadata(configuration, parquetFilePath).getFileMetaData().getSchema();
  }

  private static Map<String, String> readParquetFooter(Configuration configuration, boolean required,
      Path parquetFilePath, String... footerNames) {
    Map<String, String> footerVals = new HashMap<>();
    ParquetMetadata footer = readMetadata(configuration, parquetFilePath);
    Map<String, String> metadata = footer.getFileMetaData().getKeyValueMetaData();
    for (String footerName : footerNames) {
      if (metadata.containsKey(footerName)) {
        footerVals.put(footerName, metadata.get(footerName));
      } else if (required) {
        throw new MetadataNotFoundException(
            "Could not find index in Parquet footer. " + "Looked for key " + footerName + " in " + parquetFilePath);
      }
    }
    return footerVals;
  }

  public static Schema readAvroSchema(Configuration configuration, Path parquetFilePath) {
    return new AvroSchemaConverter().convert(readSchema(configuration, parquetFilePath));
  }

  /**
   * Read out the bloom filter from the parquet file meta data.
   */
  public static BloomFilter readBloomFilterFromParquetMetadata(Configuration configuration, Path parquetFilePath) {
    Map<String, String> footerVals = readParquetFooter(configuration, false, parquetFilePath,
        HoodieAvroWriteSupport.HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY,
        HoodieAvroWriteSupport.OLD_HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY);
    String footerVal = footerVals.get(HoodieAvroWriteSupport.HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY);
    if (null == footerVal) {
      // We use old style key "com.uber.hoodie.bloomfilter"
      footerVal = footerVals.get(HoodieAvroWriteSupport.OLD_HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY);
    }
    return footerVal != null ? new BloomFilter(footerVal) : null;
  }

  public static String[] readMinMaxRecordKeys(Configuration configuration, Path parquetFilePath) {
    Map<String, String> minMaxKeys = readParquetFooter(configuration, true, parquetFilePath,
        HoodieAvroWriteSupport.HOODIE_MIN_RECORD_KEY_FOOTER, HoodieAvroWriteSupport.HOODIE_MAX_RECORD_KEY_FOOTER);
    if (minMaxKeys.size() != 2) {
      throw new HoodieException(
          String.format("Could not read min/max record key out of footer correctly from %s. read) : %s",
              parquetFilePath, minMaxKeys));
    }
    return new String[] {minMaxKeys.get(HoodieAvroWriteSupport.HOODIE_MIN_RECORD_KEY_FOOTER),
        minMaxKeys.get(HoodieAvroWriteSupport.HOODIE_MAX_RECORD_KEY_FOOTER)};
  }

  /**
   * NOTE: This literally reads the entire file contents, thus should be used with caution.
   */
  public static List<GenericRecord> readAvroRecords(Configuration configuration, Path filePath) {
    ParquetReader reader = null;
    List<GenericRecord> records = new ArrayList<>();
    try {
      reader = AvroParquetReader.builder(filePath).withConf(configuration).build();
      Object obj = reader.read();
      while (obj != null) {
        if (obj instanceof GenericRecord) {
          records.add(((GenericRecord) obj));
        }
        obj = reader.read();
      }
    } catch (IOException e) {
      throw new HoodieIOException("Failed to read avro records from Parquet " + filePath, e);

    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e) {
          // ignore
        }
      }
    }
    return records;
  }

  static class RecordKeysFilterFunction implements Function<String, Boolean> {
    private final Set<String> candidateKeys;

    RecordKeysFilterFunction(Set<String> candidateKeys) {
      this.candidateKeys = candidateKeys;
    }

    @Override
    public Boolean apply(String recordKey) {
      return candidateKeys.contains(recordKey);
    }
  }
}
