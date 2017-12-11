/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.common.util;

import static com.uber.hoodie.common.util.FSUtils.getFs;

import com.uber.hoodie.avro.HoodieAvroWriteSupport;
import com.uber.hoodie.common.BloomFilter;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.exception.HoodieException;
import com.uber.hoodie.exception.HoodieIOException;
import com.uber.hoodie.exception.MetadataNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

/**
 * Utility functions involving with parquet.
 */
public class ParquetUtils {

  /**
   * Read the rowKey list from the given parquet file.
   *
   * @param filePath The parquet file path.
   * @param configuration configuration to build fs object
   */
  public static Set<String> readRowKeysFromParquet(Configuration configuration, Path filePath) {
    Configuration conf = new Configuration(configuration);
    conf.addResource(getFs(filePath.toString(), conf).getConf());
    Schema readSchema = HoodieAvroUtils.getRecordKeySchema();
    AvroReadSupport.setAvroReadSchema(conf, readSchema);
    AvroReadSupport.setRequestedProjection(conf, readSchema);
    ParquetReader reader = null;
    Set<String> rowKeys = new HashSet<>();
    try {
      reader = AvroParquetReader.builder(filePath).withConf(conf).build();
      Object obj = reader.read();
      while (obj != null) {
        if (obj instanceof GenericRecord) {
          rowKeys.add(((GenericRecord) obj).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString());
        }
        obj = reader.read();
      }
    } catch (IOException e) {
      throw new HoodieIOException("Failed to read row keys from Parquet " + filePath, e);

    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e) {
          // ignore
        }
      }
    }
    return rowKeys;
  }

  public static ParquetMetadata readMetadata(Configuration conf, Path parquetFilePath) {
    ParquetMetadata footer;
    try {
      // TODO(vc): Should we use the parallel reading version here?
      footer = ParquetFileReader
          .readFooter(getFs(parquetFilePath.toString(), conf).getConf(), parquetFilePath);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to read footer for parquet " + parquetFilePath,
          e);
    }
    return footer;
  }


  /**
   * Get the schema of the given parquet file.
   */
  public static MessageType readSchema(Configuration configuration, Path parquetFilePath) {
    return readMetadata(configuration, parquetFilePath).getFileMetaData().getSchema();
  }


  private static List<String> readParquetFooter(Configuration configuration, Path parquetFilePath,
      String... footerNames) {
    List<String> footerVals = new ArrayList<>();
    ParquetMetadata footer = readMetadata(configuration, parquetFilePath);
    Map<String, String> metadata = footer.getFileMetaData().getKeyValueMetaData();
    for (String footerName : footerNames) {
      if (metadata.containsKey(footerName)) {
        footerVals.add(metadata.get(footerName));
      } else {
        throw new MetadataNotFoundException("Could not find index in Parquet footer. " +
            "Looked for key " + footerName + " in " + parquetFilePath);
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
  public static BloomFilter readBloomFilterFromParquetMetadata(Configuration configuration,
      Path parquetFilePath) {
    String footerVal = readParquetFooter(configuration, parquetFilePath,
        HoodieAvroWriteSupport.HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY).get(0);
    return new BloomFilter(footerVal);
  }

  public static String[] readMinMaxRecordKeys(Configuration configuration, Path parquetFilePath) {
    List<String> minMaxKeys = readParquetFooter(configuration, parquetFilePath,
        HoodieAvroWriteSupport.HOODIE_MIN_RECORD_KEY_FOOTER,
        HoodieAvroWriteSupport.HOODIE_MAX_RECORD_KEY_FOOTER);
    if (minMaxKeys.size() != 2) {
      throw new HoodieException(String.format(
          "Could not read min/max record key out of footer correctly from %s. read) : %s",
          parquetFilePath, minMaxKeys));
    }
    return new String[]{minMaxKeys.get(0), minMaxKeys.get(1)};
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
}
