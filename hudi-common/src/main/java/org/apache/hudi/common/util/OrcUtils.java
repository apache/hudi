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

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.storage.ql.exec.vector.BytesColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;
import org.apache.hudi.avro.HoodieAvroWriteSupport;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.bloom.BloomFilterTypeCode;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.MetadataNotFoundException;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto.UserMetadataItem;
import org.apache.orc.Reader;
import org.apache.orc.Reader.Options;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;

/**
 * Utility functions for ORC files.
 */
public class OrcUtils {
  /**
   * Read the rowKey list matching the given filter, from the given ORC file. If the filter is empty, then this will
   * return all the rowkeys.
   *
   * @param conf configuration to build fs object.
   * @param filePath      The ORC file path.
   * @param filter        record keys filter
   * @return Set Set of row keys matching candidateRecordKeys
   */
  public static Set<String> filterOrcRowKeys(Configuration conf,
      Path filePath, Set<String> filter) throws HoodieIOException {
    try {
      Reader reader = OrcFile.createReader(filePath, OrcFile.readerOptions(conf));
      Set<String> filteredRowKeys = new HashSet<>();
      TypeDescription schema = reader.getSchema();
      List<String> fieldNames = schema.getFieldNames();
      VectorizedRowBatch batch = schema.createRowBatch();
      RecordReader recordReader = reader.rows(new Options(conf).schema(schema));

      // column index for the RECORD_KEY_METADATA_FIELD field
      int colIndex = -1;
      for (int i = 0; i < fieldNames.size(); i++) {
        if (fieldNames.get(i).equals(HoodieRecord.RECORD_KEY_METADATA_FIELD)) {
          colIndex = i;
          break;
        }
      }
      if (colIndex == -1) {
        throw new HoodieException(String.format("Couldn't find row keys in %s.", filePath));
      }
      while (recordReader.nextBatch(batch)) {
        BytesColumnVector rowKeys = (BytesColumnVector) batch.cols[colIndex];
        for (int i = 0; i < batch.size; i++) {
          String rowKey = rowKeys.toString(i);
          if (filter.isEmpty() || filter.contains(rowKey)) {
            filteredRowKeys.add(rowKey);
          }
        }
      }
      return filteredRowKeys;
    } catch (IOException io) {
      throw new HoodieIOException("Unable to read next batch from ORC file ", io);
    }
  }

  /**
   * Read out the bloom filter from the ORC file meta data.
   */
  public static BloomFilter readBloomFilterFromOrcMetadata(Configuration conf, Path orcFilePath) {
    Map<String, String> footerVals =
        readOrcFooter(conf, orcFilePath, false,
            HoodieAvroWriteSupport.HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY,
            HoodieAvroWriteSupport.HOODIE_BLOOM_FILTER_TYPE_CODE);
    String footerVal = footerVals.get(HoodieAvroWriteSupport.HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY);
    BloomFilter toReturn = null;
    if (footerVal != null) {
      if (footerVals.containsKey(HoodieAvroWriteSupport.HOODIE_BLOOM_FILTER_TYPE_CODE)) {
        toReturn = BloomFilterFactory.fromString(footerVal,
            footerVals.get(HoodieAvroWriteSupport.HOODIE_BLOOM_FILTER_TYPE_CODE));
      } else {
        toReturn = BloomFilterFactory.fromString(footerVal, BloomFilterTypeCode.SIMPLE.name());
      }
    }
    return toReturn;
  }

  public static String[] readMinMaxRecordKeys(Configuration conf, Path orcFilePath) {
    Map<String, String> minMaxKeys = readOrcFooter(conf, orcFilePath, true,
        HoodieAvroWriteSupport.HOODIE_MIN_RECORD_KEY_FOOTER, HoodieAvroWriteSupport.HOODIE_MAX_RECORD_KEY_FOOTER);
    if (minMaxKeys.size() != 2) {
      throw new HoodieException(
          String.format("Could not read min/max record key out of footer correctly from %s. read) : %s",
              orcFilePath, minMaxKeys));
    }
    return new String[] {minMaxKeys.get(HoodieAvroWriteSupport.HOODIE_MIN_RECORD_KEY_FOOTER),
        minMaxKeys.get(HoodieAvroWriteSupport.HOODIE_MAX_RECORD_KEY_FOOTER)};
  }

  private static Map<String, String> readOrcFooter(Configuration conf, Path orcFilePath,
                                                   boolean required, String... footerNames) {
    try {
      Reader reader = OrcFile.createReader(orcFilePath, OrcFile.readerOptions(conf));
      Map<String, String> footerVals = new HashMap<>();
      List<UserMetadataItem> metadataItemList = reader.getFileTail().getFooter().getMetadataList();
      Map<String, String> metadata = metadataItemList.stream().collect(Collectors.toMap(
          UserMetadataItem::getName,
          metadataItem -> metadataItem.getValue().toStringUtf8()));
      for (String footerName : footerNames) {
        if (metadata.containsKey(footerName)) {
          footerVals.put(footerName, metadata.get(footerName));
        } else if (required) {
          throw new MetadataNotFoundException(
              "Could not find index in ORC footer. Looked for key " + footerName + " in "
                  + orcFilePath);
        }
      }
      return footerVals;
    } catch (IOException io) {
      throw new HoodieIOException("Unable to create an ORC reader.", io);
    }
  }

  public static Schema getAvroSchema(Configuration conf, Path orcFilePath) {
    try {
      Reader reader = OrcFile.createReader(orcFilePath, OrcFile.readerOptions(conf));
      TypeDescription orcSchema = reader.getSchema();
      return AvroOrcUtils.createAvroSchema(orcSchema);
    } catch (IOException io) {
      throw new HoodieIOException("Unable to create an ORC reader.", io);
    }
  }

  public static long getRowCount(Configuration conf, Path orcFilePath) {
    try {
      Reader reader = OrcFile.createReader(orcFilePath, OrcFile.readerOptions(conf));
      return reader.getNumberOfRows();
    } catch (IOException io) {
      throw new HoodieIOException("Unable to create an ORC reader.", io);
    }
  }
}
