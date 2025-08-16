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

package org.apache.hudi.parquet.io;

import org.apache.hudi.exception.HoodieIOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hudi.avro.HoodieBloomFilterWriteSupport.HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY;
import static org.apache.hudi.avro.HoodieBloomFilterWriteSupport.HOODIE_BLOOM_FILTER_TYPE_CODE;
import static org.apache.hudi.common.bloom.BloomFilterTypeCode.SIMPLE;

public class ParquetBinaryCopyChecker {

  private ParquetBinaryCopyChecker() {

  }

  /**
   * Verify whether a set of files meet the conditions for binary stream copying
   *  1. All input parquet file schema support binary copy
   *  2. This set of files contains only one type of BloomFilterTypeCode, including null
   *  3. The same column across these files has only one repetition type
   * @param files
   * @return
   */
  public static boolean verifyFiles(List<ParquetFileInfo> files) {
    boolean schemaSupportBinaryCopy = files.stream().allMatch(ParquetFileInfo::canBinaryCopy);
    if (!schemaSupportBinaryCopy) {
      return false;
    }

    boolean hasSameFilterCodeType = files.stream()
        .map(ParquetFileInfo::getBloomFilterTypeCode)
        .distinct()
        .count() <= 1;
    if (!hasSameFilterCodeType) {
      return false;
    }

    Map<String, Set<String>> fieldsMap = collectRepetitions(files);
    return fieldsMap.values().stream().allMatch(reps -> reps.size() == 1);
  }

  private static Map<String, Set<String>> collectRepetitions(List<ParquetFileInfo> files) {
    Map<String, Set<String>> fieldsMap = new HashMap<>();
    for (ParquetFileInfo file : files) {
      String schema = file.getSchema();
      MessageType messageType = MessageTypeParser.parseMessageType(schema);
      Map<String, String> map = collectRepetitions("", messageType);
      map.forEach((field, repetition) -> {
        Set<String> repetitions = fieldsMap.computeIfAbsent(field, f -> new HashSet<>());
        repetitions.add(repetition);
        fieldsMap.put(field, repetitions);
      });
    }
    return fieldsMap;
  }

  private static Map<String, String> collectRepetitions(String parentName, GroupType schema) {
    Map<String, String> fieldsMap = new HashMap<>();
    List<Type> fields = schema.getFields();
    for (Type type : fields) {
      if (type.isPrimitive()) {
        fieldsMap.put(parentName + "." + type.getName(), type.getRepetition().name());
      } else {
        fieldsMap.put(parentName + "." + type.getName(), type.getRepetition().name());
        GroupType groupType = type.asGroupType();
        if (groupType.getOriginalType() == OriginalType.LIST) {
          Type middleType = groupType.getType(0);
          Type elementType = middleType.asGroupType().getType(0);
          fieldsMap.put(parentName + ".list", middleType.getRepetition().name());
          fieldsMap.put(parentName + ".list.element", elementType.getRepetition().name());
          if (!elementType.isPrimitive()) {
            fieldsMap.putAll(collectRepetitions(parentName + ".list.element" + elementType.getName(), elementType.asGroupType()));
          }
        } else if (groupType.getOriginalType() == OriginalType.MAP) {
          Type keyValueType = groupType.getType(0);
          fieldsMap.put(parentName + ".key_value", keyValueType.getRepetition().name());
          Type keyType = keyValueType.asGroupType().getType(0);
          Type valueType = keyValueType.asGroupType().getType(1);
          fieldsMap.put(parentName + ".key_value.key", keyType.getRepetition().name());
          fieldsMap.put(parentName + ".key_value.value", valueType.getRepetition().name());
          if (!keyType.isPrimitive()) {
            fieldsMap.putAll(collectRepetitions(parentName + ".key_value.key" + keyType.getName(), keyType.asGroupType()));
          }
          if (!valueType.isPrimitive()) {
            fieldsMap.putAll(collectRepetitions(parentName + ".key_value.value" + valueType.getName(), valueType.asGroupType()));
          }
        } else {
          fieldsMap.putAll(collectRepetitions(parentName + "." + groupType.getName(), groupType));
        }
      }
    }
    return fieldsMap;
  }

  /**
   * Preliminary inspection of a single file
   *  1. verify file schema support binary copy or not
   *  2. get hoodie bloom filter type code
   *  3. get repetition of every field
   * @param conf
   * @param file
   * @return
   */
  public static ParquetFileInfo collectFileInfo(Configuration conf, String file) {
    Path path = new Path(file);
    ParquetMetadata footer = readMetadata(conf, path, ParquetMetadataConverter.SKIP_ROW_GROUPS);
    FileMetaData fileMetaData = footer.getFileMetaData();
    MessageType fileSchema = fileMetaData.getSchema();
    List<Type> fields = fileSchema.getFields();
    if (schemaNotSupportBinaryCopy(fields)) {
      return new ParquetFileInfo(false, null, null);
    }

    Map<String, String> keyValueMetaData = fileMetaData.getKeyValueMetaData();
    String bloomFileTypeCode = keyValueMetaData.get(HOODIE_BLOOM_FILTER_TYPE_CODE);
    if (bloomFileTypeCode == null && keyValueMetaData.get(HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY) != null) {
      bloomFileTypeCode = SIMPLE.name();
    }
    return new ParquetFileInfo(true, bloomFileTypeCode, fileSchema.toString());
  }

  /**
   * Copied from org.apache.hudi.common.util.ParquetUtils#readMetadata
   * I’m not sure why he didn’t directly use the input conf parameter but instead transformed it via
   *    FSUtils.getFs(parquetFilePath.toString(), conf).getConf()
   * so I kept this approach.
   * @param conf
   * @param parquetFilePath
   * @param filter
   * @return
   */
  private static ParquetMetadata readMetadata(
      Configuration conf,
      Path parquetFilePath,
      ParquetMetadataConverter.MetadataFilter filter) {
    ParquetMetadata footer;
    try {
      footer = ParquetFileReader.readFooter(conf, parquetFilePath, filter);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to read footer for parquet " + parquetFilePath, e);
    }
    return footer;
  }

  /**
   * Check whether input schema not supports binary copy
   * Following two case can not support
   *  1. two level List structure, because the result of parquet rewrite is three level List structure
   *  2. Decimal types stored via INT32/INT64/INT96, because it can not be read by parquet-avro
   *
   * @param parquetFields
   * @return
   */
  private static boolean schemaNotSupportBinaryCopy(List<Type> parquetFields) {
    for (Type type : parquetFields) {
      if (type.getOriginalType() == OriginalType.DECIMAL) {
        PrimitiveType primitiveType = type.asPrimitiveType();
        PrimitiveType.PrimitiveTypeName typeName = primitiveType.getPrimitiveTypeName();
        if (typeName == PrimitiveType.PrimitiveTypeName.INT32
            || typeName == PrimitiveType.PrimitiveTypeName.INT64
            || typeName == PrimitiveType.PrimitiveTypeName.INT96) {
          return true;
        }
      }
      if (!type.isPrimitive()) {
        GroupType groupType = type.asGroupType();
        OriginalType originalType = groupType.getOriginalType();
        if (originalType == OriginalType.LIST
            && groupType.getType(0).getName().equals("array")) {
          return true;
        }
        if (schemaNotSupportBinaryCopy(groupType.getFields())) {
          return true;
        }
      }
    }
    return false;
  }

  public static class ParquetFileInfo implements Serializable {
    private final boolean canBinaryCopy;
    private final String bloomFilterTypeCode;
    private final String schema;

    public ParquetFileInfo(boolean canBinaryCopy, String hoodieBloomFilterTypeCode, String schema) {
      this.canBinaryCopy = canBinaryCopy;
      this.bloomFilterTypeCode = hoodieBloomFilterTypeCode;
      this.schema = schema;
    }

    /**
     * Current file can use binary copy or not
     * Following two case can not support
     *  1. two level List structure, because the result of parquet rewrite is three level List structure
     *  2. Decimal types stored via INT32/INT64/INT96, because it can not be read by parquet-avro
     *
     * @param parquetFields
     * @return
     */
    public boolean canBinaryCopy() {
      return canBinaryCopy;
    }

    public String getBloomFilterTypeCode() {
      return bloomFilterTypeCode;
    }

    public String getSchema() {
      return schema;
    }
  }

}
