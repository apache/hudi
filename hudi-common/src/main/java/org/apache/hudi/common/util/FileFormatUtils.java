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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.HoodieBloomFilterWriteSupport;
import org.apache.hudi.avro.ValueMetadata;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.bloom.BloomFilterTypeCode;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.metadata.HoodieIndexVersion;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import javax.annotation.Nonnull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Utils for file format used in Hudi.
 */
public abstract class FileFormatUtils {
  /**
   * Aggregate column range statistics across files in a partition.
   *
   * @param relativePartitionPath relative partition path for the column range stats
   * @param fileColumnRanges List of column range statistics for each file in a partition
   */
  public static <T extends Comparable<T>> HoodieColumnRangeMetadata<T> getColumnRangeInPartition(String relativePartitionPath,
                                                                                                 String columnName,
                                                                                                 @Nonnull List<HoodieColumnRangeMetadata<T>> fileColumnRanges,
                                                                                                 Map<String, Schema> colsToIndexSchemaMap,
                                                                                                 HoodieIndexVersion indexVersion) {

    ValidationUtils.checkArgument(!fileColumnRanges.isEmpty(), "fileColumnRanges should not be empty.");
    if (indexVersion.greaterThanOrEquals(HoodieIndexVersion.V2)) {
      ValueMetadata valueMetadata = ValueMetadata.getValueMetadata(colsToIndexSchemaMap.get(columnName), indexVersion);
      return fileColumnRanges.stream()
          .map(e -> {
            T minValue = (T) valueMetadata.standardizeJavaTypeAndPromote(e.getMinValue());
            T maxValue = (T) valueMetadata.standardizeJavaTypeAndPromote(e.getMaxValue());
            return HoodieColumnRangeMetadata.create(
                relativePartitionPath, e.getColumnName(), minValue, maxValue, e.getNullCount(), e.getValueCount(), e.getTotalSize(),
                e.getTotalUncompressedSize(), valueMetadata);
          }).reduce(HoodieColumnRangeMetadata::merge).orElseThrow(() -> new HoodieException("MergingColumnRanges failed."));
    }
    // we are reducing using merge so IDK why we think there are multiple cols that need to go through schema evolution
    // Let's do one pass and deduce all columns that needs to go through schema evolution.
    Map<String, Set<Class<?>>> schemaSeenForColsToIndex = new HashMap<>();
    Set<String> colsWithSchemaEvolved = new HashSet<>();
    fileColumnRanges.stream().forEach(entry -> {
      String colToIndex = entry.getColumnName();
      Class<?> minValueClass = entry.getMinValue() != null ? entry.getMinValue().getClass() : null;
      Class<?> maxValueClass = entry.getMaxValue() != null ? entry.getMaxValue().getClass() : null;
      schemaSeenForColsToIndex.computeIfAbsent(colToIndex, s -> new HashSet());
      if (minValueClass != null) {
        schemaSeenForColsToIndex.get(colToIndex).add(minValueClass);
      }
      if (maxValueClass != null) {
        schemaSeenForColsToIndex.get(colToIndex).add(maxValueClass);
      }
      if (!colsToIndexSchemaMap.isEmpty() && schemaSeenForColsToIndex.get(colToIndex).size() > 1) {
        colsWithSchemaEvolved.add(colToIndex);
      }
    });

    // There are multiple files. Compute min(file_mins) and max(file_maxs)
    return fileColumnRanges.stream()
        .map(e -> HoodieColumnRangeMetadata.create(
            relativePartitionPath, e.getColumnName(), e.getMinValue(), e.getMaxValue(),
            e.getNullCount(), e.getValueCount(), e.getTotalSize(), e.getTotalUncompressedSize(),
            e.getValueMetadata()))
        .reduce((a,b) -> {
          if (colsWithSchemaEvolved.isEmpty() || colsToIndexSchemaMap.isEmpty()
              || a.getMinValue() == null || a.getMaxValue() == null || b.getMinValue() == null || b.getMaxValue() == null
              || !colsWithSchemaEvolved.contains(a.getColumnName())) {
            return HoodieColumnRangeMetadata.merge(a, b);
          } else {
            // schema is evolving for the column of interest.
            Schema schema = colsToIndexSchemaMap.get(a.getColumnName());
            HoodieColumnRangeMetadata<T> left = HoodieColumnRangeMetadata.create(a.getFilePath(), a.getColumnName(),
                (T) HoodieTableMetadataUtil.coerceToComparable(schema, a.getMinValue()),
                (T) HoodieTableMetadataUtil.coerceToComparable(schema, a.getMaxValue()), a.getNullCount(),
                a.getValueCount(), a.getTotalSize(), a.getTotalUncompressedSize(), a.getValueMetadata());
            HoodieColumnRangeMetadata<T> right = HoodieColumnRangeMetadata.create(b.getFilePath(), b.getColumnName(),
                (T) HoodieTableMetadataUtil.coerceToComparable(schema, b.getMinValue()),
                (T) HoodieTableMetadataUtil.coerceToComparable(schema, b.getMaxValue()), b.getNullCount(),
                b.getValueCount(), b.getTotalSize(), b.getTotalUncompressedSize(), b.getValueMetadata());
            return HoodieColumnRangeMetadata.merge(left, right);
          }
        }).orElseThrow(() -> new HoodieException("MergingColumnRanges failed."));
  }

  /**
   * Read the rowKey list from the given data file.
   *
   * @param storage  {@link HoodieStorage} instance.
   * @param filePath the data file path.
   * @return set of row keys
   */
  public Set<String> readRowKeys(HoodieStorage storage, StoragePath filePath) {
    return filterRowKeys(storage, filePath, new HashSet<>())
        .stream().map(Pair::getKey).collect(Collectors.toSet());
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
   * If the filter is empty, then this will return all the row keys and corresponding positions.
   *
   * @param storage  {@link HoodieStorage} instance.
   * @param filePath the data file path.
   * @param filter   record keys filter.
   * @return set of pairs of row key and position matching candidateRecordKeys.
   */
  public abstract Set<Pair<String, Long>> filterRowKeys(HoodieStorage storage, StoragePath filePath, Set<String> filter);

  /**
   * Fetch {@link HoodieKey}s with positions from the given data file.
   *
   * @param storage  {@link HoodieStorage} instance.
   * @param filePath the data file path.
   * @return {@link List} of pairs of {@link HoodieKey} and position fetched from the data file.
   */
  public abstract ClosableIterator<Pair<HoodieKey, Long>> fetchRecordKeysWithPositions(HoodieStorage storage, StoragePath filePath);

  /**
   * Provides a closable iterator for reading the given data file.
   *
   * @param storage         {@link HoodieStorage} instance.
   * @param filePath        the data file path.
   * @param keyGeneratorOpt instance of KeyGenerator.
   * @param partitionPath optional partition path for the file, if provided only the record key is read from the file
   * @return {@link ClosableIterator} of {@link HoodieKey}s for reading the file.
   */
  public abstract ClosableIterator<HoodieKey> getHoodieKeyIterator(HoodieStorage storage,
                                                                   StoragePath filePath,
                                                                   Option<BaseKeyGenerator> keyGeneratorOpt,
                                                                   Option<String> partitionPath);

  /**
   * Provides a closable iterator for reading the given data file.
   *
   * @param storage  {@link HoodieStorage} instance.
   * @param filePath the data file path.
   * @return {@link ClosableIterator} of {@link HoodieKey}s for reading the file.
   */
  public abstract ClosableIterator<HoodieKey> getHoodieKeyIterator(HoodieStorage storage, StoragePath filePath);

  protected Schema getKeyIteratorSchema(HoodieStorage storage, StoragePath filePath, Option<BaseKeyGenerator> keyGeneratorOpt, Option<String> partitionPath) {
    return keyGeneratorOpt
        .map(keyGenerator -> {
          List<String> fields = new ArrayList<>();
          fields.addAll(keyGenerator.getRecordKeyFieldNames());
          fields.addAll(keyGenerator.getPartitionPathFields());
          return HoodieAvroUtils.projectSchema(readAvroSchema(storage, filePath), fields);
        })
        .orElse(partitionPath.isPresent() ? HoodieAvroUtils.getRecordKeySchema() : HoodieAvroUtils.getRecordKeyPartitionPathSchema());
  }

  /**
   * Fetch {@link HoodieKey}s with positions from the given data file.
   *
   * @param storage         {@link HoodieStorage} instance.
   * @param filePath        the data file path.
   * @param keyGeneratorOpt instance of KeyGenerator.
   * @param partitionPath optional partition path for the file, if provided only the record key is read from the file
   * @return {@link Iterator} of pairs of {@link HoodieKey} and position fetched from the data file.
   */
  public abstract ClosableIterator<Pair<HoodieKey, Long>> fetchRecordKeysWithPositions(HoodieStorage storage,
                                                                                       StoragePath filePath,
                                                                                       Option<BaseKeyGenerator> keyGeneratorOpt,
                                                                                       Option<String> partitionPath);

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
                                                                                          List<String> columnList,
                                                                                          HoodieIndexVersion indexVersion);

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
  public abstract ByteArrayOutputStream serializeRecordsToLogBlock(HoodieStorage storage,
                                                                   List<HoodieRecord> records,
                                                                   Schema writerSchema,
                                                                   Schema readerSchema, String keyFieldName,
                                                                   Map<String, String> paramsMap) throws IOException;

  /**
   * Serializes Hudi records to the log block and collect column range metadata.
   *
   * @param storage      {@link HoodieStorage} instance.
   * @param records      a list of {@link HoodieRecord}.
   * @param writerSchema writer schema string from the log block header.
   * @param readerSchema schema of reader.
   * @param keyFieldName name of key field.
   * @param paramsMap    additional params for serialization.
   * @return pair of byte array after serialization and format metadata.
   * @throws IOException upon serialization error.
   */
  public abstract Pair<ByteArrayOutputStream, Object> serializeRecordsToLogBlock(HoodieStorage storage,
                                                                                 Iterator<HoodieRecord> records,
                                                                                 HoodieRecord.HoodieRecordType recordType,
                                                                                 Schema writerSchema,
                                                                                 Schema readerSchema,
                                                                                 String keyFieldName,
                                                                                 Map<String, String> paramsMap) throws IOException;

  // -------------------------------------------------------------------------
  //  Inner Class
  // -------------------------------------------------------------------------

  /**
   * An iterator that can apply the given function {@code func} to transform records
   * from the underneath record iterator to hoodie keys.
   */
  public static class HoodieKeyIterator implements ClosableIterator<HoodieKey> {
    private final ClosableIterator<GenericRecord> nestedItr;
    private final Function<GenericRecord, HoodieKey> func;

    public static HoodieKeyIterator getInstance(ClosableIterator<GenericRecord> nestedItr, Option<BaseKeyGenerator> keyGenerator, Option<String> partitionPathOption) {
      return new HoodieKeyIterator(nestedItr, keyGenerator, partitionPathOption);
    }

    private HoodieKeyIterator(ClosableIterator<GenericRecord> nestedItr, Option<BaseKeyGenerator> keyGenerator, Option<String> partitionPathOption) {
      this.nestedItr = nestedItr;
      if (keyGenerator.isPresent()) {
        this.func = retVal -> {
          String recordKey = keyGenerator.get().getRecordKey(retVal);
          String partitionPath = partitionPathOption.orElseGet(() -> keyGenerator.get().getPartitionPath(retVal));
          return new HoodieKey(recordKey, partitionPath);
        };
      } else {
        this.func = retVal -> {
          String recordKey = retVal.get(0).toString();
          String partitionPath = partitionPathOption.orElseGet(() -> retVal.get(1).toString());
          return new HoodieKey(recordKey, partitionPath);
        };
      }
    }

    @Override
    public void close() {
      if (this.nestedItr != null) {
        this.nestedItr.close();
      }
    }

    @Override
    public boolean hasNext() {
      return this.nestedItr.hasNext();
    }

    @Override
    public HoodieKey next() {
      return this.func.apply(this.nestedItr.next());
    }
  }
}
