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
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.MetadataNotFoundException;
import org.apache.hudi.keygen.BaseKeyGenerator;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.DecimalMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utility functions involving with parquet.
 */
public class ParquetUtils extends BaseFileUtils {

  private static final Logger LOG = LogManager.getLogger(ParquetUtils.class);

  /**
   * Read the rowKey list matching the given filter, from the given parquet file. If the filter is empty, then this will
   * return all the rowkeys.
   *
   * @param filePath      The parquet file path.
   * @param configuration configuration to build fs object
   * @param filter        record keys filter
   * @return Set Set of row keys matching candidateRecordKeys
   */
  @Override
  public Set<String> filterRowKeys(Configuration configuration, Path filePath, Set<String> filter) {
    return filterParquetRowKeys(configuration, filePath, filter, HoodieAvroUtils.getRecordKeySchema());
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
   * Read the rowKey list matching the given filter, from the given parquet file. If the filter is empty, then this will
   * return all the rowkeys.
   *
   * @param filePath      The parquet file path.
   * @param configuration configuration to build fs object
   * @param filter        record keys filter
   * @param readSchema    schema of columns to be read
   * @return Set Set of row keys matching candidateRecordKeys
   */
  private static Set<String> filterParquetRowKeys(Configuration configuration, Path filePath, Set<String> filter,
                                                  Schema readSchema) {
    Option<RecordKeysFilterFunction> filterFunction = Option.empty();
    if (filter != null && !filter.isEmpty()) {
      filterFunction = Option.of(new RecordKeysFilterFunction(filter));
    }
    Configuration conf = new Configuration(configuration);
    conf.addResource(FSUtils.getFs(filePath.toString(), conf).getConf());
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

  /**
   * Fetch {@link HoodieKey}s from the given parquet file.
   *
   * @param filePath      The parquet file path.
   * @param configuration configuration to build fs object
   * @return {@link List} of {@link HoodieKey}s fetched from the parquet file
   */
  @Override
  public List<HoodieKey> fetchHoodieKeys(Configuration configuration, Path filePath) {
    return fetchHoodieKeys(configuration, filePath, Option.empty());
  }

  @Override
  public ClosableIterator<HoodieKey> getHoodieKeyIterator(Configuration configuration, Path filePath) {
    return getHoodieKeyIterator(configuration, filePath, Option.empty());
  }

  /**
   * Returns a closable iterator for reading the given parquet file.
   *
   * @param configuration configuration to build fs object
   * @param filePath      The parquet file path
   * @param keyGeneratorOpt instance of KeyGenerator
   *
   * @return {@link ClosableIterator} of {@link HoodieKey}s for reading the parquet file
   */
  @Override
  public ClosableIterator<HoodieKey> getHoodieKeyIterator(Configuration configuration, Path filePath, Option<BaseKeyGenerator> keyGeneratorOpt) {
    try {
      Configuration conf = new Configuration(configuration);
      conf.addResource(FSUtils.getFs(filePath.toString(), conf).getConf());
      Schema readSchema = keyGeneratorOpt.map(keyGenerator -> {
        List<String> fields = new ArrayList<>();
        fields.addAll(keyGenerator.getRecordKeyFieldNames());
        fields.addAll(keyGenerator.getPartitionPathFields());
        return HoodieAvroUtils.getSchemaForFields(readAvroSchema(conf, filePath), fields);
      })
          .orElse(HoodieAvroUtils.getRecordKeyPartitionPathSchema());
      AvroReadSupport.setAvroReadSchema(conf, readSchema);
      AvroReadSupport.setRequestedProjection(conf, readSchema);
      ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(filePath).withConf(conf).build();
      return HoodieKeyIterator.getInstance(new ParquetReaderIterator<>(reader), keyGeneratorOpt);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to read from Parquet file " + filePath, e);
    }
  }

  /**
   * Fetch {@link HoodieKey}s from the given parquet file.
   *
   * @param configuration   configuration to build fs object
   * @param filePath        The parquet file path.
   * @param keyGeneratorOpt instance of KeyGenerator.
   * @return {@link List} of {@link HoodieKey}s fetched from the parquet file
   */
  @Override
  public List<HoodieKey> fetchHoodieKeys(Configuration configuration, Path filePath, Option<BaseKeyGenerator> keyGeneratorOpt) {
    List<HoodieKey> hoodieKeys = new ArrayList<>();
    try (ClosableIterator<HoodieKey> iterator = getHoodieKeyIterator(configuration, filePath, keyGeneratorOpt)) {
      iterator.forEachRemaining(hoodieKeys::add);
      return hoodieKeys;
    }
  }

  /**
   * Get the schema of the given parquet file.
   */
  public MessageType readSchema(Configuration configuration, Path parquetFilePath) {
    return readMetadata(configuration, parquetFilePath).getFileMetaData().getSchema();
  }

  @Override
  public Map<String, String> readFooter(Configuration configuration, boolean required,
                                                       Path parquetFilePath, String... footerNames) {
    Map<String, String> footerVals = new HashMap<>();
    ParquetMetadata footer = readMetadata(configuration, parquetFilePath);
    Map<String, String> metadata = footer.getFileMetaData().getKeyValueMetaData();
    for (String footerName : footerNames) {
      if (metadata.containsKey(footerName)) {
        footerVals.put(footerName, metadata.get(footerName));
      } else if (required) {
        throw new MetadataNotFoundException(
            "Could not find index in Parquet footer. Looked for key " + footerName + " in " + parquetFilePath);
      }
    }
    return footerVals;
  }

  @Override
  public Schema readAvroSchema(Configuration configuration, Path parquetFilePath) {
    MessageType parquetSchema = readSchema(configuration, parquetFilePath);
    return new AvroSchemaConverter(configuration).convert(parquetSchema);
  }

  @Override
  public HoodieFileFormat getFormat() {
    return HoodieFileFormat.PARQUET;
  }

  /**
   * NOTE: This literally reads the entire file contents, thus should be used with caution.
   */
  @Override
  public List<GenericRecord> readAvroRecords(Configuration configuration, Path filePath) {
    List<GenericRecord> records = new ArrayList<>();
    try (ParquetReader reader = AvroParquetReader.builder(filePath).withConf(configuration).build()) {
      Object obj = reader.read();
      while (obj != null) {
        if (obj instanceof GenericRecord) {
          records.add(((GenericRecord) obj));
        }
        obj = reader.read();
      }
    } catch (IOException e) {
      throw new HoodieIOException("Failed to read avro records from Parquet " + filePath, e);

    }
    return records;
  }

  @Override
  public List<GenericRecord> readAvroRecords(Configuration configuration, Path filePath, Schema schema) {
    AvroReadSupport.setAvroReadSchema(configuration, schema);
    return readAvroRecords(configuration, filePath);
  }

  /**
   * Returns the number of records in the parquet file.
   *
   * @param conf            Configuration
   * @param parquetFilePath path of the file
   */
  @Override
  public long getRowCount(Configuration conf, Path parquetFilePath) {
    ParquetMetadata footer;
    long rowCount = 0;
    footer = readMetadata(conf, parquetFilePath);
    for (BlockMetaData b : footer.getBlocks()) {
      rowCount += b.getRowCount();
    }
    return rowCount;
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

  /**
   * Parse min/max statistics stored in parquet footers for all columns.
   */
  @SuppressWarnings("rawtype")
  public List<HoodieColumnRangeMetadata<Comparable>> readRangeFromParquetMetadata(
      @Nonnull Configuration conf,
      @Nonnull Path parquetFilePath,
      @Nonnull List<String> cols
  ) {
    ParquetMetadata metadata = readMetadata(conf, parquetFilePath);

    // NOTE: This collector has to have fully specialized generic type params since
    //       Java 1.8 struggles to infer them
    Collector<HoodieColumnRangeMetadata<Comparable>, ?, Map<String, List<HoodieColumnRangeMetadata<Comparable>>>> groupingByCollector =
        Collectors.groupingBy(HoodieColumnRangeMetadata::getColumnName);

    // Collect stats from all individual Parquet blocks
    Map<String, List<HoodieColumnRangeMetadata<Comparable>>> columnToStatsListMap =
        (Map<String, List<HoodieColumnRangeMetadata<Comparable>>>) metadata.getBlocks().stream().sequential()
          .flatMap(blockMetaData ->
              blockMetaData.getColumns().stream()
                .filter(f -> cols.contains(f.getPath().toDotString()))
                .map(columnChunkMetaData ->
                    HoodieColumnRangeMetadata.<Comparable>create(
                        parquetFilePath.getName(),
                        columnChunkMetaData.getPath().toDotString(),
                        convertToNativeJavaType(
                            columnChunkMetaData.getPrimitiveType(),
                            columnChunkMetaData.getStatistics().genericGetMin()),
                        convertToNativeJavaType(
                            columnChunkMetaData.getPrimitiveType(),
                            columnChunkMetaData.getStatistics().genericGetMax()),
                        columnChunkMetaData.getStatistics().getNumNulls(),
                        columnChunkMetaData.getValueCount(),
                        columnChunkMetaData.getTotalSize(),
                        columnChunkMetaData.getTotalUncompressedSize()))
          )
          .collect(groupingByCollector);

    // Combine those into file-level statistics
    // NOTE: Inlining this var makes javac (1.8) upset (due to its inability to infer
    // expression type correctly)
    Stream<HoodieColumnRangeMetadata<Comparable>> stream = columnToStatsListMap.values()
        .stream()
        .map(this::getColumnRangeInFile);

    return stream.collect(Collectors.toList());
  }

  private <T extends Comparable<T>> HoodieColumnRangeMetadata<T> getColumnRangeInFile(
      @Nonnull List<HoodieColumnRangeMetadata<T>> blockRanges
  ) {
    if (blockRanges.size() == 1) {
      // only one block in parquet file. we can just return that range.
      return blockRanges.get(0);
    }

    // there are multiple blocks. Compute min(block_mins) and max(block_maxs)
    return blockRanges.stream()
        .sequential()
        .reduce(this::combineRanges).get();
  }

  private <T extends Comparable<T>> HoodieColumnRangeMetadata<T> combineRanges(
      HoodieColumnRangeMetadata<T> one,
      HoodieColumnRangeMetadata<T> another
  ) {
    final T minValue;
    final T maxValue;
    if (one.getMinValue() != null && another.getMinValue() != null) {
      minValue = one.getMinValue().compareTo(another.getMinValue()) < 0 ? one.getMinValue() : another.getMinValue();
    } else if (one.getMinValue() == null) {
      minValue = another.getMinValue();
    } else {
      minValue = one.getMinValue();
    }

    if (one.getMaxValue() != null && another.getMaxValue() != null) {
      maxValue = one.getMaxValue().compareTo(another.getMaxValue()) < 0 ? another.getMaxValue() : one.getMaxValue();
    } else if (one.getMaxValue() == null) {
      maxValue = another.getMaxValue();
    } else {
      maxValue = one.getMaxValue();
    }

    return HoodieColumnRangeMetadata.create(
        one.getFilePath(),
        one.getColumnName(), minValue, maxValue,
        one.getNullCount() + another.getNullCount(),
        one.getValueCount() + another.getValueCount(),
        one.getTotalSize() + another.getTotalSize(),
        one.getTotalUncompressedSize() + another.getTotalUncompressedSize());
  }

  private static Comparable<?> convertToNativeJavaType(PrimitiveType primitiveType, Comparable<?> val) {
    if (val == null) {
      return null;
    }

    if (primitiveType.getOriginalType() == OriginalType.DECIMAL) {
      return extractDecimal(val, primitiveType.getDecimalMetadata());
    } else if (primitiveType.getOriginalType() == OriginalType.DATE) {
      // NOTE: This is a workaround to address race-condition in using
      //       {@code SimpleDataFormat} concurrently (w/in {@code DateStringifier})
      // TODO cleanup after Parquet upgrade to 1.12
      synchronized (primitiveType.stringifier()) {
        // Date logical type is implemented as a signed INT32
        // REF: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
        return java.sql.Date.valueOf(
            primitiveType.stringifier().stringify((Integer) val)
        );
      }
    } else if (primitiveType.getOriginalType() == OriginalType.UTF8) {
      // NOTE: UTF8 type designates a byte array that should be interpreted as a
      // UTF-8 encoded character string
      // REF: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
      return ((Binary) val).toStringUsingUTF8();
    } else if (primitiveType.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.BINARY) {
      // NOTE: `getBytes` access makes a copy of the underlying byte buffer
      return ((Binary) val).toByteBuffer();
    }

    return val;
  }

  @Nonnull
  private static BigDecimal extractDecimal(Object val, DecimalMetadata decimalMetadata) {
    // In Parquet, Decimal could be represented as either of
    //    1. INT32 (for 1 <= precision <= 9)
    //    2. INT64 (for 1 <= precision <= 18)
    //    3. FIXED_LEN_BYTE_ARRAY (precision is limited by the array size. Length n can store <= floor(log_10(2^(8*n - 1) - 1)) base-10 digits)
    //    4. BINARY (precision is not limited)
    // REF: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#DECIMAL
    int scale = decimalMetadata.getScale();
    if (val == null) {
      return null;
    } else if (val instanceof Integer) {
      return BigDecimal.valueOf((Integer) val, scale);
    } else if (val instanceof Long) {
      return BigDecimal.valueOf((Long) val, scale);
    } else if (val instanceof Binary) {
      // NOTE: Unscaled number is stored in BE format (most significant byte is 0th)
      return new BigDecimal(new BigInteger(((Binary) val).getBytesUnsafe()), scale);
    } else {
      throw new UnsupportedOperationException(String.format("Unsupported value type (%s)", val.getClass().getName()));
    }
  }

  // -------------------------------------------------------------------------
  //  Inner Class
  // -------------------------------------------------------------------------

  /**
   * An iterator that can apply the given function {@code func} to transform records
   * from the underneath record iterator to hoodie keys.
   */
  private static class HoodieKeyIterator implements ClosableIterator<HoodieKey> {
    private final ClosableIterator<GenericRecord> nestedItr;
    private final Function<GenericRecord, HoodieKey> func;

    public static HoodieKeyIterator getInstance(ClosableIterator<GenericRecord> nestedItr, Option<BaseKeyGenerator> keyGenerator) {
      return new HoodieKeyIterator(nestedItr, keyGenerator);
    }

    private HoodieKeyIterator(ClosableIterator<GenericRecord> nestedItr, Option<BaseKeyGenerator> keyGenerator) {
      this.nestedItr = nestedItr;
      if (keyGenerator.isPresent()) {
        this.func = retVal -> {
          String recordKey = keyGenerator.get().getRecordKey(retVal);
          String partitionPath = keyGenerator.get().getPartitionPath(retVal);
          return new HoodieKey(recordKey, partitionPath);
        };
      } else {
        this.func = retVal -> {
          String recordKey = retVal.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
          String partitionPath = retVal.get(HoodieRecord.PARTITION_PATH_METADATA_FIELD).toString();
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
