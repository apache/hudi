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

package org.apache.hudi.common.util;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.HoodieAvroWriteSupport;
import org.apache.hudi.avro.ValueMetadata;
import org.apache.hudi.avro.ValueType;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.MetadataNotFoundException;
import org.apache.hudi.io.storage.HoodieFileWriter;
import org.apache.hudi.io.storage.HoodieFileWriterFactory;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.metadata.HoodieIndexVersion;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.avro.HoodieAvroSchemaConverter;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.DecimalMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.config.HoodieStorageConfig.PARQUET_BLOCK_SIZE;
import static org.apache.hudi.common.config.HoodieStorageConfig.PARQUET_MAX_FILE_SIZE;
import static org.apache.hudi.common.config.HoodieStorageConfig.PARQUET_PAGE_SIZE;
import static org.apache.hudi.hadoop.fs.HadoopFSUtils.convertToStoragePath;

/**
 * Utility functions involving with parquet.
 */
public class ParquetUtils extends FileFormatUtils {

  private static final Logger LOG = LoggerFactory.getLogger(ParquetUtils.class);

  /**
   * Read the rowKey list matching the given filter, from the given parquet file. If the filter is empty, then this will
   * return all the rowkeys and corresponding positions.
   *
   * @param storage  {@link HoodieStorage} instance.
   * @param filePath The parquet file path.
   * @param filter   record keys filter
   * @return Set Set of pairs of row key and position matching candidateRecordKeys
   */
  @Override
  public Set<Pair<String, Long>> filterRowKeys(HoodieStorage storage, StoragePath filePath, Set<String> filter) {
    return filterParquetRowKeys(storage, new Path(filePath.toUri()), filter, HoodieAvroUtils.getRecordKeySchema());
  }

  public static ParquetMetadata readMetadata(HoodieStorage storage, StoragePath parquetFilePath) {
    Path parquetFileHadoopPath = new Path(parquetFilePath.toUri());
    ParquetMetadata footer;
    try {
      // TODO(vc): Should we use the parallel reading version here?
      footer = ParquetFileReader.readFooter(storage.newInstance(
          parquetFilePath, storage.getConf()).getConf().unwrapAs(Configuration.class), parquetFileHadoopPath);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to read footer for parquet " + parquetFileHadoopPath, e);
    }
    return footer;
  }

  /**
   * Read the rowKey list matching the given filter, from the given parquet file. If the filter is empty, then this will
   * return all the rowkeys.
   *
   * @param storage    {@link HoodieStorage} instance.
   * @param filePath   The parquet file path.
   * @param filter     record keys filter
   * @param readSchema schema of columns to be read
   * @return Set of pairs of row key and position matching candidateRecordKeys
   */
  private static Set<Pair<String, Long>> filterParquetRowKeys(HoodieStorage storage,
                                                              Path filePath, Set<String> filter,
                                                              Schema readSchema) {
    Option<RecordKeysFilterFunction> filterFunction = Option.empty();
    if (filter != null && !filter.isEmpty()) {
      filterFunction = Option.of(new RecordKeysFilterFunction(filter));
    }
    Configuration conf = storage.getConf().unwrapCopyAs(Configuration.class);
    conf.addResource(storage.newInstance(convertToStoragePath(filePath), storage.getConf()).getConf().unwrapAs(Configuration.class));
    AvroReadSupport.setAvroReadSchema(conf, readSchema);
    AvroReadSupport.setRequestedProjection(conf, readSchema);
    Set<Pair<String, Long>> rowKeys = new HashSet<>();
    long rowPosition = 0;
    try (ParquetReader reader = AvroParquetReader.builder(filePath).withConf(conf).build()) {
      Object obj = reader.read();
      while (obj != null) {
        if (obj instanceof GenericRecord) {
          String recordKey = ((GenericRecord) obj).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
          if (!filterFunction.isPresent() || filterFunction.get().apply(recordKey)) {
            rowKeys.add(Pair.of(recordKey, rowPosition));
          }
          obj = reader.read();
          rowPosition++;
        }
      }
    } catch (IOException e) {
      throw new HoodieIOException("Failed to read row keys from Parquet " + filePath, e);

    }
    // ignore
    return rowKeys;
  }

  /**
   * @param codecName codec name in String.
   * @return {@link CompressionCodecName} Enum.
   */
  public static CompressionCodecName getCompressionCodecName(String codecName) {
    return CompressionCodecName.fromConf(StringUtils.isNullOrEmpty(codecName) ? null : codecName);
  }

  /**
   * Fetch {@link HoodieKey}s with row positions from the given parquet file.
   *
   * @param storage  {@link HoodieStorage} instance.
   * @param filePath The parquet file path.
   * @return {@link List} of pairs of {@link HoodieKey} and row position fetched from the parquet file
   */
  @Override
  public ClosableIterator<Pair<HoodieKey, Long>> fetchRecordKeysWithPositions(HoodieStorage storage, StoragePath filePath) {
    return fetchRecordKeysWithPositions(storage, filePath, Option.empty(), Option.empty());
  }

  @Override
  public ClosableIterator<HoodieKey> getHoodieKeyIterator(HoodieStorage storage, StoragePath filePath) {
    return getHoodieKeyIterator(storage, filePath, Option.empty(), Option.empty());
  }

  /**
   * Returns a closable iterator for reading the given parquet file.
   *
   * @param storage         {@link HoodieStorage} instance.
   * @param filePath        The parquet file path
   * @param keyGeneratorOpt instance of KeyGenerator
   * @param partitionPath optional partition path for the file, if provided only the record key is read from the file
   * @return {@link ClosableIterator} of {@link HoodieKey}s for reading the parquet file
   */
  @Override
  public ClosableIterator<HoodieKey> getHoodieKeyIterator(HoodieStorage storage, StoragePath filePath, Option<BaseKeyGenerator> keyGeneratorOpt, Option<String> partitionPath) {
    try {
      Configuration conf = storage.getConf().unwrapCopyAs(Configuration.class);
      conf.addResource(storage.newInstance(filePath, storage.getConf()).getConf().unwrapAs(Configuration.class));
      Schema readSchema = getKeyIteratorSchema(storage, filePath, keyGeneratorOpt, partitionPath);
      AvroReadSupport.setAvroReadSchema(conf, readSchema);
      AvroReadSupport.setRequestedProjection(conf, readSchema);
      ParquetReader<GenericRecord> reader =
          AvroParquetReader.<GenericRecord>builder(new Path(filePath.toUri())).withConf(conf).build();
      return HoodieKeyIterator.getInstance(new ParquetReaderIterator<>(reader), keyGeneratorOpt, partitionPath);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to read from Parquet file " + filePath, e);
    }
  }

  /**
   * Fetch {@link HoodieKey}s with row positions from the given parquet file.
   *
   * @param storage         {@link HoodieStorage} instance.
   * @param filePath        The parquet file path.
   * @param keyGeneratorOpt instance of KeyGenerator.
   * @param partitionPath optional partition path for the file, if provided only the record key is read from the file
   * @return {@link List} of pairs of {@link HoodieKey} and row position fetched from the parquet file
   */
  @Override
  public ClosableIterator<Pair<HoodieKey, Long>> fetchRecordKeysWithPositions(HoodieStorage storage, StoragePath filePath, Option<BaseKeyGenerator> keyGeneratorOpt, Option<String> partitionPath) {
    AtomicLong position = new AtomicLong(0);
    return new CloseableMappingIterator<>(getHoodieKeyIterator(storage, filePath, keyGeneratorOpt, partitionPath), key -> Pair.of(key, position.getAndIncrement()));
  }

  /**
   * Get the schema of the given parquet file.
   */
  public MessageType readSchema(HoodieStorage storage, StoragePath parquetFilePath) {
    return readMetadata(storage, parquetFilePath).getFileMetaData().getSchema();
  }

  @Override
  public Map<String, String> readFooter(HoodieStorage storage, boolean required,
                                        StoragePath filePath, String... footerNames) {
    Map<String, String> footerVals = new HashMap<>();
    ParquetMetadata footer = readMetadata(storage, filePath);
    Map<String, String> metadata = footer.getFileMetaData().getKeyValueMetaData();
    for (String footerName : footerNames) {
      if (metadata.containsKey(footerName)) {
        footerVals.put(footerName, metadata.get(footerName));
      } else if (required) {
        throw new MetadataNotFoundException(
            "Could not find index in Parquet footer. Looked for key " + footerName + " in " + filePath);
      }
    }
    return footerVals;
  }

  @Override
  public Schema readAvroSchema(HoodieStorage storage, StoragePath filePath) {
    MessageType parquetSchema = readSchema(storage, filePath);
    return new HoodieAvroSchemaConverter(storage.getConf().unwrapAs(Configuration.class)).convert(parquetSchema);
  }

  @Override
  public List<HoodieColumnRangeMetadata<Comparable>> readColumnStatsFromMetadata(HoodieStorage storage,
                                                                                 StoragePath filePath,
                                                                                 List<String> columnList,
                                                                                 HoodieIndexVersion indexVersion) {
    ParquetMetadata metadata = readMetadata(storage, filePath);
    return readColumnStatsFromMetadata(metadata, filePath.getName(), Option.of(columnList), indexVersion);
  }

  public List<HoodieColumnRangeMetadata<Comparable>> readColumnStatsFromMetadata(ParquetMetadata metadata, String filePath, Option<List<String>> columnList, HoodieIndexVersion indexVersion) {
    // Collect stats from all individual Parquet blocks
    Stream<HoodieColumnRangeMetadata<Comparable>> hoodieColumnRangeMetadataStream =
        metadata.getBlocks().stream().sequential().flatMap(blockMetaData ->
            blockMetaData.getColumns().stream()
                .filter(f -> !columnList.isPresent() || columnList.get().contains(f.getPath().toDotString()))
                .map(columnChunkMetaData -> {
                  Statistics stats = columnChunkMetaData.getStatistics();
                  ValueMetadata valueMetadata = ValueMetadata.getValueMetadata(columnChunkMetaData.getPrimitiveType(), indexVersion);


                  return (HoodieColumnRangeMetadata<Comparable>) HoodieColumnRangeMetadata.<Comparable>create(
                      filePath,
                      columnChunkMetaData.getPath().toDotString(),
                      convertToNativeJavaType(
                          columnChunkMetaData.getPrimitiveType(),
                          stats.genericGetMin(),
                          valueMetadata),
                      convertToNativeJavaType(
                          columnChunkMetaData.getPrimitiveType(),
                          stats.genericGetMax(),
                          valueMetadata),
                      // NOTE: In case when column contains only nulls Parquet won't be creating
                      //       stats for it instead returning stubbed (empty) object. In that case
                      //       we have to equate number of nulls to the value count ourselves
                      stats.isEmpty() ? columnChunkMetaData.getValueCount() : stats.getNumNulls(),
                      columnChunkMetaData.getValueCount(),
                      columnChunkMetaData.getTotalSize(),
                      columnChunkMetaData.getTotalUncompressedSize(),
                      valueMetadata, indexVersion);
                })
        );

    return mergeColumnStats(hoodieColumnRangeMetadataStream);
  }

  public List<HoodieColumnRangeMetadata<Comparable>> mergeColumnStats(Stream<HoodieColumnRangeMetadata<Comparable>> columnStats) {
    Map<String, List<HoodieColumnRangeMetadata<Comparable>>> columnToStatsListMap =
        columnStats.collect(Collectors.groupingBy(HoodieColumnRangeMetadata::getColumnName));

    // Combine those into file-level statistics
    // NOTE: Inlining this var makes javac (1.8) upset (due to its inability to infer
    // expression type correctly)
    Stream<HoodieColumnRangeMetadata<Comparable>> stream = columnToStatsListMap.values()
        .stream()
        .map(this::getColumnRangeInFile);

    return stream.collect(Collectors.toList());
  }

  @Override
  public HoodieFileFormat getFormat() {
    return HoodieFileFormat.PARQUET;
  }

  /**
   * NOTE: This literally reads the entire file contents, thus should be used with caution.
   */
  @Override
  public List<GenericRecord> readAvroRecords(HoodieStorage storage, StoragePath filePath) {
    List<GenericRecord> records = new ArrayList<>();
    try (ParquetReader reader = AvroParquetReader.builder(new Path(filePath.toUri()))
        .withConf(storage.getConf().unwrapAs(Configuration.class)).build()) {
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
  public List<GenericRecord> readAvroRecords(HoodieStorage storage, StoragePath filePath, Schema schema) {
    AvroReadSupport.setAvroReadSchema(storage.getConf().unwrapAs(Configuration.class), schema);
    return readAvroRecords(storage, filePath);
  }

  /**
   * Returns the number of records in the parquet file.
   *
   * @param storage  {@link HoodieStorage} instance.
   * @param filePath path of the file
   */
  @Override
  public long getRowCount(HoodieStorage storage, StoragePath filePath) {
    ParquetMetadata footer;
    long rowCount = 0;
    footer = readMetadata(storage, filePath);
    for (BlockMetaData b : footer.getBlocks()) {
      rowCount += b.getRowCount();
    }
    return rowCount;
  }

  @Override
  public void writeMetaFile(HoodieStorage storage,
                            StoragePath filePath,
                            Properties props) throws IOException {
    // Since we are only interested in saving metadata to the footer, the schema, blocksizes and other
    // parameters are not important.
    Schema schema = HoodieAvroUtils.getRecordKeySchema();
    MessageType type = Types.buildMessage().optional(PrimitiveType.PrimitiveTypeName.INT64).named("dummyint").named("dummy");
    HoodieAvroWriteSupport writeSupport = new HoodieAvroWriteSupport(type, schema, Option.empty(), new Properties());
    try (ParquetWriter writer = new ParquetWriter(new Path(filePath.toUri()), writeSupport, CompressionCodecName.UNCOMPRESSED, 1024, 1024)) {
      for (String key : props.stringPropertyNames()) {
        writeSupport.addFooterMetadata(key, props.getProperty(key));
      }
    }
  }

  @Override
  public ByteArrayOutputStream serializeRecordsToLogBlock(HoodieStorage storage,
                                                          List<HoodieRecord> records,
                                                          Schema writerSchema,
                                                          Schema readerSchema,
                                                          String keyFieldName,
                                                          Map<String, String> paramsMap) throws IOException {
    if (records.size() == 0) {
      return new ByteArrayOutputStream(0);
    }

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    HoodieConfig config = new HoodieConfig();
    paramsMap.entrySet().stream().forEach(entry -> config.setValue(entry.getKey(), entry.getValue()));
    config.setValue(PARQUET_BLOCK_SIZE.key(), String.valueOf(ParquetWriter.DEFAULT_BLOCK_SIZE));
    config.setValue(PARQUET_PAGE_SIZE.key(), String.valueOf(ParquetWriter.DEFAULT_PAGE_SIZE));
    config.setValue(PARQUET_MAX_FILE_SIZE.key(), String.valueOf(1024 * 1024 * 1024));
    HoodieRecord.HoodieRecordType recordType = records.iterator().next().getRecordType();
    try (HoodieFileWriter parquetWriter = HoodieFileWriterFactory.getFileWriter(
        HoodieFileFormat.PARQUET, outputStream, storage, config, writerSchema, recordType)) {
      for (HoodieRecord<?> record : records) {
        String recordKey = record.getRecordKey(readerSchema, keyFieldName);
        parquetWriter.write(recordKey, record, writerSchema);
      }
      outputStream.flush();
    }
    return outputStream;
  }

  @Override
  public Pair<ByteArrayOutputStream, Object> serializeRecordsToLogBlock(HoodieStorage storage,
                                                                        Iterator<HoodieRecord> recordItr,
                                                                        HoodieRecord.HoodieRecordType recordType,
                                                                        Schema writerSchema,
                                                                        Schema readerSchema,
                                                                        String keyFieldName,
                                                                        Map<String, String> paramsMap) throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    HoodieConfig config = new HoodieConfig();
    paramsMap.entrySet().stream().forEach(entry -> config.setValue(entry.getKey(), entry.getValue()));
    config.setValue(PARQUET_BLOCK_SIZE.key(), String.valueOf(ParquetWriter.DEFAULT_BLOCK_SIZE));
    config.setValue(PARQUET_PAGE_SIZE.key(), String.valueOf(ParquetWriter.DEFAULT_PAGE_SIZE));
    config.setValue(PARQUET_MAX_FILE_SIZE.key(), String.valueOf(1024 * 1024 * 1024));

    HoodieFileWriter parquetWriter = HoodieFileWriterFactory.getFileWriter(
        HoodieFileFormat.PARQUET, outputStream, storage, config, writerSchema, recordType);
    while (recordItr.hasNext()) {
      HoodieRecord record = recordItr.next();
      String recordKey = record.getRecordKey(readerSchema, keyFieldName);
      parquetWriter.write(recordKey, record, writerSchema);
    }
    outputStream.flush();
    parquetWriter.close();
    return Pair.of(outputStream, parquetWriter.getFileFormatMetadata());
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
        .reduce(HoodieColumnRangeMetadata::merge).get();
  }

  private static Comparable<?> convertToNativeJavaType(PrimitiveType primitiveType, Comparable<?> val, ValueMetadata valueMetadata) {
    if (valueMetadata.getValueType() != ValueType.NONE) {
      return valueMetadata.standardizeJavaTypeAndPromote(val);
    }
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
}
