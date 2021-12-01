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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.MetadataNotFoundException;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.DecimalMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utility functions involving with parquet.
 */
public class ParquetUtils extends BaseFileUtils {

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
  public List<HoodieKey> fetchRecordKeyPartitionPath(Configuration configuration, Path filePath) {
    return fetchRecordKeyPartitionPathInternal(configuration, filePath, Option.empty());
  }

  private List<HoodieKey> fetchRecordKeyPartitionPathInternal(Configuration configuration, Path filePath, Option<BaseKeyGenerator> keyGeneratorOpt) {
    List<HoodieKey> hoodieKeys = new ArrayList<>();
    try {
      Configuration conf = new Configuration(configuration);
      conf.addResource(FSUtils.getFs(filePath.toString(), conf).getConf());
      Schema readSchema = keyGeneratorOpt.map(keyGenerator -> {
        List<String> fields = new ArrayList<>();
        fields.addAll(keyGenerator.getRecordKeyFields());
        fields.addAll(keyGenerator.getPartitionPathFields());
        return HoodieAvroUtils.getSchemaForFields(readAvroSchema(conf, filePath), fields);
      })
          .orElse(HoodieAvroUtils.getRecordKeyPartitionPathSchema());
      AvroReadSupport.setAvroReadSchema(conf, readSchema);
      AvroReadSupport.setRequestedProjection(conf, readSchema);
      ParquetReader reader = AvroParquetReader.builder(filePath).withConf(conf).build();
      Object obj = reader.read();
      while (obj != null) {
        if (obj instanceof GenericRecord) {
          String recordKey = null;
          String partitionPath = null;
          if (keyGeneratorOpt.isPresent()) {
            recordKey = keyGeneratorOpt.get().getRecordKey((GenericRecord) obj);
            partitionPath = keyGeneratorOpt.get().getPartitionPath((GenericRecord) obj);
          } else {
            recordKey = ((GenericRecord) obj).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
            partitionPath = ((GenericRecord) obj).get(HoodieRecord.PARTITION_PATH_METADATA_FIELD).toString();
          }
          hoodieKeys.add(new HoodieKey(recordKey, partitionPath));
          obj = reader.read();
        }
      }
    } catch (IOException e) {
      throw new HoodieIOException("Failed to read from Parquet file " + filePath, e);
    }
    return hoodieKeys;
  }

  /**
   * Fetch {@link HoodieKey}s from the given parquet file.
   *
   * @param configuration   configuration to build fs object
   * @param filePath        The parquet file path.
   * @param keyGeneratorOpt
   * @return {@link List} of {@link HoodieKey}s fetched from the parquet file
   */
  @Override
  public List<HoodieKey> fetchRecordKeyPartitionPath(Configuration configuration, Path filePath, Option<BaseKeyGenerator> keyGeneratorOpt) {
    return fetchRecordKeyPartitionPathInternal(configuration, filePath, keyGeneratorOpt);
  }

  public ParquetMetadata readMetadata(Configuration conf, Path parquetFilePath) {
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

  /**
   * NOTE: This literally reads the entire file contents, thus should be used with caution.
   */
  @Override
  public List<GenericRecord> readAvroRecords(Configuration configuration, Path filePath) {
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
  public List<HoodieColumnRangeMetadata<Comparable>> readRangeFromParquetMetadata(
      @Nonnull Configuration conf,
      @Nonnull Path parquetFilePath,
      @Nonnull List<String> cols
  ) {
    ParquetMetadata metadata = readMetadata(conf, parquetFilePath);
    // Collect stats from all individual Parquet blocks
    Map<String, List<HoodieColumnRangeMetadata<Comparable>>> columnToStatsListMap = metadata.getBlocks().stream().sequential()
            .flatMap(blockMetaData -> blockMetaData.getColumns().stream()
                    .filter(f -> cols.contains(f.getPath().toDotString()))
                    .map(columnChunkMetaData ->
                        new HoodieColumnRangeMetadata<Comparable>(
                            parquetFilePath.getName(),
                            columnChunkMetaData.getPath().toDotString(),
                            convertToNativeJavaType(
                                columnChunkMetaData.getPrimitiveType(),
                                columnChunkMetaData.getStatistics().genericGetMin()),
                            convertToNativeJavaType(
                                columnChunkMetaData.getPrimitiveType(),
                                columnChunkMetaData.getStatistics().genericGetMax()),
                            columnChunkMetaData.getStatistics().getNumNulls(),
                            columnChunkMetaData.getPrimitiveType().stringifier()))
            ).collect(Collectors.groupingBy(HoodieColumnRangeMetadata::getColumnName));

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
    } else  {
      maxValue = one.getMaxValue();
    }

    return new HoodieColumnRangeMetadata<T>(
        one.getFilePath(),
        one.getColumnName(), minValue, maxValue, one.getNumNulls() + another.getNumNulls(), one.getStringifier());
  }

  private static Comparable<?> convertToNativeJavaType(PrimitiveType primitiveType, Comparable val) {
    if (primitiveType.getOriginalType() == OriginalType.DECIMAL) {
      DecimalMetadata decimalMetadata = primitiveType.getDecimalMetadata();
      return BigDecimal.valueOf((Integer) val, decimalMetadata.getScale());
    } else if (primitiveType.getOriginalType() == OriginalType.DATE) {
      // NOTE: This is a workaround to address race-condition in using
      //       {@code SimpleDataFormat} concurrently (w/in {@code DateStringifier})
      // TODO cleanup after Parquet upgrade to 1.12
      synchronized (primitiveType.stringifier()) {
        return java.sql.Date.valueOf(
            primitiveType.stringifier().stringify((Integer) val)
        );
      }
    }

    return val;
  }
}
