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

package org.apache.hudi.io.storage;

import org.apache.hudi.HoodieSchemaConversionUtils;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.bloom.BloomFilterTypeCode;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieSparkRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.io.memory.HoodieArrowAllocator;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.io.vortex.VortexInputFile;

import dev.vortex.api.Expression;
import dev.vortex.api.ImmutableScanOptions;
import dev.vortex.api.Scan;
import dev.vortex.api.ScanOptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hudi.common.avro.HoodieBloomFilterWriteSupport.HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY;
import static org.apache.hudi.common.avro.HoodieBloomFilterWriteSupport.HOODIE_BLOOM_FILTER_TYPE_CODE;
import static org.apache.hudi.common.avro.HoodieBloomFilterWriteSupport.HOODIE_MAX_RECORD_KEY_FOOTER;
import static org.apache.hudi.common.avro.HoodieBloomFilterWriteSupport.HOODIE_MIN_RECORD_KEY_FOOTER;
import static org.apache.hudi.common.model.HoodieRecord.RECORD_KEY_METADATA_FIELD;
import static org.apache.hudi.common.table.read.buffer.PositionBasedFileGroupRecordBuffer.ROW_INDEX_TEMPORARY_COLUMN_NAME;
import static org.apache.hudi.common.util.TypeUtils.unsafeCast;

/**
 * {@link HoodieSparkFileReader} implementation for Vortex file format, backed by the vortex-jni
 * {@code dev.vortex.api} reader.
 *
 * <p>The file is read through the Hadoop {@code FileSystem} of its path, see
 * {@link VortexInputFile}. Each scan reads only the requested columns, in the order of the rows
 * in the file.
 *
 * <p>The bloom filter and the min/max record keys come from the Vortex file metadata segments,
 * which the Hudi Vortex writer writes with the same keys as the Parquet footer. Each value is
 * the UTF-8 bytes of the footer string.
 */
@Slf4j
public class HoodieSparkVortexReader implements HoodieSparkFileReader {
  private final StoragePath path;
  private final Configuration hadoopConf;
  private final long dataAllocatorSize;

  // Holds the footer of the file for the metadata, the schema and the row count.
  private final VortexInputFile metadataFile;
  private final BufferAllocator metadataAllocator;

  // Read on first use.
  private Map<String, byte[]> footerMetadata;
  private StructType fileSchema;

  public HoodieSparkVortexReader(StoragePath path) {
    this(path, Long.parseLong(HoodieStorageConfig.VORTEX_READ_ALLOCATOR_SIZE_BYTES.defaultValue()));
  }

  public HoodieSparkVortexReader(StoragePath path, long dataAllocatorSize) {
    this(path, dataAllocatorSize, new Configuration());
  }

  public HoodieSparkVortexReader(StoragePath path, long dataAllocatorSize, StorageConfiguration<?> storageConf) {
    this(path, dataAllocatorSize, storageConf.unwrapAs(Configuration.class));
  }

  private HoodieSparkVortexReader(StoragePath path, long dataAllocatorSize, Configuration hadoopConf) {
    this.path = path;
    this.hadoopConf = hadoopConf;
    this.dataAllocatorSize = dataAllocatorSize;
    this.metadataAllocator = HoodieArrowAllocator.newChildAllocator(
        getClass().getSimpleName() + "-meta-" + path.getName(),
        Long.parseLong(HoodieStorageConfig.VORTEX_READ_METADATA_ALLOCATOR_SIZE_BYTES.defaultValue()));

    try {
      this.metadataFile = VortexInputFile.open(hadoopConf, path.toString());
    } catch (Exception e) {
      metadataAllocator.close();
      throw new HoodieException("Failed to open Vortex file: " + path, e);
    }
  }

  @Override
  public String[] readMinMaxRecordKeys() {
    Map<String, byte[]> metadata = getFooterMetadata();
    byte[] minKey = metadata.get(HOODIE_MIN_RECORD_KEY_FOOTER);
    byte[] maxKey = metadata.get(HOODIE_MAX_RECORD_KEY_FOOTER);
    if (minKey == null || maxKey == null) {
      throw new HoodieException("Could not read min/max record key out of Vortex file: " + path);
    }

    return new String[] {
        StringUtils.fromUTF8Bytes(minKey), StringUtils.fromUTF8Bytes(maxKey)};
  }

  @Override
  public BloomFilter readBloomFilter() {
    Map<String, byte[]> metadata = getFooterMetadata();
    byte[] serialized = metadata.get(HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY);
    if (serialized == null) {
      // Callers treat null as "no bloom filter".
      return null;
    }

    byte[] typeCode = metadata.get(HOODIE_BLOOM_FILTER_TYPE_CODE);
    String typeCodeName = typeCode == null
        ? BloomFilterTypeCode.SIMPLE.name()
        : StringUtils.fromUTF8Bytes(typeCode);
    return BloomFilterFactory.fromString(StringUtils.fromUTF8Bytes(serialized), typeCodeName);
  }

  private Map<String, byte[]> getFooterMetadata() {
    if (footerMetadata == null) {
      try {
        footerMetadata = metadataFile.readMetadata();
      } catch (Exception e) {
        throw new HoodieException("Failed to read footer metadata from Vortex file: " + path, e);
      }
    }
    return footerMetadata;
  }

  /**
   * Returns the record keys of the file that are in {@code candidateRowKeys}, with the position
   * of each record in the file. If {@code candidateRowKeys} is null or empty, returns all keys.
   *
   * <p>Vortex evaluates the key filter, and the positions come from the row index of the scan.
   */
  @Override
  public Set<Pair<String, Long>> filterRowKeys(Set<String> candidateRowKeys) {
    StructType schema = new StructType()
        .add(RECORD_KEY_METADATA_FIELD, DataTypes.StringType)
        .add(ROW_INDEX_TEMPORARY_COLUMN_NAME, DataTypes.LongType, false);

    ImmutableScanOptions.Builder options = ScanOptions.builder().projection(VortexScans.projection(schema));
    if (candidateRowKeys != null && !candidateRowKeys.isEmpty()) {
      Expression keys = Expression.literalStringList(
          Expression.nullLiteral(Expression.DType.UTF8), false, candidateRowKeys.toArray(new String[0]));
      options.filter(Expression.in(Expression.column(RECORD_KEY_METADATA_FIELD), keys));
    }

    Set<Pair<String, Long>> result = new HashSet<>();
    try (ClosableIterator<UnsafeRow> rows = scan(options, schema)) {
      while (rows.hasNext()) {
        UnsafeRow row = rows.next();
        result.add(Pair.of(row.getUTF8String(0).toString(), row.getLong(1)));
      }
    }
    return result;
  }

  @Override
  public ClosableIterator<HoodieRecord<InternalRow>> getRecordIterator(HoodieSchema readerSchema, HoodieSchema requestedSchema) {
    return getRecordIterator(requestedSchema);
  }

  @Override
  public ClosableIterator<HoodieRecord<InternalRow>> getRecordIterator(HoodieSchema schema) {
    ClosableIterator<UnsafeRow> iterator = getUnsafeRowIterator(schema);
    return new CloseableMappingIterator<>(iterator, data -> unsafeCast(new HoodieSparkRecord(data)));
  }

  @Override
  public ClosableIterator<String> getRecordKeyIterator() {
    HoodieSchema recordKeySchema = HoodieSchemaUtils.getRecordKeySchema();
    ClosableIterator<UnsafeRow> iterator = getUnsafeRowIterator(recordKeySchema);
    return new CloseableMappingIterator<>(iterator, data -> data.getUTF8String(0).toString());
  }

  /**
   * Reads the requested columns, in the order of the rows in the file.
   */
  private ClosableIterator<UnsafeRow> getUnsafeRowIterator(HoodieSchema requestedSchema) {
    StructType requestedSparkSchema = HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(requestedSchema);

    // Project only the columns that the file has. The iterator reports a missing column.
    List<StructField> readFields = new ArrayList<>();
    StructType fileColumns = getFileSchema();
    for (StructField field : requestedSparkSchema.fields()) {
      if (VortexScans.isRowIndexColumn(field.name()) || fileColumns.getFieldIndex(field.name()).isDefined()) {
        readFields.add(field);
      }
    }

    StructType readSchema = new StructType(readFields.toArray(new StructField[0]));
    ImmutableScanOptions.Builder options = ScanOptions.builder()
        .projection(VortexScans.projection(readSchema, fileColumns))
        .ordered(true);
    return scan(options, requestedSparkSchema);
  }

  private ClosableIterator<UnsafeRow> scan(ImmutableScanOptions.Builder options, StructType outputSchema) {
    BufferAllocator allocator = HoodieArrowAllocator.newChildAllocator(
        getClass().getSimpleName() + "-data-" + path.getName(), dataAllocatorSize);

    VortexInputFile file = null;
    try {
      file = VortexInputFile.open(hadoopConf, path.toString());
      Scan scan = file.scan(options.build());
      return new VortexRecordIterator(allocator, file, scan, outputSchema);
    } catch (Exception e) {
      if (file != null) {
        closeAfterFailure(file, e);
      }
      allocator.close();
      throw new HoodieException("Failed to create Vortex reader for: " + path, e);
    }
  }

  private static void closeAfterFailure(VortexInputFile file, Exception failure) {
    try {
      file.close();
    } catch (Exception e) {
      failure.addSuppressed(e);
    }
  }

  private StructType getFileSchema() {
    if (fileSchema == null) {
      try {
        fileSchema = VortexArrowUtils.toStructType(metadataFile.arrowSchema(metadataAllocator));
      } catch (Exception e) {
        throw new HoodieException("Failed to read schema from Vortex file: " + path, e);
      }
    }
    return fileSchema;
  }

  @Override
  public HoodieSchema getSchema() {
    return HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(getFileSchema(), "record", "", false);
  }

  @Override
  public void close() {
    try {
      metadataFile.close();
    } finally {
      try {
        metadataAllocator.close();
      } catch (Exception e) {
        log.warn("Error while closing Vortex metadata allocator: {}", e.getMessage());
      }
    }
  }

  @Override
  public long getTotalRecords() {
    try {
      return metadataFile.exactRowCount().orElseThrow(
          () -> new HoodieException("Vortex row count unavailable for file: " + path));
    } catch (HoodieException e) {
      throw e;
    } catch (Exception e) {
      throw new HoodieException("Failed to get row count from Vortex file: " + path, e);
    }
  }
}
