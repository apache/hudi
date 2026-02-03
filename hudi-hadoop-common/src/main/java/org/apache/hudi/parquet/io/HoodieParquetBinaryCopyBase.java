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

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.exception.HoodieException;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.column.EncodingStats;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.format.BlockCipher;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.ColumnChunkPageWriteStore;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.CompressionConverter;
import org.apache.parquet.hadoop.util.HadoopCodecs;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.io.InvalidRecordException;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.parquet.column.ParquetProperties.DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH;
import static org.apache.parquet.column.ParquetProperties.DEFAULT_STATISTICS_TRUNCATE_LENGTH;
import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE;
import static org.apache.parquet.hadoop.ParquetWriter.MAX_PADDING_SIZE_DEFAULT;
import static org.apache.parquet.schema.OriginalType.LIST;
import static org.apache.parquet.schema.OriginalType.MAP;

/**
 * Copy from parquet-hadoop 1.13.1 org.apache.parquet.hadoop.rewrite.ParquetRewriter
 * The reason we did not extends ParquetRewriter is
 *    1. we need to control copy operation at block level
 *    2. We need to handle schema evolution
 *    3. We need to combine file metas added by hudi, such as 'hoodie_min_record_key'/'hoodie_max_record_key'/'org.apache.hudi.bloomfilter'
 *    4. We need to overwrite column '_hoodie_file_name' with the output file name
 */
@Slf4j
public abstract class HoodieParquetBinaryCopyBase implements Closeable {

  // Key to store original writer version in the file key-value metadata
  public static final String ORIGINAL_CREATED_BY_KEY = "original.created.by";

  private final int pageBufferSize = ParquetProperties.DEFAULT_PAGE_SIZE * 2;

  private final byte[] pageBuffer = new byte[pageBufferSize];

  private CompressionCodecName newCodecName = null;

  private Map<ColumnPath, Binary> maskColumns = new HashMap<>();

  // Writer to rewrite the input files
  private ParquetFileWriter writer;

  // Number of blocks written which is used to keep track of the actual row group ordinal
  private int numBlocksRewritten = 0;

  protected long totalRecordsWritten = 0;

  // Schema of input files (should be the same) and to write to the output file
  protected MessageType requiredSchema = null;

  protected Configuration conf;
  
  // Flag to control schema evolution behavior
  protected Boolean schemaEvolutionEnabled = null;

  public HoodieParquetBinaryCopyBase(Configuration conf) {
    this.conf = conf;
  }
  
  public void setSchemaEvolutionEnabled(boolean enabled) {
    this.schemaEvolutionEnabled = enabled;
  }

  protected void initFileWriter(Path outPutFile, CompressionCodecName newCodecName, MessageType schema) {
    try {
      // For meta column '_hoodie_file_name', rewriter will mask value with output file name
      Binary maskValue = Binary.fromString(outPutFile.getName());
      maskColumns.put(ColumnPath.fromDotString(HoodieRecord.FILENAME_METADATA_FIELD), maskValue);
      this.requiredSchema = schema;
      this.newCodecName = newCodecName;
      ParquetFileWriter.Mode writerMode = ParquetFileWriter.Mode.CREATE;
      writer = new ParquetFileWriter(
          HadoopOutputFile.fromPath(outPutFile, conf),
          schema,
          writerMode,
          DEFAULT_BLOCK_SIZE,
          MAX_PADDING_SIZE_DEFAULT,
          DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH,
          DEFAULT_STATISTICS_TRUNCATE_LENGTH,
          ParquetProperties.DEFAULT_PAGE_WRITE_CHECKSUM_ENABLED);
      writer.start();
      log.info("init writer ");
    } catch (Exception e) {
      log.error("failed to init parquet writer", e);
      throw new HoodieException(e);
    }
  }

  @Override
  public void close() throws IOException {
    Map<String, String> extraMetaData = finalizeMetadata();
    extraMetaData = extraMetaData == null ? new HashMap<>() : extraMetaData;
    extraMetaData.remove("parquet.avro.schema");
    extraMetaData.remove("org.apache.spark.sql.parquet.row.metadata");
    writer.end(extraMetaData);
  }

  protected abstract Map<String, String> finalizeMetadata();

  public void processBlocksFromReader(
      CompressionConverter.TransParquetFileReader reader,
      PageReadStore store,
      BlockMetaData block,
      String originalCreatedBy) throws IOException {
    if (store == null) {
      log.info("stores is empty");
      return;
    }

    totalRecordsWritten += store.getRowCount();
    ColumnReadStoreImpl crStore = new ColumnReadStoreImpl(
        store,
        new DummyGroupConverter(),
        requiredSchema,
        originalCreatedBy);
    Map<ColumnPath, ColumnDescriptor> descriptorsMap = requiredSchema.getColumns()
        .stream()
        .collect(Collectors.toMap(x -> ColumnPath.get(x.getPath()), x -> x));

    writer.startBlock(store.getRowCount());
    List<ColumnChunkMetaData> columnsInOrder = block.getColumns();
    List<ColumnDescriptor> converted = new ArrayList<>();

    for (int i = 0; i < columnsInOrder.size(); i++) {
      ColumnChunkMetaData chunk = columnsInOrder.get(i);
      ColumnDescriptor descriptor = descriptorsMap.get(chunk.getPath());
      if (schemaEvolutionEnabled == null) {
        throw new HoodieException("The variable 'schemaEvolutionEnabled' is supposed to be set in "
                + "binaryCopy() before calling this method processBlocksFromReader");
      }

      // resolve the conflict schema between avro parquet write support and spark native parquet write support
      // Only attempt legacy conversion if schema evolution is enabled
      if (descriptor == null && schemaEvolutionEnabled) {
        String[] path = chunk.getPath().toArray();
        path = Arrays.copyOf(path, path.length);
        if (convertLegacy3LevelArray(path) || convertLegacyMap(path)) {
          ColumnPath newPath = ColumnPath.get(path);
          ColumnChunkMetaData newChunk = ColumnChunkMetaData.get(
              newPath,
              chunk.getPrimitiveType(),
              chunk.getCodec(),
              chunk.getEncodingStats(),
              chunk.getEncodings(),
              chunk.getStatistics(),
              chunk.getFirstDataPageOffset(),
              chunk.getDictionaryPageOffset(),
              chunk.getValueCount(),
              chunk.getTotalSize(),
              chunk.getTotalUncompressedSize());
          newChunk.setRowGroupOrdinal(chunk.getRowGroupOrdinal());
          newChunk.setBloomFilterOffset(chunk.getBloomFilterOffset());
          newChunk.setColumnIndexReference(chunk.getColumnIndexReference());
          newChunk.setOffsetIndexReference(chunk.getOffsetIndexReference());
          chunk = newChunk;
          descriptor = descriptorsMap.get(chunk.getPath());
          converted.add(descriptor);
        }
      }

      // This column has been pruned.
      if (descriptor == null) {
        continue;
      }

      reader.setStreamPosition(chunk.getStartingPos());
      CompressionCodecName newCodecName = this.newCodecName == null ? chunk.getCodec() : this.newCodecName;

      if (maskColumns != null && maskColumns.containsKey(chunk.getPath())) {
        // Check if this is NOT the FILENAME_METADATA_FIELD and schema evolution is disabled
        if (!chunk.getPath().toDotString().equals(HoodieRecord.FILENAME_METADATA_FIELD)) {
          if (!schemaEvolutionEnabled) {
            throw new HoodieException("Column masking for '" + chunk.getPath().toDotString() 
                + "' requires schema evolution to be enabled. "
                + "Set 'hoodie.clustering.plan.strategy.binary.copy.schema.evolution.enable' to true.");
          }
        }
        // Mask column and compress it again.
        Binary maskValue = maskColumns.get(chunk.getPath());
        if (maskValue != null) {
          maskColumn(
              descriptor,
              chunk,
              crStore,
              writer,
              requiredSchema,
              newCodecName,
              maskValue);
        }
      } else if (this.newCodecName != null && this.newCodecName != chunk.getCodec()) {
        // Translate compression and/or encryption
        writer.startColumn(descriptor, crStore.getColumnReader(descriptor).getTotalValueCount(), newCodecName);
        processChunk(reader, chunk, newCodecName, false, originalCreatedBy);
        writer.endColumn();
      } else {
        // Nothing changed, simply copy the binary data.
        BloomFilter bloomFilter = reader.readBloomFilter(chunk);
        ColumnIndex columnIndex = reader.readColumnIndex(chunk);
        OffsetIndex offsetIndex = reader.readOffsetIndex(chunk);
        writer.appendColumnChunk(descriptor, reader.getStream(), chunk, bloomFilter, columnIndex, offsetIndex);
      }

    }

    // append missed columns
    ParquetMetadata meta = reader.getFooter();
    ColumnChunkMetaData columnChunkMetaData = columnsInOrder.get(0);
    EncodingStats encodingStats = columnChunkMetaData.getEncodingStats();
    List<ColumnDescriptor> missedColumns = missedColumns(requiredSchema, meta.getFileMetaData().getSchema())
        .stream()
        .filter(c -> !converted.contains(c))
        .collect(Collectors.toList());
    
    // If schema evolution is disabled and there are missing columns, throw an exception
    if (!schemaEvolutionEnabled && !missedColumns.isEmpty()) {
      String missingColumnsStr = missedColumns.stream()
          .map(c -> String.join(".", c.getPath()))
          .collect(Collectors.joining(", "));
      throw new HoodieException("Schema evolution is disabled but found missing columns in input file: " 
          + missingColumnsStr + ". All input files must have the same schema when schema evolution is disabled.");
    }
    
    for (ColumnDescriptor descriptor : missedColumns) {
      addNullColumn(
          descriptor,
          store.getRowCount(),
          encodingStats,
          writer,
          requiredSchema,
          newCodecName);
    }

    writer.endBlock();
    numBlocksRewritten++;
  }

  private void processChunk(
      CompressionConverter.TransParquetFileReader reader,
      ColumnChunkMetaData chunk,
      CompressionCodecName newCodecName,
      boolean encryptColumn,
      String originalCreatedBy) throws IOException {
    CompressionCodecFactory codecFactory = HadoopCodecs.newFactory(0);
    CompressionCodecFactory.BytesInputDecompressor decompressor = null;
    CompressionCodecFactory.BytesInputCompressor compressor = null;
    if (!newCodecName.equals(chunk.getCodec())) {
      // Re-compress only if a different codec has been specified
      decompressor = codecFactory.getDecompressor(chunk.getCodec());
      compressor = codecFactory.getCompressor(newCodecName);
    }

    // EncryptorRunTime is only provided when encryption is required
    BlockCipher.Encryptor metaEncryptor = null;
    BlockCipher.Encryptor dataEncryptor = null;
    byte[] dictPageAAD = null;
    byte[] dataPageAAD = null;
    byte[] dictPageHeaderAAD = null;
    byte[] dataPageHeaderAAD = null;

    ColumnIndex columnIndex = reader.readColumnIndex(chunk);
    OffsetIndex offsetIndex = reader.readOffsetIndex(chunk);

    reader.setStreamPosition(chunk.getStartingPos());
    DictionaryPage dictionaryPage = null;
    long readValues = 0;
    Statistics statistics = null;
    ParquetMetadataConverter converter = new ParquetMetadataConverter();
    int pageOrdinal = 0;
    long totalChunkValues = chunk.getValueCount();
    while (readValues < totalChunkValues) {
      PageHeader pageHeader = reader.readPageHeader();
      int compressedPageSize = pageHeader.getCompressed_page_size();
      byte[] pageLoad;
      switch (pageHeader.type) {
        case DICTIONARY_PAGE:
          if (dictionaryPage != null) {
            throw new IOException("has more than one dictionary page in column chunk");
          }
          //No quickUpdatePageAAD needed for dictionary page
          DictionaryPageHeader dictPageHeader = pageHeader.dictionary_page_header;
          pageLoad = processPageLoad(reader,
              true,
              compressor,
              decompressor,
              pageHeader.getCompressed_page_size(),
              pageHeader.getUncompressed_page_size(),
              encryptColumn,
              dataEncryptor,
              dictPageAAD);
          writer.writeDictionaryPage(new DictionaryPage(BytesInput.from(pageLoad),
                  pageHeader.getUncompressed_page_size(),
                  dictPageHeader.getNum_values(),
                  converter.getEncoding(dictPageHeader.getEncoding())),
              metaEncryptor,
              dictPageHeaderAAD);
          break;
        case DATA_PAGE:
          DataPageHeader headerV1 = pageHeader.data_page_header;
          pageLoad = processPageLoad(reader,
              true,
              compressor,
              decompressor,
              pageHeader.getCompressed_page_size(),
              pageHeader.getUncompressed_page_size(),
              encryptColumn,
              dataEncryptor,
              dataPageAAD);
          statistics = convertStatistics(
              originalCreatedBy, chunk.getPrimitiveType(), headerV1.getStatistics(), columnIndex, pageOrdinal, converter);
          readValues += headerV1.getNum_values();
          if (offsetIndex != null) {
            long rowCount = 1 + offsetIndex.getLastRowIndex(
                pageOrdinal, totalChunkValues) - offsetIndex.getFirstRowIndex(pageOrdinal);
            writer.writeDataPage(toIntWithCheck(headerV1.getNum_values()),
                pageHeader.getUncompressed_page_size(),
                BytesInput.from(pageLoad),
                statistics,
                toIntWithCheck(rowCount),
                converter.getEncoding(headerV1.getRepetition_level_encoding()),
                converter.getEncoding(headerV1.getDefinition_level_encoding()),
                converter.getEncoding(headerV1.getEncoding()));
          } else {
            writer.writeDataPage(toIntWithCheck(headerV1.getNum_values()),
                pageHeader.getUncompressed_page_size(),
                BytesInput.from(pageLoad),
                statistics,
                converter.getEncoding(headerV1.getRepetition_level_encoding()),
                converter.getEncoding(headerV1.getDefinition_level_encoding()),
                converter.getEncoding(headerV1.getEncoding()));
          }
          pageOrdinal++;
          break;
        case DATA_PAGE_V2:
          DataPageHeaderV2 headerV2 = pageHeader.data_page_header_v2;
          int rlLength = headerV2.getRepetition_levels_byte_length();
          BytesInput rlLevels = readBlockAllocate(rlLength, reader);
          int dlLength = headerV2.getDefinition_levels_byte_length();
          BytesInput dlLevels = readBlockAllocate(dlLength, reader);
          int payLoadLength = pageHeader.getCompressed_page_size() - rlLength - dlLength;
          int rawDataLength = pageHeader.getUncompressed_page_size() - rlLength - dlLength;
          pageLoad = processPageLoad(
              reader,
              headerV2.is_compressed,
              compressor,
              decompressor,
              payLoadLength,
              rawDataLength,
              encryptColumn,
              dataEncryptor,
              dataPageAAD);
          statistics = convertStatistics(
              originalCreatedBy, chunk.getPrimitiveType(), headerV2.getStatistics(), columnIndex, pageOrdinal, converter);
          readValues += headerV2.getNum_values();
          writer.writeDataPageV2(headerV2.getNum_rows(),
              headerV2.getNum_nulls(),
              headerV2.getNum_values(),
              rlLevels,
              dlLevels,
              converter.getEncoding(headerV2.getEncoding()),
              BytesInput.from(pageLoad),
              rawDataLength,
              statistics);
          pageOrdinal++;
          break;
        default:
          log.debug("skipping page of type {} of size {}", pageHeader.getType(), compressedPageSize);
          break;
      }
    }
  }

  private Statistics convertStatistics(
      String createdBy,
      PrimitiveType type,
      org.apache.parquet.format.Statistics pageStatistics,
      ColumnIndex columnIndex,
      int pageIndex,
      ParquetMetadataConverter converter) throws IOException {
    if (columnIndex != null) {
      if (columnIndex.getNullPages() == null) {
        throw new IOException("columnIndex has null variable 'nullPages' which indicates corrupted data for type: "
            + type.getName());
      }
      if (pageIndex > columnIndex.getNullPages().size()) {
        throw new IOException("There are more pages " + pageIndex + " found in the column than in the columnIndex "
            + columnIndex.getNullPages().size());
      }
      Statistics.Builder statsBuilder =
          Statistics.getBuilderForReading(type);
      statsBuilder.withNumNulls(columnIndex.getNullCounts().get(pageIndex));

      if (!columnIndex.getNullPages().get(pageIndex)) {
        statsBuilder.withMin(columnIndex.getMinValues().get(pageIndex).array().clone());
        statsBuilder.withMax(columnIndex.getMaxValues().get(pageIndex).array().clone());
      }
      return statsBuilder.build();
    } else if (pageStatistics != null) {
      return converter.fromParquetStatistics(createdBy, pageStatistics, type);
    } else {
      return null;
    }
  }

  private byte[] processPageLoad(
      CompressionConverter.TransParquetFileReader reader,
      boolean isCompressed,
      CompressionCodecFactory.BytesInputCompressor compressor,
      CompressionCodecFactory.BytesInputDecompressor decompressor,
      int payloadLength,
      int rawDataLength,
      boolean encrypt,
      BlockCipher.Encryptor dataEncryptor,
      byte[] add) throws IOException {
    BytesInput data = readBlock(payloadLength, reader);

    // recompress page load
    if (compressor != null) {
      if (isCompressed) {
        data = decompressor.decompress(data, rawDataLength);
      }
      data = compressor.compress(data);
    }

    if (!encrypt) {
      return data.toByteArray();
    }

    // encrypt page load
    return dataEncryptor.encrypt(data.toByteArray(), add);
  }

  public BytesInput readBlock(int length, CompressionConverter.TransParquetFileReader reader) throws IOException {
    byte[] data;
    if (length > pageBufferSize) {
      data = new byte[length];
    } else {
      data = pageBuffer;
    }
    reader.blockRead(data, 0, length);
    return BytesInput.from(data, 0, length);
  }

  public BytesInput readBlockAllocate(int length, CompressionConverter.TransParquetFileReader reader) throws IOException {
    byte[] data = new byte[length];
    reader.blockRead(data, 0, length);
    return BytesInput.from(data, 0, length);
  }

  private int toIntWithCheck(long size) {
    if ((int) size != size) {
      throw new ParquetEncodingException("size is bigger than " + Integer.MAX_VALUE + " bytes: " + size);
    }
    return (int) size;
  }

  private void maskColumn(
      ColumnDescriptor descriptor,
      ColumnChunkMetaData chunk,
      ColumnReadStoreImpl crStore,
      ParquetFileWriter writer,
      MessageType schema,
      CompressionCodecName newCodecName,
      Binary maskValue) throws IOException {

    long totalChunkValues = chunk.getValueCount();
    ColumnReader cReader = crStore.getColumnReader(descriptor);

    ParquetProperties.WriterVersion writerVersion = chunk.getEncodingStats().usesV2Pages()
        ? ParquetProperties.WriterVersion.PARQUET_2_0 : ParquetProperties.WriterVersion.PARQUET_1_0;
    ParquetProperties props = ParquetProperties.builder()
        .withWriterVersion(writerVersion)
        .build();
    CodecFactory codecFactory = new CodecFactory(new Configuration(), props.getPageSizeThreshold());
    CodecFactory.BytesCompressor compressor = codecFactory.getCompressor(newCodecName);

    // Create new schema that only has the current column
    MessageType newSchema = newSchema(schema, descriptor);
    ColumnChunkPageWriteStore cPageStore = new ColumnChunkPageWriteStore(
        compressor, newSchema, props.getAllocator(), props.getColumnIndexTruncateLength(),
        props.getPageWriteChecksumEnabled(), null, numBlocksRewritten);
    ColumnWriteStore cStore = props.newColumnWriteStore(newSchema, cPageStore);
    ColumnWriter cWriter = cStore.getColumnWriter(descriptor);

    for (int i = 0; i < totalChunkValues; i++) {
      int rlvl = cReader.getCurrentRepetitionLevel();
      int dlvl = cReader.getCurrentDefinitionLevel();
      cWriter.write(maskValue, rlvl, dlvl);
      cStore.endRecord();
    }

    cStore.flush();
    cPageStore.flushToFileWriter(writer);

    cStore.close();
    cWriter.close();
  }

  private void addNullColumn(
      ColumnDescriptor descriptor,
      long totalChunkValues,
      EncodingStats encodingStats,
      ParquetFileWriter writer,
      MessageType schema,
      CompressionCodecName newCodecName) throws IOException {

    ParquetProperties.WriterVersion writerVersion = encodingStats.usesV2Pages()
        ? ParquetProperties.WriterVersion.PARQUET_2_0 : ParquetProperties.WriterVersion.PARQUET_1_0;
    ParquetProperties props = ParquetProperties.builder()
        .withWriterVersion(writerVersion)
        .build();
    CodecFactory codecFactory = new CodecFactory(new Configuration(), props.getPageSizeThreshold());
    CodecFactory.BytesCompressor compressor = codecFactory.getCompressor(newCodecName);

    // Create new schema that only has the current column
    MessageType newSchema = newSchema(schema, descriptor);
    ColumnChunkPageWriteStore cPageStore = new ColumnChunkPageWriteStore(
        compressor,
        newSchema,
        props.getAllocator(),
        props.getColumnIndexTruncateLength(),
        props.getPageWriteChecksumEnabled(),
        null,
        numBlocksRewritten);
    ColumnWriteStore cStore = props.newColumnWriteStore(newSchema, cPageStore);
    ColumnWriter cWriter = cStore.getColumnWriter(descriptor);
    int dMax = descriptor.getMaxDefinitionLevel();

    for (int i = 0; i < totalChunkValues; i++) {
      int rlvl = 0;
      int dlvl = 0;
      if (dlvl == dMax) {
        // since we checked ether optional or repeated, dlvl should be > 0
        if (dlvl == 0) {
          throw new IOException("definition level is detected to be 0 for column "
              + Arrays.stream(descriptor.getPath()).collect(Collectors.joining(".")) + " to be nullified");
        }
        // we just write one null for the whole list at the top level,
        // instead of nullify the elements in the list one by one
        if (rlvl == 0) {
          cWriter.writeNull(rlvl, dlvl - 1);
        }
      } else {
        cWriter.writeNull(rlvl, dlvl); // 因为repeatition level没有重复所以后面都是以0在第一层，definition level是字段path的第0层
      }
      cStore.endRecord();
    }

    cStore.flush();
    cPageStore.flushToFileWriter(writer);

    cStore.close();
    cWriter.close();
  }

  private List<ColumnDescriptor> missedColumns(MessageType requiredSchema, MessageType fileSchema) {
    return requiredSchema.getColumns().stream()
        .filter(col -> !fileSchema.containsPath(col.getPath()))
        .collect(Collectors.toList());
  }

  private MessageType newSchema(MessageType schema, ColumnDescriptor descriptor) {
    String[] path = descriptor.getPath();
    Type type = schema.getType(path);
    if (path.length == 1) {
      return new MessageType(schema.getName(), type);
    }

    for (Type field : schema.getFields()) {
      if (!field.isPrimitive() && path[0].equals(field.getName())) {
        Type newType = extractField(field.asGroupType(), type);
        if (newType != null) {
          return new MessageType(schema.getName(), newType);
        }
      }
    }

    // We should never hit this because 'type' is returned by schema.getType().
    throw new RuntimeException("No field is found");
  }

  private Type extractField(GroupType candidate, Type targetField) {
    if (targetField.equals(candidate)) {
      return targetField;
    }

    // In case 'type' is a descendants of candidate
    for (Type field : candidate.asGroupType().getFields()) {
      if (field.isPrimitive()) {
        if (field.equals(targetField)) {
          return new GroupType(candidate.getRepetition(), candidate.getName(), targetField);
        }
      } else {
        Type tempField = extractField(field.asGroupType(), targetField);
        if (tempField != null) {
          return new GroupType(candidate.getRepetition(),candidate.getName(),tempField);
        }
      }
    }

    return null;
  }

  @VisibleForTesting
  public boolean convertLegacy3LevelArray(String[] path) {
    boolean changed = false;
    
    // For schema evolution, also check for legacy patterns even if the field doesn't exist in the schema
    for (int i = 0; i < path.length - 2; i++) {
      if ("bag".equals(path[i + 1]) && "array_element".equals(path[i + 2])) {
        // Convert from xxx.bag.array_element to xxx.list.element
        path[i + 1] = "list";
        path[i + 2] = "element";
        changed = true;
        break;
      }
    }
    
    if (!changed) {
      for (int i = 0; i < path.length; i++) {
        try {
          String[] subPath = Arrays.copyOf(path, i + 1);
          Type type = this.requiredSchema.getType(subPath);
          if (type.getOriginalType() == LIST && i + 1 < path.length && "bag".equals(path[i + 1])) {
            // Convert from xxx.bag.array to xxx.list.element
            path[i + 1] = "list";
            path[i + 2] = "element";
            changed = true;
          }
        } catch (InvalidRecordException e) {
          log.debug("field not found due to schema evolution, nothing need to do");
        }
      }
    }
    return changed;
  }

  public boolean convertLegacyMap(String[] path) {
    boolean changed = false;
    for (int i = 0; i < path.length; i++) {
      try {
        String[] subPath = Arrays.copyOf(path, i + 1);
        Type type = this.requiredSchema.getType(subPath);
        if (type.getOriginalType() == MAP && "map".equals(path[i + 1])) {
          // Convert legacy mode map
          // xxx.map.key to xxx.key_value.key
          // xxx.map.value to xxx.key_value.value
          path[i + 1] = "key_value";
          changed = true;
        }
      } catch (Throwable e) {
        log.debug("field not found due to schema evolution, nothing need to do");
      }
    }
    return changed;
  }

  private static final class DummyGroupConverter extends GroupConverter {
    @Override
    public void start() {
    }

    @Override
    public void end() {
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      return new DummyConverter();
    }
  }

  private static final class DummyConverter extends PrimitiveConverter {
    @Override
    public GroupConverter asGroupConverter() {
      return new DummyGroupConverter();
    }
  }
}