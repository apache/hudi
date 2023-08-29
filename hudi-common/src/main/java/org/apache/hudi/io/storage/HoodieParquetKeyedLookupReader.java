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

package org.apache.hudi.io.storage;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.io.storage.parquet.ParquetFileMetadataLoader;
import org.apache.hudi.io.storage.parquet.RowGroup;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.util.CompressionConverter;
import org.apache.parquet.hadoop.util.HadoopCodecs;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static org.apache.parquet.column.ValuesType.DEFINITION_LEVEL;
import static org.apache.parquet.column.ValuesType.REPETITION_LEVEL;
import static org.apache.parquet.column.ValuesType.VALUES;

/**
 * Implements an efficient lookup for a key in a Parquet file, by using page level statistics, and bloom filters.
 * Parquet file is expected to have two columns :
 * 1. `key` binary column, which is the column to be used for lookup
 * 2. `value` binary column, which is the column to be returned as a result of lookup as well as being sorted by `key` column.
 */
public class HoodieParquetKeyedLookupReader {
  private static Logger LOG = LoggerFactory.getLogger(HoodieParquetKeyedLookupReader.class);
  private static String KEY = "key";
  private static String VALUE = "value";

  private final Configuration conf;
  private final InputFile parquetFile;
  private final ParquetFileMetadataLoader metadataLoader;
  private final CompressionCodecFactory codecFactory;
  private final ParquetMetadataConverter converter;

  public HoodieParquetKeyedLookupReader(Configuration conf, InputFile parquetFile) throws Exception {
    this.conf = conf;
    this.parquetFile = parquetFile;
    this.metadataLoader = new ParquetFileMetadataLoader(
        parquetFile, ParquetFileMetadataLoader.Options.builder().enableLoadBloomFilters().build());
    this.codecFactory = HadoopCodecs.newFactory(0);
    this.converter = new ParquetMetadataConverter();

    metadataLoader.load();
  }

  /**
   * Returns a mapping between requested keys and their corresponding values if exists.
   * <p>
   * @Param keys  A set of sorted keys.
   * @Return A mapping between the provided keys and their corresponding values.
   */
  public Map<String, Option<String>> lookup(SortedSet<String> keys) throws Exception {
    Map<String, Option<String>> keyToValue = new HashMap<>();
    try (CompressionConverter.TransParquetFileReader reader = new CompressionConverter.TransParquetFileReader(
        parquetFile, HadoopReadOptions.builder(conf).build())) {
      Map<String, String> matchingRecords = getMatchingRecords(reader, new LinkedList<>(keys));
      for (String key: keys) {
        if (matchingRecords.containsKey(key)) {
          keyToValue.put(key, Option.of(matchingRecords.get(key)));
        } else {
          keyToValue.put(key, Option.empty());
        }
      }
    }
    return keyToValue;
  }

  private Map<String, String> getMatchingRecords(CompressionConverter.TransParquetFileReader reader,
                                                 Queue<String> keys) throws Exception {
    Map<String, String> keyToValue = new HashMap<>();
    for (RowGroup rowGroup : metadataLoader.getRowGroups()) {
      final int keyColNumber = searchColumn(rowGroup, KEY);
      if (keyColNumber < 0) {
        throw new IllegalArgumentException("Cannot find key column in schema.");
      }

      // Skip row group using bloom filter if possible.
      if (shouldSkip(rowGroup, keyColNumber, new LinkedList<>(keys))) {
        continue;
      }

      ArrayList<Pair<String, Long>> keyPositions = lookupKeyColumnChunk(reader, rowGroup, keyColNumber, keys);
      // Now fetch the respective values out.
      final int valueColNumber = searchColumn(rowGroup, VALUE);
      if (valueColNumber < 0) {
        throw new IllegalArgumentException("Cannot find value column in schema.");
      }
      if (!keyPositions.isEmpty()) {
        keyToValue.putAll(fetchFromValueColumnChunk(reader, rowGroup, valueColNumber, keyPositions));
      }
      LOG.debug("Done with rowGroup");
    }
    return keyToValue;
  }

  public static boolean shouldSkip(RowGroup rowGroup, int colNumber, Queue<String> keys) {
    if (rowGroup.getBloomFilters().isEmpty()) {
      return false;
    }

    BloomFilter bloomFilter = null;
    try {
      bloomFilter = rowGroup.getBloomFilters().get(colNumber);
    } catch (Exception e) {
      LOG.warn("Can not load the bloom filter correctly.", e);
      return false;
    }

    // Bloom filter is not found for this column; can not skip.
    if (bloomFilter == null) {
      return false;
    }

    while (!keys.isEmpty()) {
      String key = keys.poll();
      Binary binary = Binary.fromString(key);
      long hash = bloomFilter.hash(binary);
      // At least one key is found; can not skip.
      if (bloomFilter.findHash(hash)) {
        return false;
      }
    }

    // No keys are found in the filter; skip.
    return true;
  }

  private int searchColumn(RowGroup rowGroup, String columnDotPath) {
    final int numColumns = rowGroup.getBlockMetaData().getColumns().size();
    for (int i = 0; i < numColumns; i++) {
      if (rowGroup.getBlockMetaData().getColumns().get(i).getPath().toDotString().equals(columnDotPath)) {
        return i;
      }
    }
    return -1;
  }

  private ArrayList<Pair<String, Long>> lookupKeyColumnChunk(CompressionConverter.TransParquetFileReader reader,
                                                             RowGroup rowGroup,
                                                             int colNumber,
                                                             Queue<String> keys) throws IOException {
    ColumnChunkMetaData chunkMetaData = rowGroup.getBlockMetaData().getColumns().get(colNumber);
    ColumnIndex columnIndex = rowGroup.getColumnIndices().get(colNumber);
    OffsetIndex pageLocation = rowGroup.getPageLocations().get(colNumber);
    CompressionCodecFactory.BytesInputDecompressor decompressor = codecFactory.getDecompressor(chunkMetaData.getCodec());
    ColumnPath columnPath = rowGroup.getBlockMetaData().getColumns().get(colNumber).getPath();
    ColumnDescriptor columnDescriptor = metadataLoader.getFileMetaData().getSchema().getColumnDescription(columnPath.toArray());
    PrimitiveType type = metadataLoader.getFileMetaData().getSchema().getType(columnDescriptor.getPath()).asPrimitiveType();

    AtomicInteger pageCounter = new AtomicInteger(0);
    int totalPages = pageLocation.getPageCount();
    ArrayList<Pair<String, Long>> keyAndPositions = new ArrayList<>();
    for (int pageIndex = 0; pageIndex < totalPages; pageIndex++) {
      String pageMinKey = new String(columnIndex.getMinValues().get(pageIndex).array(), StandardCharsets.UTF_8);
      String pageMaxKey = new String(columnIndex.getMaxValues().get(pageIndex).array(), StandardCharsets.UTF_8);

      // Skip keys if the page's min is greater than the key.
      while (keys.peek() != null && keys.peek().compareTo(pageMinKey) < 0) {
        keys.poll();
      }

      // Found all the keys we need.
      if (keys.peek() == null) {
        break;
      }

      if (keys.peek().compareTo(pageMaxKey) > 0) {
        continue;
      }

      // Read the page header.
      pageCounter.getAndIncrement();
      reader.setStreamPosition(pageLocation.getOffset(pageIndex));
      PageHeader pageHeader = reader.readPageHeader();
      // Read the page out.
      DataPageHeader headerV1 = pageHeader.data_page_header;
      byte[] pageData = new byte[pageHeader.compressed_page_size];
      reader.blockRead(pageData, 0, pageHeader.compressed_page_size);
      BytesInput pageDataInput = decompressor.decompress(BytesInput.from(pageData), pageHeader.uncompressed_page_size);
      // Search for keys.
      processBinaryDataPageV1Values(columnDescriptor, pageLocation, pageIndex, headerV1, pageHeader, type, pageDataInput, (key, pos) -> {
        if (keys.peek() != null && key.equals(keys.peek())) {
          keyAndPositions.add(Pair.of(key, pos));
          keys.poll();
        }
      });
    }

    LOG.debug("Read " + pageCounter.get() + " key pages out of " + totalPages + " pages");
    return keyAndPositions;
  }

  private Map<String, String> fetchFromValueColumnChunk(CompressionConverter.TransParquetFileReader reader,
                                                        RowGroup rowGroup,
                                                        int colNumber,
                                                        ArrayList<Pair<String, Long>> keyPositions) throws IOException {
    ColumnChunkMetaData chunkMetaData = rowGroup.getBlockMetaData().getColumns().get(colNumber);
    OffsetIndex pageLocation = rowGroup.getPageLocations().get(colNumber);
    CompressionCodecFactory.BytesInputDecompressor decompressor = codecFactory.getDecompressor(chunkMetaData.getCodec());
    ColumnPath columnPath = rowGroup.getBlockMetaData().getColumns().get(colNumber).getPath();
    ColumnDescriptor cd = metadataLoader.getFileMetaData().getSchema().getColumnDescription(columnPath.toArray());
    PrimitiveType type = metadataLoader.getFileMetaData().getSchema().getType(cd.getPath()).asPrimitiveType();

    // Iterate over matching keys, in order, and find the next page that could contain the record position.
    AtomicInteger recordIndex = new AtomicInteger(0);
    AtomicInteger pageCounter = new AtomicInteger(0);
    int totalPages = pageLocation.getPageCount();
    Map<String, String> keyToValue = new HashMap<>();
    for (int p = 0; p < pageLocation.getPageCount(); p++) {
      long pageLastRecordPos = pageLocation.getLastRowIndex(p, rowGroup.getBlockMetaData().getRowCount());

      if (recordIndex.get() >= keyPositions.size()) {
        break;
      }
      if (pageLastRecordPos < keyPositions.get(recordIndex.get()).getRight()) {
        continue;
      }

      pageCounter.getAndIncrement();
      reader.setStreamPosition(pageLocation.getOffset(p));
      PageHeader pageHeader = reader.readPageHeader();
      DataPageHeader headerV1 = pageHeader.data_page_header;
      byte[] pageData = new byte[pageHeader.compressed_page_size];
      reader.blockRead(pageData, 0, pageHeader.compressed_page_size);
      BytesInput pageDataInput = decompressor.decompress(BytesInput.from(pageData), pageHeader.uncompressed_page_size);

      processBinaryDataPageV1Values(cd, pageLocation, p, headerV1, pageHeader, type, pageDataInput, (value, pos) -> {
        if (recordIndex.get() < keyPositions.size() && Objects.equals(pos, keyPositions.get(recordIndex.get()).getRight())) {
          keyToValue.put(keyPositions.get(recordIndex.get()).getLeft(), value);
          recordIndex.getAndIncrement();
        }
      });
    }
    LOG.debug(String.format("Read %d value pages out of %d pages", pageCounter.get(), totalPages));
    return keyToValue;
  }

  private void processBinaryDataPageV1Values(ColumnDescriptor cd,
                                             OffsetIndex pageLocation,
                                             int pageIndex,
                                             DataPageHeader dataPageHeader,
                                             PageHeader pageHeader,
                                             PrimitiveType type,
                                             BytesInput pageDataInput,
                                             BiConsumer<String, Long> valueAndPositionConsumeFn) throws IOException {
    DataPageV1 pageV1 = new DataPageV1(
        pageDataInput,
        dataPageHeader.num_values,
        pageHeader.uncompressed_page_size,
        converter.fromParquetStatistics(metadataLoader.getFileMetaData().getCreatedBy(),
            dataPageHeader.getStatistics(),
            type
        ),
        converter.getEncoding(dataPageHeader.getRepetition_level_encoding()),
        converter.getEncoding(dataPageHeader.getDefinition_level_encoding()),
        converter.getEncoding(dataPageHeader.getEncoding())
    );
    ValuesReader rleReader = pageV1.getRlEncoding().getValuesReader(cd, REPETITION_LEVEL);
    ValuesReader dleReader = pageV1.getDlEncoding().getValuesReader(cd, DEFINITION_LEVEL);
    ValuesReader dataReader = pageV1.getValueEncoding().getValuesReader(cd, VALUES);

    ByteBufferInputStream inputStream = pageV1.getBytes().toInputStream();
    rleReader.initFromPage(pageV1.getValueCount(), inputStream);
    dleReader.initFromPage(pageV1.getValueCount(), inputStream);
    dataReader.initFromPage(pageV1.getValueCount(), inputStream);

    for (int i = 0; i < pageV1.getValueCount(); i++) {
      String key = dataReader.readBytes().toStringUsingUTF8();
      valueAndPositionConsumeFn.accept(key, pageLocation.getFirstRowIndex(pageIndex) + i);
    }
  }
}

