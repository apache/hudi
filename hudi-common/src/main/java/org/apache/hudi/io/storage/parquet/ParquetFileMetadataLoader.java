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

package org.apache.hudi.io.storage.parquet;

import org.apache.hudi.common.util.io.HeapSeekableInputStream;

import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.hadoop.metadata.IndexReference;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.SeekableInputStream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * Loads metadata of a Parquet file into memory, which includes
 * (1) the Parquet file metadata from footer,
 * (2) the column index metadata for all row groups and column chunks,
 * (3) the page index metadata for all row groups and column chunks,
 * (4) the bloom headers for all row groups and column chunks,
 * (5) Optionally the bloom filters for all row groups and column chunks.
 */
public class ParquetFileMetadataLoader {
  private static final long BLOOM_FILTER_HEADER_SIZE_GUESS = 1024; // 1KB
  private static final long BLOOM_FILTER_SIZE_GUESS = 1024 * 1024; // 1 MB

  private final ParquetMetadataFileReader metadataFileReader;
  private final Options options;
  private final List<RowGroup> rowGroups;
  private FileMetaData fileMetaData;

  public ParquetFileMetadataLoader(InputFile file, Options options) throws IOException {
    this.metadataFileReader = new ParquetMetadataFileReader(file, ParquetReadOptions.builder().build());
    this.options = options;
    this.rowGroups = new ArrayList<>();
  }

  public Options getOptions() {
    return options;
  }

  public FileMetaData getFileMetaData() {
    return fileMetaData;
  }

  public List<RowGroup> getRowGroups() {
    return rowGroups;
  }

  public Iterator<RowGroup> rowGroups() {
    return rowGroups.iterator();
  }

  /**
   * Read four different metadata into memory for a Parquet file.
   * 1. metadata for the file, e.g., schema, version.
   * 2. metadata for row groups, e.g., block metadata, column index.
   * 3. index metadata grouped by continuous disk range for efficient disk access.
   */
  public void load() {
    if (fileMetaData == null) {
      loadFileMetaData();
    }

    if (rowGroups.isEmpty()) {
      loadRowGroupMetaData();
    }

    try {
      loadIndexMetadata();
    } catch (IOException e) {
      throw new ParquetDecodingException("Unable to read the index metadata", e);
    }
  }

  private void loadFileMetaData() {
    fileMetaData = metadataFileReader.getFileMetaData();
  }

  private void loadRowGroupMetaData() {
    metadataFileReader.getRowGroups().forEach(e -> rowGroups.add(new RowGroup(e)));
  }

  private void loadIndexMetadata() throws IOException {
    List<ContinuousRange> ranges = new ArrayList<>();
    for (RowGroup rowGroup : rowGroups) {
      ranges = generateRangesForRowGroup(rowGroup, ranges);
    }
    for (ContinuousRange range : ranges) {
      addIndexMetadataToRowGroups(range);
    }
  }

  private List<ContinuousRange> generateRangesForRowGroup(RowGroup rowGroup, List<ContinuousRange> ranges) {
    BlockMetaData blockMetaData = rowGroup.getBlockMetaData();
    List<ColumnChunkMetaData> columnsInOrder = blockMetaData.getColumns();
    int rowGroupId = rowGroups.indexOf(rowGroup);
    for (int chunkId = 0; chunkId < columnsInOrder.size(); ++chunkId) {
      ColumnChunkMetaData chunk = columnsInOrder.get(chunkId);
      ranges = generateRangesForColumn(chunk, ranges, chunkId, rowGroupId);
    }
    return ranges;
  }

  private List<ContinuousRange> generateRangesForColumn(
      ColumnChunkMetaData chunk, List<ContinuousRange> ranges, int chunkId, int rowGroupId) {
    ranges = generateRangeForColumnIndex(chunk, ranges, chunkId, rowGroupId);
    ranges = generateRangeForOffSetIndex(chunk, ranges, chunkId, rowGroupId);
    ranges = generateRangeForBloomFilterIndex(chunk, ranges, chunkId, rowGroupId);
    return ranges;
  }

  private List<ContinuousRange> generateRangeForColumnIndex(
      ColumnChunkMetaData chunk, List<ContinuousRange> ranges, int chunkId, int rowGroupId) {
    IndexReference ref = chunk.getColumnIndexReference();
    if (ref != null) {
      ColumnId columnId = new ColumnId(rowGroupId, chunkId, ColumnId.ChunkType.COLUMN_INDEX);
      ranges = ContinuousRange.insertAndMergeRanges(ranges, columnId, ref.getOffset(), ref.getOffset() + ref.getLength());
    }
    return ranges;
  }

  private List<ContinuousRange> generateRangeForOffSetIndex(
      ColumnChunkMetaData chunk, List<ContinuousRange> ranges, int chunkId, int rowGroupId) {
    IndexReference ref = chunk.getOffsetIndexReference();
    if (ref != null) {
      ColumnId columnId = new ColumnId(rowGroupId, chunkId, ColumnId.ChunkType.PAGE_INDEX);
      ranges = ContinuousRange.insertAndMergeRanges(ranges, columnId, ref.getOffset(), ref.getOffset() + ref.getLength());
    }
    return ranges;
  }

  private List<ContinuousRange> generateRangeForBloomFilterIndex(
      ColumnChunkMetaData chunk, List<ContinuousRange> ranges, int chunkId, int rowGroupId) {
    long bloomFilterOffset = chunk.getBloomFilterOffset();
    if (options.isLoadBloomFiltersEnabled() && bloomFilterOffset >= 0) {
      ColumnId columnId = new ColumnId(rowGroupId, chunkId, ColumnId.ChunkType.BLOOM_FILTER);
      ranges = ContinuousRange.insertAndMergeRanges(
          ranges, columnId, bloomFilterOffset, bloomFilterOffset + BLOOM_FILTER_HEADER_SIZE_GUESS + BLOOM_FILTER_SIZE_GUESS);
    }
    return ranges;
  }

  private void addIndexMetadataToRowGroups(ContinuousRange range) throws IOException {
    // Overflow can happen due to overestimated bloom filter size.
    range.endOffset = Math.min(range.endOffset, metadataFileReader.getInputFile().getLength());
    if (range.endOffset < range.startOffset) {
      throw new IllegalStateException(
          "Metadata range has higher startOffset " + range.startOffset + " than endOffset " + range.endOffset);
    }

    ByteBuffer metadataCache;
    metadataFileReader.setStreamPosition(range.startOffset);
    metadataCache = ByteBuffer.allocate((int) (range.endOffset - range.startOffset));
    metadataFileReader.blockRead(metadataCache);
    metadataCache.flip();

    try (SeekableInputStream indexBytesStream = HeapSeekableInputStream.wrap(metadataCache.array())) {
      for (ColumnId columnId : range.columnIds) {
        addIndexMetadata(columnId, indexBytesStream, range);
      }
    }
  }

  private void addIndexMetadata(ColumnId columnId, SeekableInputStream indexBytesStream, ContinuousRange range) throws IOException {
    ColumnChunkMetaData columnChunkMetaData = rowGroups
        .get(columnId.rowGroupId)
        .getBlockMetaData()
        .getColumns()
        .get(columnId.columnChunkId);
    RowGroup rowGroup = rowGroups.get(columnId.rowGroupId);
    int columnChunkId = columnId.columnChunkId;
    switch (columnId.chunkType) {
      case COLUMN_INDEX:
        appendColumnIndex(rowGroup, indexBytesStream, range, columnChunkMetaData, columnChunkId);
        break;
      case PAGE_INDEX:
        appendPageLocation(rowGroup, indexBytesStream, range, columnChunkMetaData, columnChunkId);
        break;
      case BLOOM_FILTER:
        appendBloomFilter(rowGroup, indexBytesStream, range, columnChunkMetaData, columnChunkId);
        break;
      default:
        throw new ParquetDecodingException("Not a valid chunk type " + columnId.chunkType);
    }
  }

  private void appendColumnIndex(RowGroup rowGroup,
                                SeekableInputStream indexBytesStream,
                                ContinuousRange range,
                                ColumnChunkMetaData columnChunkMetaData,
                                int columnChunkId) throws IOException {
    ColumnIndex columnIndex = metadataFileReader.readColumnIndex(
        metadataFileReader, indexBytesStream, range.startOffset, range.endOffset, columnChunkMetaData);
    if (columnIndex != null) {
      rowGroup.getColumnIndices().add(columnChunkId, columnIndex);
    }
  }

  private void appendPageLocation(RowGroup rowGroup,
                                 SeekableInputStream indexBytesStream,
                                 ContinuousRange range,
                                 ColumnChunkMetaData columnChunkMetaData,
                                 int columnChunkId) throws IOException {
    OffsetIndex offsetIndex = metadataFileReader.readOffsetIndex(
        metadataFileReader, indexBytesStream, range.startOffset, range.endOffset, columnChunkMetaData);
    if (offsetIndex != null) {
      rowGroup.getPageLocations().add(columnChunkId, offsetIndex);
    }
  }

  private void appendBloomFilter(RowGroup rowGroup,
                                SeekableInputStream indexBytesStream,
                                ContinuousRange range,
                                ColumnChunkMetaData columnChunkMetaData,
                                int columnChunkId) throws IOException {
    BloomFilter bloomFilter = metadataFileReader.readBloomFilter(
        metadataFileReader, indexBytesStream, range.startOffset, range.endOffset, columnChunkMetaData);
    if (bloomFilter != null) {
      rowGroup.addBloomFilter(columnChunkId, bloomFilter);
    }
  }

  // Internal use only
  public static class Options {
    private static final boolean LOAD_BLOOM_FILTERS_ENABLED_DEFAULT = false;

    private final boolean enableLoadBloomFilters;

    public Options(boolean enableLoadBloomFilters) {
      this.enableLoadBloomFilters = enableLoadBloomFilters;
    }

    public static Builder builder() {
      return new Builder();
    }

    public boolean isLoadBloomFiltersEnabled() {
      return enableLoadBloomFilters;
    }

    public static class Builder {
      protected boolean enableLoadBloomFilters = LOAD_BLOOM_FILTERS_ENABLED_DEFAULT;

      public Options.Builder enableLoadBloomFilters() {
        this.enableLoadBloomFilters = true;
        return this;
      }

      public Options build() {
        return new Options(enableLoadBloomFilters);
      }
    }
  }

  // Internal use only
  private static class ContinuousRange {
    private final List<ColumnId> columnIds;
    private final long startOffset;
    private long endOffset;

    public ContinuousRange(ColumnId columnId, long startOffset, long endOffset) {
      columnIds = new ArrayList<>();
      columnIds.add(columnId);
      this.startOffset = startOffset;
      this.endOffset = endOffset;
    }

    public static List<ContinuousRange> insertAndMergeRanges(
        List<ContinuousRange> ranges, ColumnId columnId, long startOffset, long endOffset) {
      // Insert.
      ContinuousRange inputRange = new ContinuousRange(columnId, startOffset, endOffset);
      ranges.add(inputRange);

      // TODO: Avoid sorting everytime.
      // Sort.
      ranges.sort(Comparator.comparingLong(a -> a.startOffset));

      // Merge.
      return mergeRanges(ranges);
    }

    public static List<ContinuousRange> mergeRanges(List<ContinuousRange> ranges) {
      List<ContinuousRange> mergedRanges = new ArrayList<>();

      ContinuousRange prevRange = ranges.get(0);
      for (int i = 1; i < ranges.size(); i++) {
        ContinuousRange currentRange = ranges.get(i);
        if (currentRange.startOffset <= prevRange.endOffset) {
          prevRange.endOffset = Math.max(prevRange.endOffset, currentRange.endOffset);
          prevRange.columnIds.addAll(currentRange.columnIds);
        } else {
          mergedRanges.add(prevRange);
          prevRange = currentRange;
        }
      }

      mergedRanges.add(prevRange);
      return mergedRanges;
    }

    @Override
    public String toString() {
      return String.format(
          "{ ContinuousRange startRange: %d endRange: %d columnIds: %s }",
          startOffset,
          endOffset,
          columnIds);
    }
  }

  private static class ColumnId {
    private final int rowGroupId;
    private final int columnChunkId;
    private final ChunkType chunkType;

    public ColumnId(int rowGroupId, int columnChunkId, ChunkType chunkType) {
      this.rowGroupId = rowGroupId;
      this.columnChunkId = columnChunkId;
      this.chunkType = chunkType;
    }

    public enum ChunkType {
      COLUMN_INDEX,
      PAGE_INDEX,
      BLOOM_FILTER
    }

    @Override
    public String toString() {
      return String.format(
          "{ ColumnId: rowId: %d, columnChunkId: %d, chunkType %s }",
          rowGroupId,
          columnChunkId,
          chunkType.name());
    }
  }
}

