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

import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.hadoop.metadata.BlockMetaData;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A POJO class that contains four kinds of metadata for a
 * row group in a Parquet file.
 * <ul>
 *     <li>BlockMetaData, e.g., row count, block size.</li>
 *     <li>ColumnIndex, e.g., min, max of a column.</li>
 *     <li>OffsetIndex, e.g., offset for a data page in a column.</li>
 *     <li>BloomFilter, e.g., bloom filters for a colum.</li>
 * </ul>
 */
public class RowGroup {
  private final BlockMetaData blockMetaData;
  private final List<ColumnIndex> columnIndices;
  private final List<OffsetIndex> pageLocations;
  private List<BloomFilter> bloomFilters;

  public RowGroup(BlockMetaData blockMetaData) {
    this.blockMetaData = blockMetaData;
    this.columnIndices = new ArrayList<ColumnIndex>(Collections.nCopies(blockMetaData.getColumns().size(), null));
    this.pageLocations = new ArrayList<OffsetIndex>(Collections.nCopies(blockMetaData.getColumns().size(), null));
    this.bloomFilters = Collections.<BloomFilter>emptyList();
  }

  /**
   * Add a {@link BloomFilter} object for a column.
   *
   * @param chunkId The id of the column.
   * @param bloomFilter The {@link BloomFilter} object.
   */
  public void addBloomFilter(int chunkId, BloomFilter bloomFilter) {
    if (bloomFilters.isEmpty()) {
      bloomFilters = new ArrayList<BloomFilter>(Collections.nCopies(blockMetaData.getColumns().size(), null));
    }
    bloomFilters.add(chunkId, bloomFilter);
  }

  /**
   * @return the metadata for the row group {@link BlockMetaData} that is fetched from the Parquet Footer.
   */
  public BlockMetaData getBlockMetaData() {
    return blockMetaData;
  }

  /**
   * @return the page level stats for a column {@link ColumnIndex} that is fetched from the offsets present in the Parquet Footer.
   */
  public List<ColumnIndex> getColumnIndices() {
    return columnIndices;
  }

  /**
   * @return the page location (row index) for a column {@link OffsetIndex} that is fetched from the offsets present in the Parquet Footer.
   */
  public List<OffsetIndex> getPageLocations() {
    return pageLocations;
  }

  /**
   * @return the Bloom filters for a column {@link BloomFilter} that is fetched from the offsets present in the Parquet Footer.
   */
  public List<BloomFilter> getBloomFilters() {
    return bloomFilters;
  }
}
