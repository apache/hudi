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

package org.apache.parquet.hadoop;

import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptySet;

/**
 * Internal implementation of {@link ColumnIndexStore}.
 */
class HoodieColumnIndexStoreImpl implements ColumnIndexStore {

  private interface IndexStore {
    ColumnIndex getColumnIndex();

    OffsetIndex getOffsetIndex();
  }

  private class IndexStoreImpl implements IndexStore {
    private final ColumnChunkMetaData meta;
    private ColumnIndex columnIndex;
    private boolean columnIndexRead;
    private final OffsetIndex offsetIndex;

    IndexStoreImpl(ColumnChunkMetaData meta) {
      this.meta = meta;
      OffsetIndex oi;
      try {
        oi = reader.readOffsetIndex(meta);
      } catch (IOException e) {
        // If the I/O issue still stands it will fail the reading later;
        // otherwise we fail the filtering only with a missing offset index.
        LOGGER.warn("Unable to read offset index for column {}", meta.getPath(), e);
        oi = null;
      }
      if (oi == null) {
        throw new MissingOffsetIndexException(meta.getPath());
      }
      offsetIndex = oi;
    }

    @Override
    public ColumnIndex getColumnIndex() {
      if (!columnIndexRead) {
        try {
          columnIndex = reader.readColumnIndex(meta);
        } catch (IOException e) {
          // If the I/O issue still stands it will fail the reading later;
          // otherwise we fail the filtering only with a missing column index.
          LOGGER.warn("Unable to read column index for column {}", meta.getPath(), e);
        }
        columnIndexRead = true;
      }
      return columnIndex;
    }

    @Override
    public OffsetIndex getOffsetIndex() {
      return offsetIndex;
    }
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(HoodieColumnIndexStoreImpl.class);
  // Used for columns are not in this parquet file
  private static final IndexStore MISSING_INDEX_STORE = new IndexStore() {
    @Override
    public ColumnIndex getColumnIndex() {
      return null;
    }

    @Override
    public OffsetIndex getOffsetIndex() {
      return null;
    }
  };
  private static final HoodieColumnIndexStoreImpl EMPTY = new HoodieColumnIndexStoreImpl(null, new BlockMetaData(), emptySet()) {
    @Override
    public ColumnIndex getColumnIndex(ColumnPath column) {
      return null;
    }

    @Override
    public OffsetIndex getOffsetIndex(ColumnPath column) {
      throw new MissingOffsetIndexException(column);
    }
  };

  private final HoodieParquetFileReader reader;
  private final Map<ColumnPath, IndexStore> store;

  /*
   * Creates a column index store which lazily reads column/offset indexes for the columns in paths. (paths are the set
   * of columns used for the projection)
   */
  static ColumnIndexStore create(HoodieParquetFileReader reader, BlockMetaData block, Set<ColumnPath> paths) {
    try {
      return new HoodieColumnIndexStoreImpl(reader, block, paths);
    } catch (MissingOffsetIndexException e) {
      return EMPTY;
    }
  }

  private HoodieColumnIndexStoreImpl(HoodieParquetFileReader reader, BlockMetaData block, Set<ColumnPath> paths) {
    // TODO[GS]: Offset index for every paths will be required; pre-read the consecutive ones at once?
    // TODO[GS]: Pre-read column index based on filter?
    this.reader = reader;
    Map<ColumnPath, IndexStore> store = new HashMap<>();
    for (ColumnChunkMetaData column : block.getColumns()) {
      ColumnPath path = column.getPath();
      if (paths.contains(path)) {
        store.put(path, new IndexStoreImpl(column));
      }
    }
    this.store = store;
  }

  @Override
  public ColumnIndex getColumnIndex(ColumnPath column) {
    return store.getOrDefault(column, MISSING_INDEX_STORE).getColumnIndex();
  }

  @Override
  public OffsetIndex getOffsetIndex(ColumnPath column) {
    return store.getOrDefault(column, MISSING_INDEX_STORE).getOffsetIndex();
  }
}
