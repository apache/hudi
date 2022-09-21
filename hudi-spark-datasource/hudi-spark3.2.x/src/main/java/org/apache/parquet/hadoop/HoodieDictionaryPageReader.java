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

import org.apache.hudi.common.util.Option;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.DictionaryPageReadStore;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.io.ParquetDecodingException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A {@link DictionaryPageReadStore} implementation that reads dictionaries from
 * an open {@link ParquetFileReader}.
 *
 * This implementation will delegate dictionary reads to a
 * {@link HoodieColumnChunkPageReadStore} to avoid extra reads after a row group has
 * been loaded into memory.
 */
class HoodieDictionaryPageReader implements DictionaryPageReadStore {

  private final HoodieParquetFileReader reader;
  private final Map<String, ColumnChunkMetaData> columns;
  private final Map<String, Option<DictionaryPage>> dictionaryPageCache;
  private HoodieColumnChunkPageReadStore rowGroup = null;

  /**
   * Instantiate a new HoodieDictionaryPageReader.
   *
   * @param reader The target ParquetFileReader
   * @param block The target BlockMetaData
   *
   * @throws NullPointerException if {@code reader} or {@code block} is
   *           {@code null}
   */
  HoodieDictionaryPageReader(HoodieParquetFileReader reader, BlockMetaData block) {
    this.reader = Objects.requireNonNull(reader);
    this.columns = new HashMap<>();
    this.dictionaryPageCache = new ConcurrentHashMap<>();

    for (ColumnChunkMetaData column : block.getColumns()) {
      columns.put(column.getPath().toDotString(), column);
    }
  }

  /**
   * Sets this reader's row group's page store. When a row group is set, this
   * reader will delegate to that row group to return dictionary pages. This
   * avoids seeking and re-reading dictionary bytes after this reader's row
   * group is loaded into memory.
   *
   * @param rowGroup a HoodieColumnChunkPageReadStore for this reader's row group
   */
  void setRowGroup(HoodieColumnChunkPageReadStore rowGroup) {
    this.rowGroup = rowGroup;
  }

  @Override
  public DictionaryPage readDictionaryPage(ColumnDescriptor descriptor) {
    if (rowGroup != null) {
      // if the row group has already been read, use that dictionary
      return rowGroup.readDictionaryPage(descriptor);
    }

    String dotPath = String.join(".", descriptor.getPath());
    ColumnChunkMetaData column = columns.get(dotPath);
    if (column == null) {
      throw new ParquetDecodingException(
          "Failed to load dictionary, unknown column: " + dotPath);
    }

    return dictionaryPageCache.computeIfAbsent(dotPath, key -> {
      try {
        final DictionaryPage dict =
            column.hasDictionaryPage() ? reader.readDictionary(column) : null;

        // Copy the dictionary to ensure it can be reused if it is returned
        // more than once. This can happen when a DictionaryFilter has two or
        // more predicates for the same column. Cache misses as well.
        return (dict != null) ? Option.of(reusableCopy(dict)) : Option.empty();
      } catch (IOException e) {
        throw new ParquetDecodingException("Failed to read dictionary", e);
      }
    }).orElse(null);
  }

  private static DictionaryPage reusableCopy(DictionaryPage dict)
      throws IOException {
    return new DictionaryPage(BytesInput.from(dict.getBytes().toByteArray()),
        dict.getDictionarySize(), dict.getEncoding());
  }
}
