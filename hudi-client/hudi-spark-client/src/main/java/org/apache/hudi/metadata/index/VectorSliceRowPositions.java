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

package org.apache.hudi.metadata.index;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.core.io.storage.HoodieIOFactory;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.keygen.factory.HoodieAvroKeyGeneratorFactory;

import org.apache.spark.sql.catalyst.InternalRow;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.common.table.read.buffer.PositionBasedFileGroupRecordBuffer.ROW_INDEX_TEMPORARY_COLUMN_NAME;

/**
 * File-absolute base-file row positions for vector index reads, following the pattern the record
 * and secondary indexes use: positions are never taken from a merged (base + log) read.
 *
 * <ul>
 *   <li>Slices without log files read the row index through the file group reader. The reader adds
 *       no merge fields there, so the requested row-index column keeps its ordinal.</li>
 *   <li>Slices with log files are merged by key and do not request the row index; base-row
 *       positions come from the base file itself via
 *       {@code FileFormatUtils#fetchRecordKeysWithPositions}, as {@code HoodieKeyLocationFetchHandle}
 *       does for the simple index.</li>
 * </ul>
 */
final class VectorSliceRowPositions {

  private VectorSliceRowPositions() {
  }

  /** Whether the file group reader may merge by position and return the row-index column. */
  static boolean readsRowIndex(FileSlice fileSlice) {
    return !fileSlice.hasLogFiles();
  }

  /** The requested schema for {@code fileSlice}: the row-index column only when it is safe to read. */
  static HoodieSchema requestedSchema(FileSlice fileSlice, HoodieSchema withRowIndex, HoodieSchema withoutRowIndex) {
    return readsRowIndex(fileSlice) ? withRowIndex : withoutRowIndex;
  }

  /**
   * Base-file positions by record key for a slice with log files; empty when the reader supplies
   * the row index itself or the slice has no base file.
   */
  static Map<String, Long> basePositions(
      HoodieTableMetaClient metaClient, TypedProperties keyGeneratorProps, String partitionPath, FileSlice fileSlice) {
    if (readsRowIndex(fileSlice) || !fileSlice.getBaseFile().isPresent()) {
      return Collections.emptyMap();
    }
    HoodieBaseFile baseFile = fileSlice.getBaseFile().get();
    Map<String, Long> positions = new HashMap<>();
    try (ClosableIterator<Pair<HoodieKey, Long>> keys = HoodieIOFactory.getIOFactory(metaClient.getStorage())
        .getFileFormatUtils(baseFile.getStoragePath())
        .fetchRecordKeysWithPositions(metaClient.getStorage(), baseFile.getStoragePath(),
            keyGenerator(metaClient, keyGeneratorProps), Option.of(partitionPath))) {
      keys.forEachRemaining(entry -> positions.put(entry.getLeft().getRecordKey(), entry.getRight()));
    }
    return positions;
  }

  /**
   * The base-file position of {@code record}, or {@code -1} when it has none (it lives only in log
   * files).
   */
  static long rowPosition(
      HoodieReaderContext<InternalRow> readerContext,
      InternalRow record,
      HoodieSchema requestedSchema,
      String recordKey,
      FileSlice fileSlice,
      Map<String, Long> basePositions) {
    if (readsRowIndex(fileSlice)) {
      return readerContext.getRecordContext().extractRecordPosition(
          record, requestedSchema, ROW_INDEX_TEMPORARY_COLUMN_NAME, -1L);
    }
    return basePositions.getOrDefault(recordKey, -1L);
  }

  // Virtual-key tables need the key generator to read record keys from the base file; mirrors the
  // simple index's key generator selection.
  private static Option<BaseKeyGenerator> keyGenerator(HoodieTableMetaClient metaClient, TypedProperties props) {
    if (metaClient.getTableConfig().populateMetaFields()) {
      return Option.empty();
    }
    try {
      return Option.of((BaseKeyGenerator) HoodieAvroKeyGeneratorFactory.createKeyGenerator(props));
    } catch (IOException e) {
      throw new HoodieIOException("KeyGenerator instantiation failed", e);
    }
  }
}
