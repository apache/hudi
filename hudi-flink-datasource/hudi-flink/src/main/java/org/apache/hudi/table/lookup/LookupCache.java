/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table.lookup;

import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * Cache abstraction for lookup join dimension table data.
 *
 * <p>Implementations may store data on-heap (e.g. {@link HeapLookupCache}) or off-heap
 * (e.g. {@link RocksDBLookupCache}) to avoid JVM OutOfMemoryError on large tables.
 */
public interface LookupCache extends Closeable {

  /**
   * Adds a single row to the cache under the given lookup key.
   * Multiple rows may be associated with the same key.
   *
   * @param key   the lookup key row (contains only the join key fields)
   * @param row   the full dimension table row
   * @throws IOException if the write fails
   */
  void addRow(RowData key, RowData row) throws IOException;

  /**
   * Returns all rows matching the given lookup key, or {@code null} / empty list if none exist.
   *
   * @param key the lookup key row
   * @return matching rows, or {@code null} if not found
   */
  @Nullable
  List<RowData> getRows(RowData key) throws IOException;

  /**
   * Clears all entries from the cache so it can be reloaded.
   */
  void clear() throws IOException;
}
