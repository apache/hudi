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

package org.apache.hudi.common.index.vector.search;

import java.util.Iterator;

/**
 * A Hudi read handle for projected, position-based base-file reads (RFC-109 §9). Given a
 * {@link VectorFetchTask}, returns only the record-key and vector columns as {@link VectorRecord}s
 * — no full-row materialization. SERVE rows are read by row position (page-index skipping); STALE
 * rows fall back to a key-based lookup within the same file.
 *
 * <p>The interface is engine- and format-neutral: implementations group positions by row group and
 * page, cache footer/page-index metadata, coalesce neighboring ranges, use bounded concurrency, and
 * never expose storage-specific (GCS/S3) APIs to the vector-search layer. The Parquet implementation
 * is the first; the same shape supports ORC and is reusable for record-index point reads.
 */
public interface HoodieVectorBatchReadHandle extends AutoCloseable {

  /**
   * Read the requested rows of one file, decoding only {@code recordKeyField} and {@code vectorColumn}.
   *
   * @param task          the per-file fetch task (base file + row requests)
   * @param recordKeyField the record-key column name
   * @param vectorColumn  the vector column name
   * @return an iterator of decoded {@link VectorRecord}s (record key + vector + live location)
   */
  Iterator<VectorRecord> read(VectorFetchTask task, String recordKeyField, String vectorColumn);
}
