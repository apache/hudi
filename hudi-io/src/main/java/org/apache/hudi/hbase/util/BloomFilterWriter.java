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

package org.apache.hudi.hbase.util;

import org.apache.hudi.hbase.Cell;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hudi.hbase.regionserver.CellSink;
import org.apache.hudi.hbase.regionserver.ShipperListener;
import org.apache.hadoop.io.Writable;

/**
 * Specifies methods needed to add elements to a Bloom filter and serialize the
 * resulting Bloom filter as a sequence of bytes.
 */
@InterfaceAudience.Private
public interface BloomFilterWriter extends BloomFilterBase, CellSink, ShipperListener {

  /** Compact the Bloom filter before writing metadata &amp; data to disk. */
  void compactBloom();
  /**
   * Get a writable interface into bloom filter meta data.
   *
   * @return a writable instance that can be later written to a stream
   */
  Writable getMetaWriter();

  /**
   * Get a writable interface into bloom filter data (the actual Bloom bits).
   * Not used for compound Bloom filters.
   *
   * @return a writable instance that can be later written to a stream
   */
  Writable getDataWriter();

  /**
   * Returns the previous cell written by this writer
   * @return the previous cell
   */
  Cell getPrevCell();
}
