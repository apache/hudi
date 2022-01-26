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

package org.apache.hudi.hbase.regionserver;

import java.io.IOException;

import org.apache.hudi.hbase.Cell;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hudi.hbase.util.BloomFilterWriter;

/**
 * A sink of cells that allows appending cells to the Writers that implement it.
 * {@link org.apache.hudi.hbase.io.hfile.HFile.Writer},
 * {@link StoreFileWriter}, {@link AbstractMultiFileWriter},
 * {@link BloomFilterWriter} are some implementors of this.
 */
@InterfaceAudience.Private
public interface CellSink {
  /**
   * Append the given cell
   * @param cell the cell to be added
   * @throws IOException
   */
  void append(Cell cell) throws IOException;
}
