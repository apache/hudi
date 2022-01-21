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

package org.apache.hudi.hbase;

import java.io.IOException;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * An interface for iterating through a sequence of cells. Similar to Java's Iterator, but without
 * the hasNext() or remove() methods. The hasNext() method is problematic because it may require
 * actually loading the next object, which in turn requires storing the previous object somewhere.
 *
 * <p>The core data block decoder should be as fast as possible, so we push the complexity and
 * performance expense of concurrently tracking multiple cells to layers above the CellScanner.
 * <p>
 * The {@link #current()} method will return a reference to a Cell implementation. This reference
 * may or may not point to a reusable cell implementation, so users of the CellScanner should not,
 * for example, accumulate a List of Cells. All of the references may point to the same object,
 * which would be the latest state of the underlying Cell. In short, the Cell is mutable.
 * </p>
 * Typical usage:
 *
 * <pre>
 * while (scanner.advance()) {
 *   Cell cell = scanner.current();
 *   // do something
 * }
 * </pre>
 * <p>Often used reading {@link org.apache.hadoop.hbase.Cell}s written by
 * {@link org.apache.hadoop.hbase.io.CellOutputStream}.
 */
@InterfaceAudience.Public
public interface CellScanner {
  /**
   * @return the current Cell which may be mutable
   */
  Cell current();

  /**
   * Advance the scanner 1 cell.
   * @return true if the next cell is found and {@link #current()} will return a valid Cell
   * @throws IOException if advancing the scanner fails
   */
  boolean advance() throws IOException;
}
