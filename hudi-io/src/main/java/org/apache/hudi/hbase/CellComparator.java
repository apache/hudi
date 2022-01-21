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

import java.nio.ByteBuffer;
import java.util.Comparator;
import org.apache.hudi.hbase.util.ByteBufferUtils;
import org.apache.hudi.hbase.util.Bytes;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Comparator for comparing cells and has some specialized methods that allows comparing individual
 * cell components like row, family, qualifier and timestamp
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface CellComparator extends Comparator<Cell> {
  /**
   * A comparator for ordering cells in user-space tables. Useful when writing cells in sorted
   * order as necessary for bulk import (i.e. via MapReduce).
   * <p>
   * CAUTION: This comparator may provide inaccurate ordering for cells from system tables,
   * and should not be relied upon in that case.
   */
  // For internal use, see CellComparatorImpl utility methods.
  static CellComparator getInstance() {
    return CellComparatorImpl.COMPARATOR;
  }

  /**
   * Lexographically compares two cells. The key part of the cell is taken for comparison which
   * includes row, family, qualifier, timestamp and type
   * @param leftCell the left hand side cell
   * @param rightCell the right hand side cell
   * @return greater than 0 if leftCell is bigger, less than 0 if rightCell is bigger, 0 if both
   *         cells are equal
   */
  @Override
  int compare(Cell leftCell, Cell rightCell);

  /**
   * Compare cells.
   * @param ignoreSequenceid True if we are to compare the key portion only and ignore
   *    the sequenceid. Set to false to compare key and consider sequenceid.
   * @return 0 if equal, -1 if a &lt; b, and +1 if a &gt; b.
   */
  int compare(Cell leftCell, Cell rightCell, boolean ignoreSequenceid);

  /**
   * Lexographically compares the rows of two cells.
   * @param leftCell the left hand side cell
   * @param rightCell the right hand side cell
   * @return greater than 0 if leftCell is bigger, less than 0 if rightCell is bigger, 0 if both
   *         cells are equal
   */
  int compareRows(Cell leftCell, Cell rightCell);

  /**
   * Compares the row part of the cell with a simple plain byte[] like the
   * stopRow in Scan.
   * @param cell the cell
   * @param bytes the byte[] representing the row to be compared with
   * @param offset the offset of the byte[]
   * @param length the length of the byte[]
   * @return greater than 0 if leftCell is bigger, less than 0 if rightCell is bigger, 0 if both
   *         cells are equal
   */
  int compareRows(Cell cell, byte[] bytes, int offset, int length);

  /**
   * Compares two row bytes
   * @param leftRow the byte array of the left row
   * @param rightRow the byte array of the right row
   * @return greater than 0 if leftRow is bigger, less than 0 if rightRow is bigger, 0 if both
   *         rows are equal
   */
  default int compareRows(byte[] leftRow, byte[] rightRow) {
    return Bytes.compareTo(leftRow, rightRow);
  }

  /**
   * @param row ByteBuffer that wraps a row; will read from current position and will reading all
   *            remaining; will not disturb the ByteBuffer internal state.
   * @return greater than 0 if leftCell is bigger, less than 0 if rightCell is bigger, 0 if both
   *         cells are equal
   */
  default int compareRows(ByteBuffer row, Cell cell) {
    if (cell instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.compareTo(row, row.position(), row.remaining(),
          ((ByteBufferExtendedCell) cell).getRowByteBuffer(),
          ((ByteBufferExtendedCell) cell).getRowPosition(),
          cell.getRowLength());
    }
    return ByteBufferUtils.compareTo(row, row.position(), row.remaining(),
        cell.getRowArray(), cell.getRowOffset(),
        cell.getRowLength());
  }

  /**
   * Lexographically compares the two cells excluding the row part. It compares family, qualifier,
   * timestamp and the type
   * @param leftCell the left hand side cell
   * @param rightCell the right hand side cell
   * @return greater than 0 if leftCell is bigger, less than 0 if rightCell is bigger, 0 if both
   *         cells are equal
   */
  int compareWithoutRow(Cell leftCell, Cell rightCell);

  /**
   * Lexographically compares the families of the two cells
   * @param leftCell the left hand side cell
   * @param rightCell the right hand side cell
   * @return greater than 0 if leftCell is bigger, less than 0 if rightCell is bigger, 0 if both
   *         cells are equal
   */
  int compareFamilies(Cell leftCell, Cell rightCell);

  /**
   * Lexographically compares the qualifiers of the two cells
   * @param leftCell the left hand side cell
   * @param rightCell the right hand side cell
   * @return greater than 0 if leftCell is bigger, less than 0 if rightCell is bigger, 0 if both
   *         cells are equal
   */
  int compareQualifiers(Cell leftCell, Cell rightCell);

  /**
   * Compares cell's timestamps in DESCENDING order. The below older timestamps sorting ahead of
   * newer timestamps looks wrong but it is intentional. This way, newer timestamps are first found
   * when we iterate over a memstore and newer versions are the first we trip over when reading from
   * a store file.
   * @param leftCell the left hand side cell
   * @param rightCell the right hand side cell
   * @return 1 if left's timestamp &lt; right's timestamp -1 if left's timestamp &gt; right's
   *         timestamp 0 if both timestamps are equal
   */
  int compareTimestamps(Cell leftCell, Cell rightCell);

  /**
   * Compares cell's timestamps in DESCENDING order. The below older timestamps sorting ahead of
   * newer timestamps looks wrong but it is intentional. This way, newer timestamps are first found
   * when we iterate over a memstore and newer versions are the first we trip over when reading from
   * a store file.
   * @param leftCellts the left cell's timestamp
   * @param rightCellts the right cell's timestamp
   * @return 1 if left's timestamp &lt; right's timestamp -1 if left's timestamp &gt; right's
   *         timestamp 0 if both timestamps are equal
   */
  int compareTimestamps(long leftCellts, long rightCellts);

  /**
   * @return A dumbed-down, fast comparator for hbase2 base-type, the {@link ByteBufferKeyValue}.
   *   Create an instance when you make a new memstore, when you know only BBKVs will be passed.
   *   Do not pollute with types other than BBKV if can be helped; the Comparator will slow.
   */
  Comparator getSimpleComparator();
}

