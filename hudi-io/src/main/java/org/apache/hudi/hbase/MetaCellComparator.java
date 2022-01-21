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

import org.apache.hbase.thirdparty.com.google.common.primitives.Longs;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * A {@link CellComparatorImpl} for <code>hbase:meta</code> catalog table
 * {@link KeyValue}s.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class MetaCellComparator extends CellComparatorImpl {

  /**
   * A {@link MetaCellComparator} for <code>hbase:meta</code> catalog table
   * {@link KeyValue}s.
   */
  public static final MetaCellComparator META_COMPARATOR = new MetaCellComparator();

  // TODO: Do we need a ByteBufferKeyValue version of this?
  @Override
  public int compareRows(final Cell left, final Cell right) {
    return compareRows(left.getRowArray(), left.getRowOffset(), left.getRowLength(),
        right.getRowArray(), right.getRowOffset(), right.getRowLength());
  }

  @Override
  public int compareRows(Cell left, byte[] right, int roffset, int rlength) {
    return compareRows(left.getRowArray(), left.getRowOffset(), left.getRowLength(), right, roffset,
        rlength);
  }

  @Override
  public int compareRows(byte[] leftRow, byte[] rightRow) {
    return compareRows(leftRow, 0, leftRow.length, rightRow, 0, rightRow.length);
  }

  @Override
  public int compare(final Cell a, final Cell b, boolean ignoreSequenceid) {
    int diff = compareRows(a, b);
    if (diff != 0) {
      return diff;
    }

    diff = compareWithoutRow(a, b);
    if (diff != 0) {
      return diff;
    }

    // Negate following comparisons so later edits show up first mvccVersion: later sorts first
    return ignoreSequenceid ? diff : Longs.compare(b.getSequenceId(), a.getSequenceId());
  }

  private static int compareRows(byte[] left, int loffset, int llength, byte[] right, int roffset,
                                 int rlength) {
    int leftDelimiter = Bytes.searchDelimiterIndex(left, loffset, llength, HConstants.DELIMITER);
    int rightDelimiter = Bytes.searchDelimiterIndex(right, roffset, rlength, HConstants.DELIMITER);
    // Compare up to the delimiter
    int lpart = (leftDelimiter < 0 ? llength : leftDelimiter - loffset);
    int rpart = (rightDelimiter < 0 ? rlength : rightDelimiter - roffset);
    int result = Bytes.compareTo(left, loffset, lpart, right, roffset, rpart);
    if (result != 0) {
      return result;
    } else {
      if (leftDelimiter < 0 && rightDelimiter >= 0) {
        return -1;
      } else if (rightDelimiter < 0 && leftDelimiter >= 0) {
        return 1;
      } else if (leftDelimiter < 0) {
        return 0;
      }
    }
    // Compare middle bit of the row.
    // Move past delimiter
    leftDelimiter++;
    rightDelimiter++;
    int leftFarDelimiter = Bytes
        .searchDelimiterIndexInReverse(left, leftDelimiter, llength - (leftDelimiter - loffset),
            HConstants.DELIMITER);
    int rightFarDelimiter = Bytes
        .searchDelimiterIndexInReverse(right, rightDelimiter, rlength - (rightDelimiter - roffset),
            HConstants.DELIMITER);
    // Now compare middlesection of row.
    lpart = (leftFarDelimiter < 0 ? llength + loffset : leftFarDelimiter) - leftDelimiter;
    rpart = (rightFarDelimiter < 0 ? rlength + roffset : rightFarDelimiter) - rightDelimiter;
    result = Bytes.compareTo(left, leftDelimiter, lpart, right, rightDelimiter, rpart);
    if (result != 0) {
      return result;
    } else {
      if (leftDelimiter < 0 && rightDelimiter >= 0) {
        return -1;
      } else if (rightDelimiter < 0 && leftDelimiter >= 0) {
        return 1;
      } else if (leftDelimiter < 0) {
        return 0;
      }
    }
    // Compare last part of row, the rowid.
    leftFarDelimiter++;
    rightFarDelimiter++;
    result = Bytes.compareTo(left, leftFarDelimiter, llength - (leftFarDelimiter - loffset), right,
        rightFarDelimiter, rlength - (rightFarDelimiter - roffset));
    return result;
  }

  @Override
  public int compareRows(ByteBuffer row, Cell cell) {
    byte[] array;
    int offset;
    int len = row.remaining();
    if (row.hasArray()) {
      array = row.array();
      offset = row.position() + row.arrayOffset();
    } else {
      // We copy the row array if offheap just so we can do a compare. We do this elsewhere too
      // in BBUtils when Cell is backed by an offheap ByteBuffer. Needs fixing so no copy. TODO.
      array = new byte[len];
      offset = 0;
      ByteBufferUtils.copyFromBufferToArray(array, row, row.position(), 0, len);
    }
    // Reverse result since we swap the order of the params we pass below.
    return -compareRows(cell, array, offset, len);
  }

  @Override
  public Comparator getSimpleComparator() {
    return this;
  }

}
