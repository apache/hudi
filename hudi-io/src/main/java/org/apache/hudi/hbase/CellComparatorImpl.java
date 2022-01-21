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

import java.util.Comparator;
import org.apache.hudi.hbase.KeyValue.Type;
import org.apache.hudi.hbase.util.ByteBufferUtils;
import org.apache.hudi.hbase.util.Bytes;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Compare two HBase cells.  Do not use this method comparing <code>-ROOT-</code> or
 * <code>hbase:meta</code> cells.  Cells from these tables need a specialized comparator, one that
 * takes account of the special formatting of the row where we have commas to delimit table from
 * regionname, from row.  See KeyValue for how it has a special comparator to do hbase:meta cells
 * and yet another for -ROOT-.
 * <p>While using this comparator for {{@link #compareRows(Cell, Cell)} et al, the hbase:meta cells
 * format should be taken into consideration, for which the instance of this comparator
 * should be used.  In all other cases the static APIs in this comparator would be enough
 * <p>HOT methods. We spend a good portion of CPU comparing. Anything that makes the compare
 * faster will likely manifest at the macro level. See also
 * {@link BBKVComparator}. Use it when mostly {@link ByteBufferKeyValue}s.
 * </p>
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class CellComparatorImpl implements CellComparator {

  /**
   * Comparator for plain key/values; i.e. non-catalog table key/values. Works on Key portion
   * of KeyValue only.
   */
  public static final CellComparatorImpl COMPARATOR = new CellComparatorImpl();

  @Override
  public final int compare(final Cell a, final Cell b) {
    return compare(a, b, false);
  }

  @Override
  public int compare(final Cell l, final Cell r, boolean ignoreSequenceid) {
    int diff = 0;
    // "Peel off" the most common path.
    if (l instanceof KeyValue && r instanceof KeyValue) {
      diff = compareKeyValues((KeyValue) l, (KeyValue) r);
      if (diff != 0) {
        return diff;
      }
    } else if (l instanceof KeyValue && r instanceof ByteBufferKeyValue) {
      diff = compareKVVsBBKV((KeyValue) l, (ByteBufferKeyValue) r);
      if (diff != 0) {
        return diff;
      }
    } else if (l instanceof ByteBufferKeyValue && r instanceof KeyValue) {
      diff = compareKVVsBBKV((KeyValue) r, (ByteBufferKeyValue) l);
      if (diff != 0) {
        // negate- Findbugs will complain?
        return -diff;
      }
    } else if (l instanceof ByteBufferKeyValue && r instanceof ByteBufferKeyValue) {
      diff = compareBBKV((ByteBufferKeyValue) l, (ByteBufferKeyValue) r);
      if (diff != 0) {
        return diff;
      }
    } else {
      int leftRowLength = l.getRowLength();
      int rightRowLength = r.getRowLength();
      diff = compareRows(l, leftRowLength, r, rightRowLength);
      if (diff != 0) {
        return diff;
      }

      diff = compareWithoutRow(l, r);
      if (diff != 0) {
        return diff;
      }
    }
    // Negate following comparisons so later edits show up first mvccVersion: later sorts first
    return ignoreSequenceid ? diff : Long.compare(r.getSequenceId(), l.getSequenceId());
  }

  private static int compareKeyValues(final KeyValue left, final KeyValue right) {
    int diff;
    // Compare Rows. Cache row length.
    int leftRowLength = left.getRowLength();
    int rightRowLength = right.getRowLength();
    diff = Bytes.compareTo(left.getRowArray(), left.getRowOffset(), leftRowLength,
        right.getRowArray(), right.getRowOffset(), rightRowLength);
    if (diff != 0) {
      return diff;
    }

    // If the column is not specified, the "minimum" key type appears as latest in the sorted
    // order, regardless of the timestamp. This is used for specifying the last key/value in a
    // given row, because there is no "lexicographically last column" (it would be infinitely
    // long).
    // The "maximum" key type does not need this behavior. Copied from KeyValue. This is bad in
    // that
    // we can't do memcmp w/ special rules like this.
    // TODO: Is there a test for this behavior?
    int leftFamilyLengthPosition = left.getFamilyLengthPosition(leftRowLength);
    int leftFamilyLength = left.getFamilyLength(leftFamilyLengthPosition);
    int leftKeyLength = left.getKeyLength();
    int leftQualifierLength =
        left.getQualifierLength(leftKeyLength, leftRowLength, leftFamilyLength);

    // No need of left row length below here.

    byte leftType = left.getTypeByte(leftKeyLength);
    if (leftType == KeyValue.Type.Minimum.getCode()
        && leftFamilyLength + leftQualifierLength == 0) {
      // left is "bigger", i.e. it appears later in the sorted order
      return 1;
    }

    int rightFamilyLengthPosition = right.getFamilyLengthPosition(rightRowLength);
    int rightFamilyLength = right.getFamilyLength(rightFamilyLengthPosition);
    int rightKeyLength = right.getKeyLength();
    int rightQualifierLength =
        right.getQualifierLength(rightKeyLength, rightRowLength, rightFamilyLength);

    // No need of right row length below here.

    byte rightType = right.getTypeByte(rightKeyLength);
    if (rightType == KeyValue.Type.Minimum.getCode()
        && rightFamilyLength + rightQualifierLength == 0) {
      return -1;
    }

    // Compare families.
    int leftFamilyPosition = left.getFamilyOffset(leftFamilyLengthPosition);
    int rightFamilyPosition = right.getFamilyOffset(rightFamilyLengthPosition);
    diff = Bytes.compareTo(left.getFamilyArray(), leftFamilyPosition, leftFamilyLength,
        right.getFamilyArray(), rightFamilyPosition, rightFamilyLength);
    if (diff != 0) {
      return diff;
    }

    // Compare qualifiers
    diff = Bytes.compareTo(left.getQualifierArray(),
        left.getQualifierOffset(leftFamilyPosition, leftFamilyLength), leftQualifierLength,
        right.getQualifierArray(), right.getQualifierOffset(rightFamilyPosition, rightFamilyLength),
        rightQualifierLength);
    if (diff != 0) {
      return diff;
    }

    // Timestamps.
    // Swap order we pass into compare so we get DESCENDING order.
    // TODO : Ensure we read the bytes and do the compare instead of the value.
    diff = Long.compare(right.getTimestamp(rightKeyLength), left.getTimestamp(leftKeyLength));
    if (diff != 0) {
      return diff;
    }

    // Compare types. Let the delete types sort ahead of puts; i.e. types
    // of higher numbers sort before those of lesser numbers. Maximum (255)
    // appears ahead of everything, and minimum (0) appears after
    // everything.
    return (0xff & rightType) - (0xff & leftType);
  }

  private static int compareBBKV(final ByteBufferKeyValue left, final ByteBufferKeyValue right) {
    int diff;
    // Compare Rows. Cache row length.
    int leftRowLength = left.getRowLength();
    int rightRowLength = right.getRowLength();
    diff = ByteBufferUtils.compareTo(left.getRowByteBuffer(), left.getRowPosition(),
        leftRowLength, right.getRowByteBuffer(), right.getRowPosition(), rightRowLength);
    if (diff != 0) {
      return diff;
    }

    // If the column is not specified, the "minimum" key type appears as latest in the sorted
    // order, regardless of the timestamp. This is used for specifying the last key/value in a
    // given row, because there is no "lexicographically last column" (it would be infinitely
    // long).
    // The "maximum" key type does not need this behavior. Copied from KeyValue. This is bad in
    // that
    // we can't do memcmp w/ special rules like this.
    // TODO: Is there a test for this behavior?
    int leftFamilyLengthPosition = left.getFamilyLengthPosition(leftRowLength);
    int leftFamilyLength = left.getFamilyLength(leftFamilyLengthPosition);
    int leftKeyLength = left.getKeyLength();
    int leftQualifierLength =
        left.getQualifierLength(leftKeyLength, leftRowLength, leftFamilyLength);

    // No need of left row length below here.

    byte leftType = left.getTypeByte(leftKeyLength);
    if (leftType == KeyValue.Type.Minimum.getCode()
        && leftFamilyLength + leftQualifierLength == 0) {
      // left is "bigger", i.e. it appears later in the sorted order
      return 1;
    }

    int rightFamilyLengthPosition = right.getFamilyLengthPosition(rightRowLength);
    int rightFamilyLength = right.getFamilyLength(rightFamilyLengthPosition);
    int rightKeyLength = right.getKeyLength();
    int rightQualifierLength =
        right.getQualifierLength(rightKeyLength, rightRowLength, rightFamilyLength);

    // No need of right row length below here.

    byte rightType = right.getTypeByte(rightKeyLength);
    if (rightType == KeyValue.Type.Minimum.getCode()
        && rightFamilyLength + rightQualifierLength == 0) {
      return -1;
    }

    // Compare families.
    int leftFamilyPosition = left.getFamilyPosition(leftFamilyLengthPosition);
    int rightFamilyPosition = right.getFamilyPosition(rightFamilyLengthPosition);
    diff = ByteBufferUtils.compareTo(left.getFamilyByteBuffer(), leftFamilyPosition,
        leftFamilyLength, right.getFamilyByteBuffer(), rightFamilyPosition, rightFamilyLength);
    if (diff != 0) {
      return diff;
    }

    // Compare qualifiers
    diff = ByteBufferUtils.compareTo(left.getQualifierByteBuffer(),
        left.getQualifierPosition(leftFamilyPosition, leftFamilyLength), leftQualifierLength,
        right.getQualifierByteBuffer(),
        right.getQualifierPosition(rightFamilyPosition, rightFamilyLength), rightQualifierLength);
    if (diff != 0) {
      return diff;
    }

    // Timestamps.
    // Swap order we pass into compare so we get DESCENDING order.
    diff = Long.compare(right.getTimestamp(rightKeyLength), left.getTimestamp(leftKeyLength));
    if (diff != 0) {
      return diff;
    }

    // Compare types. Let the delete types sort ahead of puts; i.e. types
    // of higher numbers sort before those of lesser numbers. Maximum (255)
    // appears ahead of everything, and minimum (0) appears after
    // everything.
    return (0xff & rightType) - (0xff & leftType);
  }

  private static int compareKVVsBBKV(final KeyValue left, final ByteBufferKeyValue right) {
    int diff;
    // Compare Rows. Cache row length.
    int leftRowLength = left.getRowLength();
    int rightRowLength = right.getRowLength();
    diff = ByteBufferUtils.compareTo(left.getRowArray(), left.getRowOffset(), leftRowLength,
        right.getRowByteBuffer(), right.getRowPosition(), rightRowLength);
    if (diff != 0) {
      return diff;
    }

    // If the column is not specified, the "minimum" key type appears as latest in the sorted
    // order, regardless of the timestamp. This is used for specifying the last key/value in a
    // given row, because there is no "lexicographically last column" (it would be infinitely
    // long).
    // The "maximum" key type does not need this behavior. Copied from KeyValue. This is bad in
    // that
    // we can't do memcmp w/ special rules like this.
    // TODO: Is there a test for this behavior?
    int leftFamilyLengthPosition = left.getFamilyLengthPosition(leftRowLength);
    int leftFamilyLength = left.getFamilyLength(leftFamilyLengthPosition);
    int leftKeyLength = left.getKeyLength();
    int leftQualifierLength =
        left.getQualifierLength(leftKeyLength, leftRowLength, leftFamilyLength);

    // No need of left row length below here.

    byte leftType = left.getTypeByte(leftKeyLength);
    if (leftType == KeyValue.Type.Minimum.getCode()
        && leftFamilyLength + leftQualifierLength == 0) {
      // left is "bigger", i.e. it appears later in the sorted order
      return 1;
    }

    int rightFamilyLengthPosition = right.getFamilyLengthPosition(rightRowLength);
    int rightFamilyLength = right.getFamilyLength(rightFamilyLengthPosition);
    int rightKeyLength = right.getKeyLength();
    int rightQualifierLength =
        right.getQualifierLength(rightKeyLength, rightRowLength, rightFamilyLength);

    // No need of right row length below here.

    byte rightType = right.getTypeByte(rightKeyLength);
    if (rightType == KeyValue.Type.Minimum.getCode()
        && rightFamilyLength + rightQualifierLength == 0) {
      return -1;
    }

    // Compare families.
    int leftFamilyPosition = left.getFamilyOffset(leftFamilyLengthPosition);
    int rightFamilyPosition = right.getFamilyPosition(rightFamilyLengthPosition);
    diff = ByteBufferUtils.compareTo(left.getFamilyArray(), leftFamilyPosition, leftFamilyLength,
        right.getFamilyByteBuffer(), rightFamilyPosition, rightFamilyLength);
    if (diff != 0) {
      return diff;
    }

    // Compare qualifiers
    diff = ByteBufferUtils.compareTo(left.getQualifierArray(),
        left.getQualifierOffset(leftFamilyPosition, leftFamilyLength), leftQualifierLength,
        right.getQualifierByteBuffer(),
        right.getQualifierPosition(rightFamilyPosition, rightFamilyLength), rightQualifierLength);
    if (diff != 0) {
      return diff;
    }

    // Timestamps.
    // Swap order we pass into compare so we get DESCENDING order.
    diff = Long.compare(right.getTimestamp(rightKeyLength), left.getTimestamp(leftKeyLength));
    if (diff != 0) {
      return diff;
    }

    // Compare types. Let the delete types sort ahead of puts; i.e. types
    // of higher numbers sort before those of lesser numbers. Maximum (255)
    // appears ahead of everything, and minimum (0) appears after
    // everything.
    return (0xff & rightType) - (0xff & leftType);
  }

  /**
   * Compares the family and qualifier part of the cell
   * @return 0 if both cells are equal, 1 if left cell is bigger than right, -1 otherwise
   */
  public final int compareColumns(final Cell left, final Cell right) {
    int diff = compareFamilies(left, right);
    if (diff != 0) {
      return diff;
    }
    return compareQualifiers(left, right);
  }

  private int compareColumns(final Cell left, final int leftFamLen, final int leftQualLen,
                             final Cell right, final int rightFamLen, final int rightQualLen) {
    int diff = compareFamilies(left, leftFamLen, right, rightFamLen);
    if (diff != 0) {
      return diff;
    }
    return compareQualifiers(left, leftQualLen, right, rightQualLen);
  }

  private int compareFamilies(Cell left, int leftFamLen, Cell right, int rightFamLen) {
    if (left instanceof ByteBufferExtendedCell && right instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.compareTo(((ByteBufferExtendedCell) left).getFamilyByteBuffer(),
          ((ByteBufferExtendedCell) left).getFamilyPosition(), leftFamLen,
          ((ByteBufferExtendedCell) right).getFamilyByteBuffer(),
          ((ByteBufferExtendedCell) right).getFamilyPosition(), rightFamLen);
    }
    if (left instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.compareTo(((ByteBufferExtendedCell) left).getFamilyByteBuffer(),
          ((ByteBufferExtendedCell) left).getFamilyPosition(), leftFamLen, right.getFamilyArray(),
          right.getFamilyOffset(), rightFamLen);
    }
    if (right instanceof ByteBufferExtendedCell) {
      // Notice how we flip the order of the compare here. We used to negate the return value but
      // see what FindBugs says
      // http://findbugs.sourceforge.net/bugDescriptions.html#RV_NEGATING_RESULT_OF_COMPARETO
      // It suggest flipping the order to get same effect and 'safer'.
      return ByteBufferUtils.compareTo(left.getFamilyArray(), left.getFamilyOffset(), leftFamLen,
          ((ByteBufferExtendedCell) right).getFamilyByteBuffer(),
          ((ByteBufferExtendedCell) right).getFamilyPosition(), rightFamLen);
    }
    return Bytes.compareTo(left.getFamilyArray(), left.getFamilyOffset(), leftFamLen,
        right.getFamilyArray(), right.getFamilyOffset(), rightFamLen);
  }

  private final int compareQualifiers(Cell left, int leftQualLen, Cell right, int rightQualLen) {
    if (left instanceof ByteBufferExtendedCell && right instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.compareTo(((ByteBufferExtendedCell) left).getQualifierByteBuffer(),
          ((ByteBufferExtendedCell) left).getQualifierPosition(), leftQualLen,
          ((ByteBufferExtendedCell) right).getQualifierByteBuffer(),
          ((ByteBufferExtendedCell) right).getQualifierPosition(), rightQualLen);
    }
    if (left instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.compareTo(((ByteBufferExtendedCell) left).getQualifierByteBuffer(),
          ((ByteBufferExtendedCell) left).getQualifierPosition(), leftQualLen,
          right.getQualifierArray(), right.getQualifierOffset(), rightQualLen);
    }
    if (right instanceof ByteBufferExtendedCell) {
      // Notice how we flip the order of the compare here. We used to negate the return value but
      // see what FindBugs says
      // http://findbugs.sourceforge.net/bugDescriptions.html#RV_NEGATING_RESULT_OF_COMPARETO
      // It suggest flipping the order to get same effect and 'safer'.
      return ByteBufferUtils.compareTo(left.getQualifierArray(), left.getQualifierOffset(),
          leftQualLen, ((ByteBufferExtendedCell) right).getQualifierByteBuffer(),
          ((ByteBufferExtendedCell) right).getQualifierPosition(), rightQualLen);
    }
    return Bytes.compareTo(left.getQualifierArray(), left.getQualifierOffset(), leftQualLen,
        right.getQualifierArray(), right.getQualifierOffset(), rightQualLen);
  }

  /**
   * Compare the families of left and right cell
   * @return 0 if both cells are equal, 1 if left cell is bigger than right, -1 otherwise
   */
  @Override
  public final int compareFamilies(Cell left, Cell right) {
    if (left instanceof ByteBufferExtendedCell && right instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.compareTo(((ByteBufferExtendedCell) left).getFamilyByteBuffer(),
          ((ByteBufferExtendedCell) left).getFamilyPosition(), left.getFamilyLength(),
          ((ByteBufferExtendedCell) right).getFamilyByteBuffer(),
          ((ByteBufferExtendedCell) right).getFamilyPosition(), right.getFamilyLength());
    }
    if (left instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.compareTo(((ByteBufferExtendedCell) left).getFamilyByteBuffer(),
          ((ByteBufferExtendedCell) left).getFamilyPosition(), left.getFamilyLength(),
          right.getFamilyArray(), right.getFamilyOffset(), right.getFamilyLength());
    }
    if (right instanceof ByteBufferExtendedCell) {
      // Notice how we flip the order of the compare here. We used to negate the return value but
      // see what FindBugs says
      // http://findbugs.sourceforge.net/bugDescriptions.html#RV_NEGATING_RESULT_OF_COMPARETO
      // It suggest flipping the order to get same effect and 'safer'.
      return ByteBufferUtils.compareTo(
          left.getFamilyArray(), left.getFamilyOffset(), left.getFamilyLength(),
          ((ByteBufferExtendedCell)right).getFamilyByteBuffer(),
          ((ByteBufferExtendedCell)right).getFamilyPosition(), right.getFamilyLength());
    }
    return Bytes.compareTo(left.getFamilyArray(), left.getFamilyOffset(), left.getFamilyLength(),
        right.getFamilyArray(), right.getFamilyOffset(), right.getFamilyLength());
  }

  static int compareQualifiers(KeyValue left, KeyValue right) {
    // NOTE: Same method is in CellComparatorImpl, also private, not shared, intentionally. Not
    // sharing gets us a few percent more throughput in compares. If changes here or there, make
    // sure done in both places.
    // Compare Rows. Cache row length.
    int leftRowLength = left.getRowLength();
    int rightRowLength = right.getRowLength();

    int leftFamilyLengthPosition = left.getFamilyLengthPosition(leftRowLength);
    byte leftFamilyLength = left.getFamilyLength(leftFamilyLengthPosition);
    int leftKeyLength = left.getKeyLength();
    int leftQualifierLength =
        left.getQualifierLength(leftKeyLength, leftRowLength, leftFamilyLength);

    // No need of left row length below here.

    int rightFamilyLengthPosition = right.getFamilyLengthPosition(rightRowLength);
    byte rightFamilyLength = right.getFamilyLength(rightFamilyLengthPosition);
    int rightKeyLength = right.getKeyLength();
    int rightQualifierLength =
        right.getQualifierLength(rightKeyLength, rightRowLength, rightFamilyLength);

    // Compare families.
    int leftFamilyOffset = left.getFamilyOffset(leftFamilyLengthPosition);
    int rightFamilyOffset = right.getFamilyOffset(rightFamilyLengthPosition);

    // Compare qualifiers
    return Bytes.compareTo(left.getQualifierArray(), leftFamilyOffset + leftFamilyLength,
        leftQualifierLength, right.getQualifierArray(), rightFamilyOffset + rightFamilyLength,
        rightQualifierLength);
  }

  static int compareQualifiers(KeyValue left, ByteBufferKeyValue right) {
    // NOTE: Same method is in CellComparatorImpl, also private, not shared, intentionally. Not
    // sharing gets us a few percent more throughput in compares. If changes here or there, make
    // sure done in both places.
    // Compare Rows. Cache row length.
    int leftRowLength = left.getRowLength();
    int rightRowLength = right.getRowLength();

    int leftFamilyLengthPosition = left.getFamilyLengthPosition(leftRowLength);
    byte leftFamilyLength = left.getFamilyLength(leftFamilyLengthPosition);
    int leftKeyLength = left.getKeyLength();
    int leftQualifierLength =
        left.getQualifierLength(leftKeyLength, leftRowLength, leftFamilyLength);

    // No need of left row length below here.

    int rightFamilyLengthPosition = right.getFamilyLengthPosition(rightRowLength);
    byte rightFamilyLength = right.getFamilyLength(rightFamilyLengthPosition);
    int rightKeyLength = right.getKeyLength();
    int rightQualifierLength =
        right.getQualifierLength(rightKeyLength, rightRowLength, rightFamilyLength);

    // Compare families.
    int leftFamilyOffset = left.getFamilyOffset(leftFamilyLengthPosition);
    int rightFamilyPosition = right.getFamilyPosition(rightFamilyLengthPosition);

    // Compare qualifiers
    return ByteBufferUtils.compareTo(left.getQualifierArray(),
        leftFamilyOffset + leftFamilyLength, leftQualifierLength, right.getQualifierByteBuffer(),
        rightFamilyPosition + rightFamilyLength, rightQualifierLength);
  }

  static int compareQualifiers(ByteBufferKeyValue left, KeyValue right) {
    // NOTE: Same method is in CellComparatorImpl, also private, not shared, intentionally. Not
    // sharing gets us a few percent more throughput in compares. If changes here or there, make
    // sure done in both places.
    // Compare Rows. Cache row length.
    int leftRowLength = left.getRowLength();
    int rightRowLength = right.getRowLength();

    int leftFamilyLengthPosition = left.getFamilyLengthPosition(leftRowLength);
    byte leftFamilyLength = left.getFamilyLength(leftFamilyLengthPosition);
    int leftKeyLength = left.getKeyLength();
    int leftQualifierLength =
        left.getQualifierLength(leftKeyLength, leftRowLength, leftFamilyLength);

    // No need of left row length below here.

    int rightFamilyLengthPosition = right.getFamilyLengthPosition(rightRowLength);
    byte rightFamilyLength = right.getFamilyLength(rightFamilyLengthPosition);
    int rightKeyLength = right.getKeyLength();
    int rightQualifierLength =
        right.getQualifierLength(rightKeyLength, rightRowLength, rightFamilyLength);

    // Compare families.
    int leftFamilyPosition = left.getFamilyPosition(leftFamilyLengthPosition);
    int rightFamilyOffset = right.getFamilyOffset(rightFamilyLengthPosition);

    // Compare qualifiers
    return ByteBufferUtils.compareTo(left.getQualifierByteBuffer(),
        leftFamilyPosition + leftFamilyLength, leftQualifierLength, right.getQualifierArray(),
        rightFamilyOffset + rightFamilyLength, rightQualifierLength);
  }

  static int compareQualifiers(ByteBufferKeyValue left, ByteBufferKeyValue right) {
    // NOTE: Same method is in CellComparatorImpl, also private, not shared, intentionally. Not
    // sharing gets us a few percent more throughput in compares. If changes here or there, make
    // sure done in both places.
    // Compare Rows. Cache row length.
    int leftRowLength = left.getRowLength();
    int rightRowLength = right.getRowLength();

    int leftFamilyLengthPosition = left.getFamilyLengthPosition(leftRowLength);
    byte leftFamilyLength = left.getFamilyLength(leftFamilyLengthPosition);
    int leftKeyLength = left.getKeyLength();
    int leftQualifierLength =
        left.getQualifierLength(leftKeyLength, leftRowLength, leftFamilyLength);

    // No need of left row length below here.

    int rightFamilyLengthPosition = right.getFamilyLengthPosition(rightRowLength);
    byte rightFamilyLength = right.getFamilyLength(rightFamilyLengthPosition);
    int rightKeyLength = right.getKeyLength();
    int rightQualifierLength =
        right.getQualifierLength(rightKeyLength, rightRowLength, rightFamilyLength);

    // Compare families.
    int leftFamilyPosition = left.getFamilyPosition(leftFamilyLengthPosition);
    int rightFamilyPosition = right.getFamilyPosition(rightFamilyLengthPosition);

    // Compare qualifiers
    return ByteBufferUtils.compareTo(left.getQualifierByteBuffer(),
        leftFamilyPosition + leftFamilyLength, leftQualifierLength, right.getQualifierByteBuffer(),
        rightFamilyPosition + rightFamilyLength, rightQualifierLength);
  }

  /**
   * Compare the qualifiers part of the left and right cells.
   * @return 0 if both cells are equal, 1 if left cell is bigger than right, -1 otherwise
   */
  @Override
  public final int compareQualifiers(Cell left, Cell right) {
    if ((left instanceof ByteBufferKeyValue) && (right instanceof ByteBufferKeyValue)) {
      return compareQualifiers((ByteBufferKeyValue) left, (ByteBufferKeyValue) right);
    } else if ((left instanceof KeyValue) && (right instanceof KeyValue)) {
      return compareQualifiers((KeyValue) left, (KeyValue) right);
    } else if ((left instanceof KeyValue) && (right instanceof ByteBufferKeyValue)) {
      return compareQualifiers((KeyValue) left, (ByteBufferKeyValue) right);
    } else if ((left instanceof ByteBufferKeyValue) && (right instanceof KeyValue)) {
      return compareQualifiers((ByteBufferKeyValue) left, (KeyValue) right);
    } else {
      if (left instanceof ByteBufferExtendedCell && right instanceof ByteBufferExtendedCell) {
        return ByteBufferUtils.compareTo(((ByteBufferExtendedCell) left).getQualifierByteBuffer(),
            ((ByteBufferExtendedCell) left).getQualifierPosition(), left.getQualifierLength(),
            ((ByteBufferExtendedCell) right).getQualifierByteBuffer(),
            ((ByteBufferExtendedCell) right).getQualifierPosition(), right.getQualifierLength());
      }
      if (left instanceof ByteBufferExtendedCell) {
        return ByteBufferUtils.compareTo(((ByteBufferExtendedCell) left).getQualifierByteBuffer(),
            ((ByteBufferExtendedCell) left).getQualifierPosition(), left.getQualifierLength(),
            right.getQualifierArray(), right.getQualifierOffset(), right.getQualifierLength());
      }
      if (right instanceof ByteBufferExtendedCell) {
        // Notice how we flip the order of the compare here. We used to negate the return value but
        // see what FindBugs says
        // http://findbugs.sourceforge.net/bugDescriptions.html#RV_NEGATING_RESULT_OF_COMPARETO
        // It suggest flipping the order to get same effect and 'safer'.
        return ByteBufferUtils.compareTo(left.getQualifierArray(), left.getQualifierOffset(),
            left.getQualifierLength(), ((ByteBufferExtendedCell) right).getQualifierByteBuffer(),
            ((ByteBufferExtendedCell) right).getQualifierPosition(), right.getQualifierLength());
      }
      return Bytes.compareTo(left.getQualifierArray(), left.getQualifierOffset(),
          left.getQualifierLength(), right.getQualifierArray(), right.getQualifierOffset(),
          right.getQualifierLength());
    }

  }

  /**
   * Compares the rows of the left and right cell.
   * For the hbase:meta case this method is overridden such that it can handle hbase:meta cells.
   * The caller should ensure using the appropriate comparator for hbase:meta.
   * @return 0 if both cells are equal, 1 if left cell is bigger than right, -1 otherwise
   */
  @Override
  public int compareRows(final Cell left, final Cell right) {
    return compareRows(left, left.getRowLength(), right, right.getRowLength());
  }

  static int compareRows(final Cell left, int leftRowLength, final Cell right, int rightRowLength) {
    // left and right can be exactly the same at the beginning of a row
    if (left == right) {
      return 0;
    }
    if (left instanceof ByteBufferExtendedCell && right instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.compareTo(((ByteBufferExtendedCell) left).getRowByteBuffer(),
          ((ByteBufferExtendedCell) left).getRowPosition(), leftRowLength,
          ((ByteBufferExtendedCell) right).getRowByteBuffer(),
          ((ByteBufferExtendedCell) right).getRowPosition(), rightRowLength);
    }
    if (left instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.compareTo(((ByteBufferExtendedCell) left).getRowByteBuffer(),
          ((ByteBufferExtendedCell) left).getRowPosition(), leftRowLength,
          right.getRowArray(), right.getRowOffset(), rightRowLength);
    }
    if (right instanceof ByteBufferExtendedCell) {
      // Notice how we flip the order of the compare here. We used to negate the return value but
      // see what FindBugs says
      // http://findbugs.sourceforge.net/bugDescriptions.html#RV_NEGATING_RESULT_OF_COMPARETO
      // It suggest flipping the order to get same effect and 'safer'.
      return ByteBufferUtils.compareTo(left.getRowArray(), left.getRowOffset(), leftRowLength,
          ((ByteBufferExtendedCell)right).getRowByteBuffer(),
          ((ByteBufferExtendedCell)right).getRowPosition(), rightRowLength);
    }
    return Bytes.compareTo(left.getRowArray(), left.getRowOffset(), leftRowLength,
        right.getRowArray(), right.getRowOffset(), rightRowLength);
  }

  /**
   * Compares the row part of the cell with a simple plain byte[] like the
   * stopRow in Scan. This should be used with context where for hbase:meta
   * cells the {{@link MetaCellComparator#META_COMPARATOR} should be used
   *
   * @param left
   *          the cell to be compared
   * @param right
   *          the kv serialized byte[] to be compared with
   * @param roffset
   *          the offset in the byte[]
   * @param rlength
   *          the length in the byte[]
   * @return 0 if both cell and the byte[] are equal, 1 if the cell is bigger
   *         than byte[], -1 otherwise
   */
  @Override
  public int compareRows(Cell left, byte[] right, int roffset, int rlength) {
    if (left instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.compareTo(((ByteBufferExtendedCell) left).getRowByteBuffer(),
          ((ByteBufferExtendedCell) left).getRowPosition(), left.getRowLength(), right,
          roffset, rlength);
    }
    return Bytes.compareTo(left.getRowArray(), left.getRowOffset(), left.getRowLength(), right,
        roffset, rlength);
  }

  @Override
  public final int compareWithoutRow(final Cell left, final Cell right) {
    // If the column is not specified, the "minimum" key type appears the
    // latest in the sorted order, regardless of the timestamp. This is used
    // for specifying the last key/value in a given row, because there is no
    // "lexicographically last column" (it would be infinitely long). The
    // "maximum" key type does not need this behavior.
    // Copied from KeyValue. This is bad in that we can't do memcmp w/ special rules like this.
    int lFamLength = left.getFamilyLength();
    int rFamLength = right.getFamilyLength();
    int lQualLength = left.getQualifierLength();
    int rQualLength = right.getQualifierLength();
    if (lFamLength + lQualLength == 0
        && left.getTypeByte() == Type.Minimum.getCode()) {
      // left is "bigger", i.e. it appears later in the sorted order
      return 1;
    }
    if (rFamLength + rQualLength == 0
        && right.getTypeByte() == Type.Minimum.getCode()) {
      return -1;
    }
    if (lFamLength != rFamLength) {
      // comparing column family is enough.
      return compareFamilies(left, lFamLength, right, rFamLength);
    }
    // Compare cf:qualifier
    int diff = compareColumns(left, lFamLength, lQualLength, right, rFamLength, rQualLength);
    if (diff != 0) {
      return diff;
    }

    diff = compareTimestamps(left.getTimestamp(), right.getTimestamp());
    if (diff != 0) {
      return diff;
    }

    // Compare types. Let the delete types sort ahead of puts; i.e. types
    // of higher numbers sort before those of lesser numbers. Maximum (255)
    // appears ahead of everything, and minimum (0) appears after
    // everything.
    return (0xff & right.getTypeByte()) - (0xff & left.getTypeByte());
  }

  @Override
  public int compareTimestamps(final Cell left, final Cell right) {
    return compareTimestamps(left.getTimestamp(), right.getTimestamp());
  }

  @Override
  public int compareTimestamps(final long ltimestamp, final long rtimestamp) {
    // Swap order we pass into compare so we get DESCENDING order.
    return Long.compare(rtimestamp, ltimestamp);
  }

  @Override
  public Comparator getSimpleComparator() {
    return this;
  }

  /**
   * Utility method that makes a guess at comparator to use based off passed tableName.
   * Use in extreme when no comparator specified.
   * @return CellComparator to use going off the {@code tableName} passed.
   */
  public static CellComparator getCellComparator(TableName tableName) {
    return getCellComparator(tableName.toBytes());
  }

  /**
   * Utility method that makes a guess at comparator to use based off passed tableName.
   * Use in extreme when no comparator specified.
   * @return CellComparator to use going off the {@code tableName} passed.
   */
  public static CellComparator getCellComparator(byte [] tableName) {
    // FYI, TableName.toBytes does not create an array; just returns existing array pointer.
    return Bytes.equals(tableName, TableName.META_TABLE_NAME.toBytes())?
        MetaCellComparator.META_COMPARATOR: CellComparatorImpl.COMPARATOR;
  }
}
