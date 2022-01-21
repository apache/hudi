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

import static org.apache.hudi.hbase.KeyValue.COLUMN_FAMILY_DELIMITER;
import static org.apache.hudi.hbase.KeyValue.COLUMN_FAMILY_DELIM_ARRAY;
import static org.apache.hudi.hbase.KeyValue.getDelimiter;
import static org.apache.hudi.hbase.Tag.TAG_LENGTH_SIZE;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.function.Function;
import org.apache.hudi.hbase.KeyValue.Type;
import org.apache.hudi.hbase.io.HeapSize;
import org.apache.hudi.hbase.util.ByteBufferUtils;
import org.apache.hudi.hbase.util.ByteRange;
import org.apache.hudi.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Utility methods helpful for slinging {@link Cell} instances. Some methods below are for internal
 * use only and are marked InterfaceAudience.Private at the method level. Note that all such methods
 * have been marked deprecated in HBase-2.0 which will be subsequently removed in HBase-3.0
 */
@InterfaceAudience.Public
public final class CellUtil {

  /**
   * Private constructor to keep this class from being instantiated.
   */
  private CellUtil() {
  }

  /******************* ByteRange *******************************/

  /**
   * @deprecated As of HBase-2.0. Will be removed in HBase-3.0.
   */
  @Deprecated
  public static ByteRange fillRowRange(Cell cell, ByteRange range) {
    return range.set(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
  }

  /**
   * @deprecated As of HBase-2.0. Will be removed in HBase-3.0.
   */
  @Deprecated
  public static ByteRange fillFamilyRange(Cell cell, ByteRange range) {
    return range.set(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
  }

  /**
   * @deprecated As of HBase-2.0. Will be removed in HBase-3.0.
   */
  @Deprecated
  public static ByteRange fillQualifierRange(Cell cell, ByteRange range) {
    return range.set(cell.getQualifierArray(), cell.getQualifierOffset(),
        cell.getQualifierLength());
  }

  /**
   * @deprecated As of HBase-2.0. Will be removed in HBase-3.0.
   */
  @Deprecated
  public static ByteRange fillValueRange(Cell cell, ByteRange range) {
    return range.set(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
  }

  /**
   * @deprecated As of HBase-2.0. Will be removed in HBase-3.0.
   */
  @Deprecated
  public static ByteRange fillTagRange(Cell cell, ByteRange range) {
    return range.set(cell.getTagsArray(), cell.getTagsOffset(), cell.getTagsLength());
  }

  /***************** get individual arrays for tests ************/

  public static byte[] cloneRow(Cell cell) {
    byte[] output = new byte[cell.getRowLength()];
    copyRowTo(cell, output, 0);
    return output;
  }

  public static byte[] cloneFamily(Cell cell) {
    byte[] output = new byte[cell.getFamilyLength()];
    copyFamilyTo(cell, output, 0);
    return output;
  }

  public static byte[] cloneQualifier(Cell cell) {
    byte[] output = new byte[cell.getQualifierLength()];
    copyQualifierTo(cell, output, 0);
    return output;
  }

  public static byte[] cloneValue(Cell cell) {
    byte[] output = new byte[cell.getValueLength()];
    copyValueTo(cell, output, 0);
    return output;
  }

  /**
   * @deprecated As of HBase-2.0. Will be removed in HBase-3.0.
   *             Use {@link RawCell#cloneTags()}
   */
  @Deprecated
  public static byte[] cloneTags(Cell cell) {
    byte[] output = new byte[cell.getTagsLength()];
    PrivateCellUtil.copyTagsTo(cell, output, 0);
    return output;
  }

  /**
   * Returns tag value in a new byte array. If server-side, use {@link Tag#getValueArray()} with
   * appropriate {@link Tag#getValueOffset()} and {@link Tag#getValueLength()} instead to save on
   * allocations.
   * @param cell
   * @return tag value in a new byte array.
   * @deprecated As of HBase-2.0. Will be removed in HBase-3.0
   */
  @Deprecated
  public static byte[] getTagArray(Cell cell) {
    byte[] output = new byte[cell.getTagsLength()];
    PrivateCellUtil.copyTagsTo(cell, output, 0);
    return output;
  }

  /**
   * Makes a column in family:qualifier form from separate byte arrays.
   * <p>
   * Not recommended for usage as this is old-style API.
   * @param family
   * @param qualifier
   * @return family:qualifier
   */
  public static byte[] makeColumn(byte[] family, byte[] qualifier) {
    return Bytes.add(family, COLUMN_FAMILY_DELIM_ARRAY, qualifier);
  }

  /**
   * Splits a column in {@code family:qualifier} form into separate byte arrays. An empty qualifier
   * (ie, {@code fam:}) is parsed as <code>{ fam, EMPTY_BYTE_ARRAY }</code> while no delimiter (ie,
   * {@code fam}) is parsed as an array of one element, <code>{ fam }</code>.
   * <p>
   * Don't forget, HBase DOES support empty qualifiers. (see HBASE-9549)
   * </p>
   * <p>
   * Not recommend to be used as this is old-style API.
   * </p>
   * @param c The column.
   * @return The parsed column.
   */
  public static byte[][] parseColumn(byte[] c) {
    final int index = getDelimiter(c, 0, c.length, COLUMN_FAMILY_DELIMITER);
    if (index == -1) {
      // If no delimiter, return array of size 1
      return new byte[][] { c };
    } else if (index == c.length - 1) {
      // family with empty qualifier, return array size 2
      byte[] family = new byte[c.length - 1];
      System.arraycopy(c, 0, family, 0, family.length);
      return new byte[][] { family, HConstants.EMPTY_BYTE_ARRAY };
    }
    // Family and column, return array size 2
    final byte[][] result = new byte[2][];
    result[0] = new byte[index];
    System.arraycopy(c, 0, result[0], 0, index);
    final int len = c.length - (index + 1);
    result[1] = new byte[len];
    System.arraycopy(c, index + 1 /* Skip delimiter */, result[1], 0, len);
    return result;
  }

  /******************** copyTo **********************************/

  /**
   * Copies the row to the given byte[]
   * @param cell the cell whose row has to be copied
   * @param destination the destination byte[] to which the row has to be copied
   * @param destinationOffset the offset in the destination byte[]
   * @return the offset of the byte[] after the copy has happened
   */
  public static int copyRowTo(Cell cell, byte[] destination, int destinationOffset) {
    short rowLen = cell.getRowLength();
    if (cell instanceof ByteBufferExtendedCell) {
      ByteBufferUtils.copyFromBufferToArray(destination,
          ((ByteBufferExtendedCell) cell).getRowByteBuffer(),
          ((ByteBufferExtendedCell) cell).getRowPosition(), destinationOffset, rowLen);
    } else {
      System.arraycopy(cell.getRowArray(), cell.getRowOffset(), destination, destinationOffset,
          rowLen);
    }
    return destinationOffset + rowLen;
  }

  /**
   * Copies the row to the given bytebuffer
   * @param cell cell the cell whose row has to be copied
   * @param destination the destination bytebuffer to which the row has to be copied
   * @param destinationOffset the offset in the destination byte[]
   * @return the offset of the bytebuffer after the copy has happened
   */
  public static int copyRowTo(Cell cell, ByteBuffer destination, int destinationOffset) {
    short rowLen = cell.getRowLength();
    if (cell instanceof ByteBufferExtendedCell) {
      ByteBufferUtils.copyFromBufferToBuffer(((ByteBufferExtendedCell) cell).getRowByteBuffer(),
          destination, ((ByteBufferExtendedCell) cell).getRowPosition(), destinationOffset, rowLen);
    } else {
      ByteBufferUtils.copyFromArrayToBuffer(destination, destinationOffset, cell.getRowArray(),
          cell.getRowOffset(), rowLen);
    }
    return destinationOffset + rowLen;
  }

  /**
   * Copies the row to a new byte[]
   * @param cell the cell from which row has to copied
   * @return the byte[] containing the row
   */
  public static byte[] copyRow(Cell cell) {
    if (cell instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.copyOfRange(((ByteBufferExtendedCell) cell).getRowByteBuffer(),
          ((ByteBufferExtendedCell) cell).getRowPosition(),
          ((ByteBufferExtendedCell) cell).getRowPosition() + cell.getRowLength());
    } else {
      return Arrays.copyOfRange(cell.getRowArray(), cell.getRowOffset(),
          cell.getRowOffset() + cell.getRowLength());
    }
  }

  /**
   * Copies the family to the given byte[]
   * @param cell the cell whose family has to be copied
   * @param destination the destination byte[] to which the family has to be copied
   * @param destinationOffset the offset in the destination byte[]
   * @return the offset of the byte[] after the copy has happened
   */
  public static int copyFamilyTo(Cell cell, byte[] destination, int destinationOffset) {
    byte fLen = cell.getFamilyLength();
    if (cell instanceof ByteBufferExtendedCell) {
      ByteBufferUtils.copyFromBufferToArray(destination,
          ((ByteBufferExtendedCell) cell).getFamilyByteBuffer(),
          ((ByteBufferExtendedCell) cell).getFamilyPosition(), destinationOffset, fLen);
    } else {
      System.arraycopy(cell.getFamilyArray(), cell.getFamilyOffset(), destination,
          destinationOffset, fLen);
    }
    return destinationOffset + fLen;
  }

  /**
   * Copies the family to the given bytebuffer
   * @param cell the cell whose family has to be copied
   * @param destination the destination bytebuffer to which the family has to be copied
   * @param destinationOffset the offset in the destination bytebuffer
   * @return the offset of the bytebuffer after the copy has happened
   */
  public static int copyFamilyTo(Cell cell, ByteBuffer destination, int destinationOffset) {
    byte fLen = cell.getFamilyLength();
    if (cell instanceof ByteBufferExtendedCell) {
      ByteBufferUtils.copyFromBufferToBuffer(((ByteBufferExtendedCell) cell).getFamilyByteBuffer(),
          destination, ((ByteBufferExtendedCell) cell).getFamilyPosition(), destinationOffset, fLen);
    } else {
      ByteBufferUtils.copyFromArrayToBuffer(destination, destinationOffset, cell.getFamilyArray(),
          cell.getFamilyOffset(), fLen);
    }
    return destinationOffset + fLen;
  }

  /**
   * Copies the qualifier to the given byte[]
   * @param cell the cell whose qualifier has to be copied
   * @param destination the destination byte[] to which the qualifier has to be copied
   * @param destinationOffset the offset in the destination byte[]
   * @return the offset of the byte[] after the copy has happened
   */
  public static int copyQualifierTo(Cell cell, byte[] destination, int destinationOffset) {
    int qlen = cell.getQualifierLength();
    if (cell instanceof ByteBufferExtendedCell) {
      ByteBufferUtils.copyFromBufferToArray(destination,
          ((ByteBufferExtendedCell) cell).getQualifierByteBuffer(),
          ((ByteBufferExtendedCell) cell).getQualifierPosition(), destinationOffset, qlen);
    } else {
      System.arraycopy(cell.getQualifierArray(), cell.getQualifierOffset(), destination,
          destinationOffset, qlen);
    }
    return destinationOffset + qlen;
  }

  /**
   * Copies the qualifier to the given bytebuffer
   * @param cell the cell whose qualifier has to be copied
   * @param destination the destination bytebuffer to which the qualifier has to be copied
   * @param destinationOffset the offset in the destination bytebuffer
   * @return the offset of the bytebuffer after the copy has happened
   */
  public static int copyQualifierTo(Cell cell, ByteBuffer destination, int destinationOffset) {
    int qlen = cell.getQualifierLength();
    if (cell instanceof ByteBufferExtendedCell) {
      ByteBufferUtils.copyFromBufferToBuffer(
          ((ByteBufferExtendedCell) cell).getQualifierByteBuffer(),
          destination, ((ByteBufferExtendedCell) cell).getQualifierPosition(),
          destinationOffset, qlen);
    } else {
      ByteBufferUtils.copyFromArrayToBuffer(destination, destinationOffset,
          cell.getQualifierArray(), cell.getQualifierOffset(), qlen);
    }
    return destinationOffset + qlen;
  }

  /**
   * Copies the value to the given byte[]
   * @param cell the cell whose value has to be copied
   * @param destination the destination byte[] to which the value has to be copied
   * @param destinationOffset the offset in the destination byte[]
   * @return the offset of the byte[] after the copy has happened
   */
  public static int copyValueTo(Cell cell, byte[] destination, int destinationOffset) {
    int vlen = cell.getValueLength();
    if (cell instanceof ByteBufferExtendedCell) {
      ByteBufferUtils.copyFromBufferToArray(destination,
          ((ByteBufferExtendedCell) cell).getValueByteBuffer(),
          ((ByteBufferExtendedCell) cell).getValuePosition(), destinationOffset, vlen);
    } else {
      System.arraycopy(cell.getValueArray(), cell.getValueOffset(), destination, destinationOffset,
          vlen);
    }
    return destinationOffset + vlen;
  }

  /**
   * Copies the value to the given bytebuffer
   * @param cell the cell whose value has to be copied
   * @param destination the destination bytebuffer to which the value has to be copied
   * @param destinationOffset the offset in the destination bytebuffer
   * @return the offset of the bytebuffer after the copy has happened
   */
  public static int copyValueTo(Cell cell, ByteBuffer destination, int destinationOffset) {
    int vlen = cell.getValueLength();
    if (cell instanceof ByteBufferExtendedCell) {
      ByteBufferUtils.copyFromBufferToBuffer(((ByteBufferExtendedCell) cell).getValueByteBuffer(),
          destination, ((ByteBufferExtendedCell) cell).getValuePosition(), destinationOffset, vlen);
    } else {
      ByteBufferUtils.copyFromArrayToBuffer(destination, destinationOffset, cell.getValueArray(),
          cell.getValueOffset(), vlen);
    }
    return destinationOffset + vlen;
  }

  /**
   * Copies the tags info into the tag portion of the cell
   * @param cell
   * @param destination
   * @param destinationOffset
   * @return position after tags
   * @deprecated As of HBase-2.0. Will be removed in HBase-3.0.
   */
  @Deprecated
  public static int copyTagTo(Cell cell, byte[] destination, int destinationOffset) {
    int tlen = cell.getTagsLength();
    if (cell instanceof ByteBufferExtendedCell) {
      ByteBufferUtils
          .copyFromBufferToArray(destination, ((ByteBufferExtendedCell) cell).getTagsByteBuffer(),
              ((ByteBufferExtendedCell) cell).getTagsPosition(), destinationOffset, tlen);
    } else {
      System
          .arraycopy(cell.getTagsArray(), cell.getTagsOffset(), destination, destinationOffset, tlen);
    }
    return destinationOffset + tlen;
  }

  /**
   * Copies the tags info into the tag portion of the cell
   * @param cell
   * @param destination
   * @param destinationOffset
   * @return position after tags
   * @deprecated As of HBase-2.0. Will be removed in 3.0.
   */
  @Deprecated
  public static int copyTagTo(Cell cell, ByteBuffer destination, int destinationOffset) {
    int tlen = cell.getTagsLength();
    if (cell instanceof ByteBufferExtendedCell) {
      ByteBufferUtils.copyFromBufferToBuffer(((ByteBufferExtendedCell) cell).getTagsByteBuffer(),
          destination, ((ByteBufferExtendedCell) cell).getTagsPosition(), destinationOffset, tlen);
    } else {
      ByteBufferUtils.copyFromArrayToBuffer(destination, destinationOffset, cell.getTagsArray(),
          cell.getTagsOffset(), tlen);
    }
    return destinationOffset + tlen;
  }

  /********************* misc *************************************/

  @InterfaceAudience.Private
  /**
   * @deprecated As of HBase-2.0. Will be removed in HBase-3.0.
   */
  @Deprecated
  public static byte getRowByte(Cell cell, int index) {
    if (cell instanceof ByteBufferExtendedCell) {
      return ((ByteBufferExtendedCell) cell).getRowByteBuffer()
          .get(((ByteBufferExtendedCell) cell).getRowPosition() + index);
    }
    return cell.getRowArray()[cell.getRowOffset() + index];
  }

  /**
   * @deprecated As of HBase-2.0. Will be removed in 3.0.
   */
  @Deprecated
  public static ByteBuffer getValueBufferShallowCopy(Cell cell) {
    ByteBuffer buffer =
        ByteBuffer.wrap(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
    return buffer;
  }

  /**
   * @param cell
   * @return cell's qualifier wrapped into a ByteBuffer.
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public static ByteBuffer getQualifierBufferShallowCopy(Cell cell) {
    // No usage of this in code.
    ByteBuffer buffer = ByteBuffer.wrap(cell.getQualifierArray(), cell.getQualifierOffset(),
        cell.getQualifierLength());
    return buffer;
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0. Use {@link CellBuilder}
   *             instead
   */
  @Deprecated
  public static Cell createCell(final byte[] row, final byte[] family, final byte[] qualifier,
                                final long timestamp, final byte type, final byte[] value) {
    return ExtendedCellBuilderFactory.create(CellBuilderType.DEEP_COPY)
        .setRow(row)
        .setFamily(family)
        .setQualifier(qualifier)
        .setTimestamp(timestamp)
        .setType(type)
        .setValue(value)
        .build();
  }

  /**
   * Creates a cell with deep copy of all passed bytes.
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0. Use {@link CellBuilder}
   *             instead
   */
  @Deprecated
  public static Cell createCell(final byte[] rowArray, final int rowOffset, final int rowLength,
                                final byte[] familyArray, final int familyOffset, final int familyLength,
                                final byte[] qualifierArray, final int qualifierOffset, final int qualifierLength) {
    // See createCell(final byte [] row, final byte [] value) for why we default Maximum type.
    return ExtendedCellBuilderFactory.create(CellBuilderType.DEEP_COPY)
        .setRow(rowArray, rowOffset, rowLength)
        .setFamily(familyArray, familyOffset, familyLength)
        .setQualifier(qualifierArray, qualifierOffset, qualifierLength)
        .setTimestamp(HConstants.LATEST_TIMESTAMP)
        .setType(KeyValue.Type.Maximum.getCode())
        .setValue(HConstants.EMPTY_BYTE_ARRAY, 0, HConstants.EMPTY_BYTE_ARRAY.length)
        .build();
  }

  /**
   * Marked as audience Private as of 1.2.0.
   * Creating a Cell with a memstoreTS/mvcc is an internal
   * implementation detail not for public use.
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0. Use
   *             {@link ExtendedCellBuilder} instead
   */
  @InterfaceAudience.Private
  @Deprecated
  public static Cell createCell(final byte[] row, final byte[] family, final byte[] qualifier,
                                final long timestamp, final byte type, final byte[] value, final long memstoreTS) {
    return createCell(row, family, qualifier, timestamp, type, value, null, memstoreTS);
  }

  /**
   * Marked as audience Private as of 1.2.0.
   * Creating a Cell with tags and a memstoreTS/mvcc is an
   * internal implementation detail not for public use.
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0. Use
   *             {@link ExtendedCellBuilder} instead
   */
  @InterfaceAudience.Private
  @Deprecated
  public static Cell createCell(final byte[] row, final byte[] family, final byte[] qualifier,
                                final long timestamp, final byte type, final byte[] value, byte[] tags,
                                final long memstoreTS) {
    return ExtendedCellBuilderFactory.create(CellBuilderType.DEEP_COPY)
        .setRow(row)
        .setFamily(family)
        .setQualifier(qualifier)
        .setTimestamp(timestamp)
        .setType(type)
        .setValue(value)
        .setTags(tags)
        .setSequenceId(memstoreTS)
        .build();
  }

  /**
   * Marked as audience Private as of 1.2.0.
   * Creating a Cell with tags is an internal implementation detail not for public use.
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0. Use
   *             {@link ExtendedCellBuilder} instead
   */
  @InterfaceAudience.Private
  @Deprecated
  public static Cell createCell(final byte[] row, final byte[] family, final byte[] qualifier,
                                final long timestamp, Type type, final byte[] value, byte[] tags) {
    return createCell(row, family, qualifier, timestamp, type.getCode(), value, tags, 0);
  }

  /**
   * Create a Cell with specific row. Other fields defaulted.
   * @param row
   * @return Cell with passed row but all other fields are arbitrary
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0. Use {@link CellBuilder}
   *             instead
   */
  @Deprecated
  public static Cell createCell(final byte[] row) {
    return createCell(row, HConstants.EMPTY_BYTE_ARRAY);
  }

  /**
   * Create a Cell with specific row and value. Other fields are defaulted.
   * @param row
   * @param value
   * @return Cell with passed row and value but all other fields are arbitrary
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0. Use {@link CellBuilder}
   *             instead
   */
  @Deprecated
  public static Cell createCell(final byte[] row, final byte[] value) {
    // An empty family + empty qualifier + Type.Minimum is used as flag to indicate last on row.
    // See the CellComparator and KeyValue comparator. Search for compareWithoutRow.
    // Lets not make a last-on-row key as default but at same time, if you are making a key
    // without specifying type, etc., flag it as weird by setting type to be Maximum.
    return createCell(row, HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY,
        HConstants.LATEST_TIMESTAMP, KeyValue.Type.Maximum.getCode(), value);
  }

  /**
   * Create a Cell with specific row. Other fields defaulted.
   * @param row
   * @param family
   * @param qualifier
   * @return Cell with passed row but all other fields are arbitrary
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0. Use {@link CellBuilder}
   *             instead
   */
  @Deprecated
  public static Cell createCell(final byte[] row, final byte[] family, final byte[] qualifier) {
    // See above in createCell(final byte [] row, final byte [] value) why we set type to Maximum.
    return createCell(row, family, qualifier, HConstants.LATEST_TIMESTAMP,
        KeyValue.Type.Maximum.getCode(), HConstants.EMPTY_BYTE_ARRAY);
  }

  /**
   * Note : Now only CPs can create cell with tags using the CP environment
   * Within CP, use {@link RawCell#createCell(Cell, List)} method instead
   * @return A new cell which is having the extra tags also added to it.
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   *
   */
  @Deprecated
  public static Cell createCell(Cell cell, List<Tag> tags) {
    return PrivateCellUtil.createCell(cell, tags);
  }

  /**
   * Now only CPs can create cell with tags using the CP environment
   * Within CP, use {@link RawCell#createCell(Cell, List)} method instead
   * @return A new cell which is having the extra tags also added to it.
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public static Cell createCell(Cell cell, byte[] tags) {
    return PrivateCellUtil.createCell(cell, tags);
  }

  /**
   * Now only CPs can create cell with tags using the CP environment
   * Within CP, use {@link RawCell#createCell(Cell, List)} method instead
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public static Cell createCell(Cell cell, byte[] value, byte[] tags) {
    return PrivateCellUtil.createCell(cell, value, tags);
  }

  /**
   * @param cellScannerables
   * @return CellScanner interface over <code>cellIterables</code>
   */
  public static CellScanner
  createCellScanner(final List<? extends CellScannable> cellScannerables) {
    return new CellScanner() {
      private final Iterator<? extends CellScannable> iterator = cellScannerables.iterator();
      private CellScanner cellScanner = null;

      @Override
      public Cell current() {
        return this.cellScanner != null ? this.cellScanner.current() : null;
      }

      @Override
      public boolean advance() throws IOException {
        while (true) {
          if (this.cellScanner == null) {
            if (!this.iterator.hasNext()) return false;
            this.cellScanner = this.iterator.next().cellScanner();
          }
          if (this.cellScanner.advance()) return true;
          this.cellScanner = null;
        }
      }
    };
  }

  /**
   * @param cellIterable
   * @return CellScanner interface over <code>cellIterable</code>
   */
  public static CellScanner createCellScanner(final Iterable<Cell> cellIterable) {
    if (cellIterable == null) return null;
    return createCellScanner(cellIterable.iterator());
  }

  /**
   * @param cells
   * @return CellScanner interface over <code>cellIterable</code> or null if <code>cells</code> is
   *         null
   */
  public static CellScanner createCellScanner(final Iterator<Cell> cells) {
    if (cells == null) return null;
    return new CellScanner() {
      private final Iterator<Cell> iterator = cells;
      private Cell current = null;

      @Override
      public Cell current() {
        return this.current;
      }

      @Override
      public boolean advance() {
        boolean hasNext = this.iterator.hasNext();
        this.current = hasNext ? this.iterator.next() : null;
        return hasNext;
      }
    };
  }

  /**
   * @param cellArray
   * @return CellScanner interface over <code>cellArray</code>
   */
  public static CellScanner createCellScanner(final Cell[] cellArray) {
    return new CellScanner() {
      private final Cell[] cells = cellArray;
      private int index = -1;

      @Override
      public Cell current() {
        if (cells == null) return null;
        return (index < 0) ? null : this.cells[index];
      }

      @Override
      public boolean advance() {
        if (cells == null) return false;
        return ++index < this.cells.length;
      }
    };
  }

  /**
   * Flatten the map of cells out under the CellScanner
   * @param map Map of Cell Lists; for example, the map of families to Cells that is used inside
   *          Put, etc., keeping Cells organized by family.
   * @return CellScanner interface over <code>cellIterable</code>
   */
  public static CellScanner createCellScanner(final NavigableMap<byte[], List<Cell>> map) {
    return new CellScanner() {
      private final Iterator<Entry<byte[], List<Cell>>> entries = map.entrySet().iterator();
      private Iterator<Cell> currentIterator = null;
      private Cell currentCell;

      @Override
      public Cell current() {
        return this.currentCell;
      }

      @Override
      public boolean advance() {
        while (true) {
          if (this.currentIterator == null) {
            if (!this.entries.hasNext()) return false;
            this.currentIterator = this.entries.next().getValue().iterator();
          }
          if (this.currentIterator.hasNext()) {
            this.currentCell = this.currentIterator.next();
            return true;
          }
          this.currentCell = null;
          this.currentIterator = null;
        }
      }
    };
  }

  /**
   * @param left
   * @param right
   * @return True if the rows in <code>left</code> and <code>right</code> Cells match
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0. Instead use
   *             {@link #matchingRows(Cell, Cell)}
   */
  @Deprecated
  public static boolean matchingRow(final Cell left, final Cell right) {
    return matchingRows(left, right);
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0. Instead use
   *   {@link #matchingRows(Cell, byte[])}
   */
  @Deprecated
  public static boolean matchingRow(final Cell left, final byte[] buf) {
    return matchingRows(left, buf);
  }

  public static boolean matchingRows(final Cell left, final byte[] buf) {
    if (buf == null) {
      return left.getRowLength() == 0;
    }
    return PrivateCellUtil.matchingRows(left, buf, 0, buf.length);
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0. Instead use
   *             {@link #matchingRows(Cell, Cell)}
   * @return true if the row is matching
   */
  @Deprecated
  public static boolean matchingRow(final Cell left, final byte[] buf, final int offset,
                                    final int length) {
    if (left instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.equals(((ByteBufferExtendedCell) left).getRowByteBuffer(),
          ((ByteBufferExtendedCell) left).getRowPosition(), left.getRowLength(), buf, offset, length);
    }
    return Bytes.equals(left.getRowArray(), left.getRowOffset(), left.getRowLength(), buf, offset,
        length);
  }

  public static boolean matchingFamily(final Cell left, final Cell right) {
    byte lfamlength = left.getFamilyLength();
    byte rfamlength = right.getFamilyLength();
    return matchingFamily(left, lfamlength, right, rfamlength);
  }

  public static boolean matchingFamily(final Cell left, final byte lfamlength, final Cell right,
                                       final byte rfamlength) {
    if (left instanceof ByteBufferExtendedCell && right instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.equals(((ByteBufferExtendedCell) left).getFamilyByteBuffer(),
          ((ByteBufferExtendedCell) left).getFamilyPosition(), lfamlength,
          ((ByteBufferExtendedCell) right).getFamilyByteBuffer(),
          ((ByteBufferExtendedCell) right).getFamilyPosition(), rfamlength);
    }
    if (left instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.equals(((ByteBufferExtendedCell) left).getFamilyByteBuffer(),
          ((ByteBufferExtendedCell) left).getFamilyPosition(), lfamlength, right.getFamilyArray(),
          right.getFamilyOffset(), rfamlength);
    }
    if (right instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.equals(((ByteBufferExtendedCell) right).getFamilyByteBuffer(),
          ((ByteBufferExtendedCell) right).getFamilyPosition(), rfamlength, left.getFamilyArray(),
          left.getFamilyOffset(), lfamlength);
    }
    return Bytes.equals(left.getFamilyArray(), left.getFamilyOffset(), lfamlength,
        right.getFamilyArray(), right.getFamilyOffset(), rfamlength);
  }

  public static boolean matchingFamily(final Cell left, final byte[] buf) {
    if (buf == null) {
      return left.getFamilyLength() == 0;
    }
    return PrivateCellUtil.matchingFamily(left, buf, 0, buf.length);
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public static boolean matchingFamily(final Cell left, final byte[] buf, final int offset,
                                       final int length) {
    if (left instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.equals(((ByteBufferExtendedCell) left).getFamilyByteBuffer(),
          ((ByteBufferExtendedCell) left).getFamilyPosition(), left.getFamilyLength(), buf, offset,
          length);
    }
    return Bytes
        .equals(left.getFamilyArray(), left.getFamilyOffset(), left.getFamilyLength(), buf, offset,
            length);
  }

  public static boolean matchingQualifier(final Cell left, final Cell right) {
    int lqlength = left.getQualifierLength();
    int rqlength = right.getQualifierLength();
    return matchingQualifier(left, lqlength, right, rqlength);
  }

  private static boolean matchingQualifier(final Cell left, final int lqlength, final Cell right,
                                           final int rqlength) {
    if (left instanceof ByteBufferExtendedCell && right instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.equals(((ByteBufferExtendedCell) left).getQualifierByteBuffer(),
          ((ByteBufferExtendedCell) left).getQualifierPosition(), lqlength,
          ((ByteBufferExtendedCell) right).getQualifierByteBuffer(),
          ((ByteBufferExtendedCell) right).getQualifierPosition(), rqlength);
    }
    if (left instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.equals(((ByteBufferExtendedCell) left).getQualifierByteBuffer(),
          ((ByteBufferExtendedCell) left).getQualifierPosition(), lqlength, right.getQualifierArray(),
          right.getQualifierOffset(), rqlength);
    }
    if (right instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.equals(((ByteBufferExtendedCell) right).getQualifierByteBuffer(),
          ((ByteBufferExtendedCell) right).getQualifierPosition(), rqlength, left.getQualifierArray(),
          left.getQualifierOffset(), lqlength);
    }
    return Bytes.equals(left.getQualifierArray(), left.getQualifierOffset(), lqlength,
        right.getQualifierArray(), right.getQualifierOffset(), rqlength);
  }

  /**
   * Finds if the qualifier part of the cell and the KV serialized byte[] are equal
   * @param left
   * @param buf the serialized keyvalue format byte[]
   * @return true if the qualifier matches, false otherwise
   */
  public static boolean matchingQualifier(final Cell left, final byte[] buf) {
    if (buf == null) {
      return left.getQualifierLength() == 0;
    }
    return PrivateCellUtil.matchingQualifier(left, buf, 0, buf.length);
  }

  /**
   * Finds if the qualifier part of the cell and the KV serialized byte[] are equal
   * @param left
   * @param buf the serialized keyvalue format byte[]
   * @param offset the offset of the qualifier in the byte[]
   * @param length the length of the qualifier in the byte[]
   * @return true if the qualifier matches, false otherwise
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public static boolean matchingQualifier(final Cell left, final byte[] buf, final int offset,
                                          final int length) {
    if (buf == null) {
      return left.getQualifierLength() == 0;
    }
    if (left instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.equals(((ByteBufferExtendedCell) left).getQualifierByteBuffer(),
          ((ByteBufferExtendedCell) left).getQualifierPosition(), left.getQualifierLength(), buf,
          offset, length);
    }
    return Bytes
        .equals(left.getQualifierArray(), left.getQualifierOffset(), left.getQualifierLength(), buf,
            offset, length);
  }

  public static boolean matchingColumn(final Cell left, final byte[] fam, final byte[] qual) {
    return matchingFamily(left, fam) && matchingQualifier(left, qual);
  }

  /**
   * @return True if matching column family and the qualifier starts with <code>qual</code>
   */
  public static boolean matchingColumnFamilyAndQualifierPrefix(final Cell left, final byte[] fam,
                                                               final byte[] qual) {
    return matchingFamily(left, fam) && PrivateCellUtil.qualifierStartsWith(left, qual);
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public static boolean matchingColumn(final Cell left, final byte[] fam, final int foffset,
                                       final int flength, final byte[] qual, final int qoffset, final int qlength) {
    if (!PrivateCellUtil.matchingFamily(left, fam, foffset, flength)) return false;
    return PrivateCellUtil.matchingQualifier(left, qual, qoffset, qlength);
  }

  public static boolean matchingColumn(final Cell left, final Cell right) {
    if (!matchingFamily(left, right)) return false;
    return matchingQualifier(left, right);
  }

  private static boolean matchingColumn(final Cell left, final byte lFamLen, final int lQualLength,
                                        final Cell right, final byte rFamLen, final int rQualLength) {
    if (!matchingFamily(left, lFamLen, right, rFamLen)) {
      return false;
    }
    return matchingQualifier(left, lQualLength, right, rQualLength);
  }

  public static boolean matchingValue(final Cell left, final Cell right) {
    return PrivateCellUtil.matchingValue(left, right, left.getValueLength(),
        right.getValueLength());
  }

  public static boolean matchingValue(final Cell left, final byte[] buf) {
    if (left instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.compareTo(((ByteBufferExtendedCell) left).getValueByteBuffer(),
          ((ByteBufferExtendedCell) left).getValuePosition(), left.getValueLength(), buf, 0,
          buf.length) == 0;
    }
    return Bytes.equals(left.getValueArray(), left.getValueOffset(), left.getValueLength(), buf, 0,
        buf.length);
  }

  public static boolean matchingTags(final Cell left, final Cell right) {
    return PrivateCellUtil.matchingTags(left, right, left.getTagsLength(), right.getTagsLength());
  }

  /**
   * @return True if a delete type, a {@link KeyValue.Type#Delete} or a {KeyValue.Type#DeleteFamily}
   *         or a {@link KeyValue.Type#DeleteColumn} KeyValue type.
   */
  @SuppressWarnings("deprecation")
  public static boolean isDelete(final Cell cell) {
    return PrivateCellUtil.isDelete(cell.getTypeByte());
  }

  /**
   * @return True if a delete type, a {@link KeyValue.Type#Delete} or a {KeyValue.Type#DeleteFamily}
   *         or a {@link KeyValue.Type#DeleteColumn} KeyValue type.
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public static boolean isDelete(final byte type) {
    return Type.Delete.getCode() <= type && type <= Type.DeleteFamily.getCode();
  }

  /**
   * @return True if this cell is a {@link KeyValue.Type#Delete} type.
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public static boolean isDeleteType(Cell cell) {
    return cell.getTypeByte() == Type.Delete.getCode();
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public static boolean isDeleteFamily(final Cell cell) {
    return cell.getTypeByte() == Type.DeleteFamily.getCode();
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public static boolean isDeleteFamilyVersion(final Cell cell) {
    return cell.getTypeByte() == Type.DeleteFamilyVersion.getCode();
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public static boolean isDeleteColumns(final Cell cell) {
    return cell.getTypeByte() == Type.DeleteColumn.getCode();
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public static boolean isDeleteColumnVersion(final Cell cell) {
    return cell.getTypeByte() == Type.Delete.getCode();
  }

  /**
   * @return True if this cell is a delete family or column type.
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public static boolean isDeleteColumnOrFamily(Cell cell) {
    int t = cell.getTypeByte();
    return t == Type.DeleteColumn.getCode() || t == Type.DeleteFamily.getCode();
  }

  /**
   * @return True if this cell is a Put.
   */
  @SuppressWarnings("deprecation")
  public static boolean isPut(Cell cell) {
    return cell.getTypeByte() == Type.Put.getCode();
  }

  /**
   * Estimate based on keyvalue's serialization format in the RPC layer. Note that there is an extra
   * SIZEOF_INT added to the size here that indicates the actual length of the cell for cases where
   * cell's are serialized in a contiguous format (For eg in RPCs).
   * @param cell
   * @return Estimate of the <code>cell</code> size in bytes plus an extra SIZEOF_INT indicating the
   *         actual cell length.
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public static int estimatedSerializedSizeOf(final Cell cell) {
    if (cell instanceof ExtendedCell) {
      return ((ExtendedCell) cell).getSerializedSize(true) + Bytes.SIZEOF_INT;
    }

    return getSumOfCellElementLengths(cell) +
        // Use the KeyValue's infrastructure size presuming that another implementation would have
        // same basic cost.
        KeyValue.ROW_LENGTH_SIZE + KeyValue.FAMILY_LENGTH_SIZE +
        // Serialization is probably preceded by a length (it is in the KeyValueCodec at least).
        Bytes.SIZEOF_INT;
  }

  /**
   * @param cell
   * @return Sum of the lengths of all the elements in a Cell; does not count in any infrastructure
   */
  private static int getSumOfCellElementLengths(final Cell cell) {
    return getSumOfCellKeyElementLengths(cell) + cell.getValueLength() + cell.getTagsLength();
  }

  /**
   * @param cell
   * @return Sum of all elements that make up a key; does not include infrastructure, tags or
   *         values.
   */
  private static int getSumOfCellKeyElementLengths(final Cell cell) {
    return cell.getRowLength() + cell.getFamilyLength() + cell.getQualifierLength()
        + KeyValue.TIMESTAMP_TYPE_SIZE;
  }

  /**
   * Calculates the serialized key size. We always serialize in the KeyValue's serialization format.
   * @param cell the cell for which the key size has to be calculated.
   * @return the key size
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public static int estimatedSerializedSizeOfKey(final Cell cell) {
    if (cell instanceof KeyValue) return ((KeyValue) cell).getKeyLength();
    return cell.getRowLength() + cell.getFamilyLength() + cell.getQualifierLength()
        + KeyValue.KEY_INFRASTRUCTURE_SIZE;
  }

  /**
   * This is an estimate of the heap space occupied by a cell. When the cell is of type
   * {@link HeapSize} we call {@link HeapSize#heapSize()} so cell can give a correct value. In other
   * cases we just consider the bytes occupied by the cell components ie. row, CF, qualifier,
   * timestamp, type, value and tags.
   * @param cell
   * @return estimate of the heap space
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   *             Use {@link RawCell#getTags()}
   */
  @Deprecated
  public static long estimatedHeapSizeOf(final Cell cell) {
    return cell.heapSize();
  }

  /********************* tags *************************************/
  /**
   * Util method to iterate through the tags
   * @param tags
   * @param offset
   * @param length
   * @return iterator for the tags
   * @deprecated As of 2.0.0 and will be removed in 3.0.0 Instead use
   *             {@link PrivateCellUtil#tagsIterator(Cell)}
   */
  @Deprecated
  public static Iterator<Tag> tagsIterator(final byte[] tags, final int offset, final int length) {
    return new Iterator<Tag>() {
      private int pos = offset;
      private int endOffset = offset + length - 1;

      @Override
      public boolean hasNext() {
        return this.pos < endOffset;
      }

      @Override
      public Tag next() {
        if (hasNext()) {
          int curTagLen = Bytes.readAsInt(tags, this.pos, Tag.TAG_LENGTH_SIZE);
          Tag tag = new ArrayBackedTag(tags, pos, curTagLen + TAG_LENGTH_SIZE);
          this.pos += Bytes.SIZEOF_SHORT + curTagLen;
          return tag;
        }
        return null;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  /**
   * @param cell The Cell
   * @return Tags in the given Cell as a List
   * @deprecated As of 2.0.0 and will be removed in 3.0.0
   */
  @Deprecated
  public static List<Tag> getTags(Cell cell) {
    List<Tag> tags = new ArrayList<>();
    Iterator<Tag> tagsItr = PrivateCellUtil.tagsIterator(cell);
    while (tagsItr.hasNext()) {
      tags.add(tagsItr.next());
    }
    return tags;
  }

  /**
   * Retrieve Cell's first tag, matching the passed in type
   * @param cell The Cell
   * @param type Type of the Tag to retrieve
   * @return null if there is no tag of the passed in tag type
   * @deprecated As of 2.0.0 and will be removed in HBase-3.0.0
   *             Use {@link RawCell#getTag(byte)}
   */
  @Deprecated
  public static Tag getTag(Cell cell, byte type) {
    Optional<Tag> tag = PrivateCellUtil.getTag(cell, type);
    if (tag.isPresent()) {
      return tag.get();
    } else {
      return null;
    }
  }

  /**
   * Returns true if the first range start1...end1 overlaps with the second range start2...end2,
   * assuming the byte arrays represent row keys
   * @deprecated As of 2.0.0 and will be removed in 3.0.0
   */
  @Deprecated
  public static boolean overlappingKeys(final byte[] start1, final byte[] end1, final byte[] start2,
                                        final byte[] end2) {
    return (end2.length == 0 || start1.length == 0 || Bytes.compareTo(start1, end2) < 0)
        && (end1.length == 0 || start2.length == 0 || Bytes.compareTo(start2, end1) < 0);
  }

  /**
   * Sets the given seqId to the cell. Marked as audience Private as of 1.2.0. Setting a Cell
   * sequenceid is an internal implementation detail not for general public use.
   * @param cell
   * @param seqId
   * @throws IOException when the passed cell is not of type {@link ExtendedCell}
   * @deprecated As of HBase-2.0. Will be removed in HBase-3.0
   */
  @Deprecated
  public static void setSequenceId(Cell cell, long seqId) throws IOException {
    PrivateCellUtil.setSequenceId(cell, seqId);
  }

  /**
   * Sets the given timestamp to the cell.
   * @param cell
   * @param ts
   * @throws IOException when the passed cell is not of type {@link ExtendedCell}
   * @deprecated As of HBase-2.0. Will be a LimitedPrivate API in HBase-3.0.
   */
  @Deprecated
  public static void setTimestamp(Cell cell, long ts) throws IOException {
    PrivateCellUtil.setTimestamp(cell, ts);
  }

  /**
   * Sets the given timestamp to the cell.
   * @param cell
   * @param ts buffer containing the timestamp value
   * @param tsOffset offset to the new timestamp
   * @throws IOException when the passed cell is not of type {@link ExtendedCell}
   * @deprecated As of HBase-2.0. Will be a LimitedPrivate API in HBase-3.0.
   */
  @Deprecated
  public static void setTimestamp(Cell cell, byte[] ts, int tsOffset) throws IOException {
    PrivateCellUtil.setTimestamp(cell, Bytes.toLong(ts, tsOffset));
  }

  /**
   * Sets the given timestamp to the cell iff current timestamp is
   * {@link HConstants#LATEST_TIMESTAMP}.
   * @param cell
   * @param ts
   * @return True if cell timestamp is modified.
   * @throws IOException when the passed cell is not of type {@link ExtendedCell}
   * @deprecated As of HBase-2.0. Will be removed in HBase-3.0
   */
  @Deprecated
  public static boolean updateLatestStamp(Cell cell, long ts) throws IOException {
    return PrivateCellUtil.updateLatestStamp(cell, ts);
  }

  /**
   * Sets the given timestamp to the cell iff current timestamp is
   * {@link HConstants#LATEST_TIMESTAMP}.
   * @param cell
   * @param ts buffer containing the timestamp value
   * @param tsOffset offset to the new timestamp
   * @return True if cell timestamp is modified.
   * @throws IOException when the passed cell is not of type {@link ExtendedCell}
   * @deprecated As of HBase-2.0. Will be removed in HBase-3.0
   */
  @Deprecated
  public static boolean updateLatestStamp(Cell cell, byte[] ts, int tsOffset) throws IOException {
    return PrivateCellUtil.updateLatestStamp(cell, Bytes.toLong(ts, tsOffset));
  }

  /**
   * Writes the Cell's key part as it would have serialized in a KeyValue. The format is &lt;2 bytes
   * rk len&gt;&lt;rk&gt;&lt;1 byte cf len&gt;&lt;cf&gt;&lt;qualifier&gt;&lt;8 bytes
   * timestamp&gt;&lt;1 byte type&gt;
   * @param cell
   * @param out
   * @deprecated As of HBase-2.0. Will be removed in HBase-3.0
   * @throws IOException
   */
  @Deprecated
  public static void writeFlatKey(Cell cell, DataOutputStream out) throws IOException {
    short rowLen = cell.getRowLength();
    byte fLen = cell.getFamilyLength();
    int qLen = cell.getQualifierLength();
    // Using just one if/else loop instead of every time checking before writing every
    // component of cell
    if (cell instanceof ByteBufferExtendedCell) {
      out.writeShort(rowLen);
      ByteBufferUtils
          .copyBufferToStream((DataOutput) out, ((ByteBufferExtendedCell) cell).getRowByteBuffer(),
              ((ByteBufferExtendedCell) cell).getRowPosition(), rowLen);
      out.writeByte(fLen);
      ByteBufferUtils
          .copyBufferToStream((DataOutput) out, ((ByteBufferExtendedCell) cell).getFamilyByteBuffer(),
              ((ByteBufferExtendedCell) cell).getFamilyPosition(), fLen);
      ByteBufferUtils.copyBufferToStream((DataOutput) out,
          ((ByteBufferExtendedCell) cell).getQualifierByteBuffer(),
          ((ByteBufferExtendedCell) cell).getQualifierPosition(), qLen);
    } else {
      out.writeShort(rowLen);
      out.write(cell.getRowArray(), cell.getRowOffset(), rowLen);
      out.writeByte(fLen);
      out.write(cell.getFamilyArray(), cell.getFamilyOffset(), fLen);
      out.write(cell.getQualifierArray(), cell.getQualifierOffset(), qLen);
    }
    out.writeLong(cell.getTimestamp());
    out.writeByte(cell.getTypeByte());
  }

  /**
   * Writes the row from the given cell to the output stream excluding the common prefix
   * @param out The dataoutputstream to which the data has to be written
   * @param cell The cell whose contents has to be written
   * @param rlength the row length
   * @throws IOException
   * @deprecated As of 2.0. Will be removed in hbase-3.0
   */
  @Deprecated
  public static void writeRowSkippingBytes(DataOutputStream out, Cell cell, short rlength,
                                           int commonPrefix) throws IOException {
    if (cell instanceof ByteBufferExtendedCell) {
      ByteBufferUtils
          .copyBufferToStream((DataOutput) out, ((ByteBufferExtendedCell) cell).getRowByteBuffer(),
              ((ByteBufferExtendedCell) cell).getRowPosition() + commonPrefix, rlength - commonPrefix);
    } else {
      out.write(cell.getRowArray(), cell.getRowOffset() + commonPrefix, rlength - commonPrefix);
    }
  }

  /**
   * @param cell
   * @return The Key portion of the passed <code>cell</code> as a String.
   */
  public static String getCellKeyAsString(Cell cell) {
    return getCellKeyAsString(cell,
        c -> Bytes.toStringBinary(c.getRowArray(), c.getRowOffset(), c.getRowLength()));
  }

  /**
   * @param cell the cell to convert
   * @param rowConverter used to convert the row of the cell to a string
   * @return The Key portion of the passed <code>cell</code> as a String.
   */
  public static String getCellKeyAsString(Cell cell, Function<Cell, String> rowConverter) {
    StringBuilder sb = new StringBuilder(rowConverter.apply(cell));
    sb.append('/');
    sb.append(cell.getFamilyLength() == 0 ? "" :
        Bytes.toStringBinary(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
    // KeyValue only added ':' if family is non-null. Do same.
    if (cell.getFamilyLength() > 0) sb.append(':');
    sb.append(cell.getQualifierLength() == 0 ? "" :
        Bytes.toStringBinary(cell.getQualifierArray(), cell.getQualifierOffset(),
            cell.getQualifierLength()));
    sb.append('/');
    sb.append(KeyValue.humanReadableTimestamp(cell.getTimestamp()));
    sb.append('/');
    sb.append(Type.codeToType(cell.getTypeByte()));
    if (!(cell instanceof KeyValue.KeyOnlyKeyValue)) {
      sb.append("/vlen=");
      sb.append(cell.getValueLength());
    }
    sb.append("/seqid=");
    sb.append(cell.getSequenceId());
    return sb.toString();
  }

  /**
   * This method exists just to encapsulate how we serialize keys. To be replaced by a factory that
   * we query to figure what the Cell implementation is and then, what serialization engine to use
   * and further, how to serialize the key for inclusion in hfile index. TODO.
   * @param cell
   * @return The key portion of the Cell serialized in the old-school KeyValue way or null if passed
   *         a null <code>cell</code>
   * @deprecated As of HBase-2.0. Will be removed in HBase-3.0
   */
  @Deprecated
  public static byte[] getCellKeySerializedAsKeyValueKey(final Cell cell) {
    if (cell == null) return null;
    byte[] b = new byte[KeyValueUtil.keyLength(cell)];
    KeyValueUtil.appendKeyTo(cell, b, 0);
    return b;
  }

  /**
   * Write rowkey excluding the common part.
   * @param cell
   * @param rLen
   * @param commonPrefix
   * @param out
   * @throws IOException
   * @deprecated As of HBase-2.0. Will be removed in HBase-3.0
   */
  @Deprecated
  public static void writeRowKeyExcludingCommon(Cell cell, short rLen, int commonPrefix,
                                                DataOutputStream out) throws IOException {
    if (commonPrefix == 0) {
      out.writeShort(rLen);
    } else if (commonPrefix == 1) {
      out.writeByte((byte) rLen);
      commonPrefix--;
    } else {
      commonPrefix -= KeyValue.ROW_LENGTH_SIZE;
    }
    if (rLen > commonPrefix) {
      PrivateCellUtil.writeRowSkippingBytes(out, cell, rLen, commonPrefix);
    }
  }

  /**
   * Find length of common prefix in keys of the cells, considering key as byte[] if serialized in
   * {@link KeyValue}. The key format is &lt;2 bytes rk len&gt;&lt;rk&gt;&lt;1 byte cf
   * len&gt;&lt;cf&gt;&lt;qualifier&gt;&lt;8 bytes timestamp&gt;&lt;1 byte type&gt;
   * @param c1 the cell
   * @param c2 the cell
   * @param bypassFamilyCheck when true assume the family bytes same in both cells. Pass it as true
   *          when dealing with Cells in same CF so as to avoid some checks
   * @param withTsType when true check timestamp and type bytes also.
   * @return length of common prefix
   * @deprecated As of HBase-2.0. Will be removed in HBase-3.0
   */
  @Deprecated
  public static int findCommonPrefixInFlatKey(Cell c1, Cell c2, boolean bypassFamilyCheck,
                                              boolean withTsType) {
    // Compare the 2 bytes in RK length part
    short rLen1 = c1.getRowLength();
    short rLen2 = c2.getRowLength();
    int commonPrefix = KeyValue.ROW_LENGTH_SIZE;
    if (rLen1 != rLen2) {
      // early out when the RK length itself is not matching
      return ByteBufferUtils
          .findCommonPrefix(Bytes.toBytes(rLen1), 0, KeyValue.ROW_LENGTH_SIZE, Bytes.toBytes(rLen2),
              0, KeyValue.ROW_LENGTH_SIZE);
    }
    // Compare the RKs
    int rkCommonPrefix = 0;
    if (c1 instanceof ByteBufferExtendedCell && c2 instanceof ByteBufferExtendedCell) {
      rkCommonPrefix = ByteBufferUtils
          .findCommonPrefix(((ByteBufferExtendedCell) c1).getRowByteBuffer(),
              ((ByteBufferExtendedCell) c1).getRowPosition(), rLen1,
              ((ByteBufferExtendedCell) c2).getRowByteBuffer(),
              ((ByteBufferExtendedCell) c2).getRowPosition(), rLen2);
    } else {
      // There cannot be a case where one cell is BBCell and other is KeyValue. This flow comes
      // either
      // in flush or compactions. In flushes both cells are KV and in case of compaction it will be
      // either
      // KV or BBCell
      rkCommonPrefix = ByteBufferUtils
          .findCommonPrefix(c1.getRowArray(), c1.getRowOffset(), rLen1, c2.getRowArray(),
              c2.getRowOffset(), rLen2);
    }
    commonPrefix += rkCommonPrefix;
    if (rkCommonPrefix != rLen1) {
      // Early out when RK is not fully matching.
      return commonPrefix;
    }
    // Compare 1 byte CF length part
    byte fLen1 = c1.getFamilyLength();
    if (bypassFamilyCheck) {
      // This flag will be true when caller is sure that the family will be same for both the cells
      // Just make commonPrefix to increment by the family part
      commonPrefix += KeyValue.FAMILY_LENGTH_SIZE + fLen1;
    } else {
      byte fLen2 = c2.getFamilyLength();
      if (fLen1 != fLen2) {
        // early out when the CF length itself is not matching
        return commonPrefix;
      }
      // CF lengths are same so there is one more byte common in key part
      commonPrefix += KeyValue.FAMILY_LENGTH_SIZE;
      // Compare the CF names
      int fCommonPrefix;
      if (c1 instanceof ByteBufferExtendedCell && c2 instanceof ByteBufferExtendedCell) {
        fCommonPrefix = ByteBufferUtils
            .findCommonPrefix(((ByteBufferExtendedCell) c1).getFamilyByteBuffer(),
                ((ByteBufferExtendedCell) c1).getFamilyPosition(), fLen1,
                ((ByteBufferExtendedCell) c2).getFamilyByteBuffer(),
                ((ByteBufferExtendedCell) c2).getFamilyPosition(), fLen2);
      } else {
        fCommonPrefix = ByteBufferUtils
            .findCommonPrefix(c1.getFamilyArray(), c1.getFamilyOffset(), fLen1, c2.getFamilyArray(),
                c2.getFamilyOffset(), fLen2);
      }
      commonPrefix += fCommonPrefix;
      if (fCommonPrefix != fLen1) {
        return commonPrefix;
      }
    }
    // Compare the Qualifiers
    int qLen1 = c1.getQualifierLength();
    int qLen2 = c2.getQualifierLength();
    int qCommon;
    if (c1 instanceof ByteBufferExtendedCell && c2 instanceof ByteBufferExtendedCell) {
      qCommon = ByteBufferUtils
          .findCommonPrefix(((ByteBufferExtendedCell) c1).getQualifierByteBuffer(),
              ((ByteBufferExtendedCell) c1).getQualifierPosition(), qLen1,
              ((ByteBufferExtendedCell) c2).getQualifierByteBuffer(),
              ((ByteBufferExtendedCell) c2).getQualifierPosition(), qLen2);
    } else {
      qCommon = ByteBufferUtils
          .findCommonPrefix(c1.getQualifierArray(), c1.getQualifierOffset(), qLen1,
              c2.getQualifierArray(), c2.getQualifierOffset(), qLen2);
    }
    commonPrefix += qCommon;
    if (!withTsType || Math.max(qLen1, qLen2) != qCommon) {
      return commonPrefix;
    }
    // Compare the timestamp parts
    int tsCommonPrefix = ByteBufferUtils
        .findCommonPrefix(Bytes.toBytes(c1.getTimestamp()), 0, KeyValue.TIMESTAMP_SIZE,
            Bytes.toBytes(c2.getTimestamp()), 0, KeyValue.TIMESTAMP_SIZE);
    commonPrefix += tsCommonPrefix;
    if (tsCommonPrefix != KeyValue.TIMESTAMP_SIZE) {
      return commonPrefix;
    }
    // Compare the type
    if (c1.getTypeByte() == c2.getTypeByte()) {
      commonPrefix += KeyValue.TYPE_SIZE;
    }
    return commonPrefix;
  }

  /** Returns a string representation of the cell */
  public static String toString(Cell cell, boolean verbose) {
    if (cell == null) {
      return "";
    }
    StringBuilder builder = new StringBuilder();
    String keyStr = getCellKeyAsString(cell);

    String tag = null;
    String value = null;
    if (verbose) {
      // TODO: pretty print tags as well
      if (cell.getTagsLength() > 0) {
        tag = Bytes.toStringBinary(cell.getTagsArray(), cell.getTagsOffset(), cell.getTagsLength());
      }
      if (!(cell instanceof KeyValue.KeyOnlyKeyValue)) {
        value = Bytes.toStringBinary(cell.getValueArray(), cell.getValueOffset(),
            cell.getValueLength());
      }
    }

    builder.append(keyStr);
    if (tag != null && !tag.isEmpty()) {
      builder.append("/").append(tag);
    }
    if (value != null) {
      builder.append("/").append(value);
    }

    return builder.toString();
  }

  /***************** special cases ****************************/

  /**
   * special case for Cell.equals
   * @deprecated As of HBase-2.0. Will be removed in HBase-3.0
   */
  @Deprecated
  public static boolean equalsIgnoreMvccVersion(Cell a, Cell b) {
    // row
    boolean res = matchingRows(a, b);
    if (!res) return res;

    // family
    res = matchingColumn(a, b);
    if (!res) return res;

    // timestamp: later sorts first
    if (!matchingTimestamp(a, b)) return false;

    // type
    int c = (0xff & b.getTypeByte()) - (0xff & a.getTypeByte());
    if (c != 0) return false;
    else return true;
  }

  /**************** equals ****************************/

  public static boolean equals(Cell a, Cell b) {
    return matchingRows(a, b) && matchingFamily(a, b) && matchingQualifier(a, b)
        && matchingTimestamp(a, b) && PrivateCellUtil.matchingType(a, b);
  }

  public static boolean matchingTimestamp(Cell a, Cell b) {
    return CellComparator.getInstance().compareTimestamps(a.getTimestamp(), b.getTimestamp()) == 0;
  }

  /**
   * @deprecated As of HBase-2.0. Will be removed in HBase-3.0
   */
  @Deprecated
  public static boolean matchingType(Cell a, Cell b) {
    return a.getTypeByte() == b.getTypeByte();
  }

  /**
   * Compares the row of two keyvalues for equality
   * @param left
   * @param right
   * @return True if rows match.
   */
  public static boolean matchingRows(final Cell left, final Cell right) {
    short lrowlength = left.getRowLength();
    short rrowlength = right.getRowLength();
    return matchingRows(left, lrowlength, right, rrowlength);
  }

  public static boolean matchingRows(final Cell left, final short lrowlength, final Cell right,
                                     final short rrowlength) {
    if (lrowlength != rrowlength) return false;
    if (left instanceof ByteBufferExtendedCell && right instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.equals(((ByteBufferExtendedCell) left).getRowByteBuffer(),
          ((ByteBufferExtendedCell) left).getRowPosition(), lrowlength,
          ((ByteBufferExtendedCell) right).getRowByteBuffer(),
          ((ByteBufferExtendedCell) right).getRowPosition(), rrowlength);
    }
    if (left instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.equals(((ByteBufferExtendedCell) left).getRowByteBuffer(),
          ((ByteBufferExtendedCell) left).getRowPosition(), lrowlength, right.getRowArray(),
          right.getRowOffset(), rrowlength);
    }
    if (right instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.equals(((ByteBufferExtendedCell) right).getRowByteBuffer(),
          ((ByteBufferExtendedCell) right).getRowPosition(), rrowlength, left.getRowArray(),
          left.getRowOffset(), lrowlength);
    }
    return Bytes.equals(left.getRowArray(), left.getRowOffset(), lrowlength, right.getRowArray(),
        right.getRowOffset(), rrowlength);
  }

  /**
   * Compares the row and column of two keyvalues for equality
   * @param left
   * @param right
   * @return True if same row and column.
   */
  public static boolean matchingRowColumn(final Cell left, final Cell right) {
    short lrowlength = left.getRowLength();
    short rrowlength = right.getRowLength();
    // match length
    if (lrowlength != rrowlength) {
      return false;
    }

    byte lfamlength = left.getFamilyLength();
    byte rfamlength = right.getFamilyLength();
    if (lfamlength != rfamlength) {
      return false;
    }

    int lqlength = left.getQualifierLength();
    int rqlength = right.getQualifierLength();
    if (lqlength != rqlength) {
      return false;
    }

    if (!matchingRows(left, lrowlength, right, rrowlength)) {
      return false;
    }
    return matchingColumn(left, lfamlength, lqlength, right, rfamlength, rqlength);
  }

  public static boolean matchingRowColumnBytes(final Cell left, final Cell right) {
    int lrowlength = left.getRowLength();
    int rrowlength = right.getRowLength();
    int lfamlength = left.getFamilyLength();
    int rfamlength = right.getFamilyLength();
    int lqlength = left.getQualifierLength();
    int rqlength = right.getQualifierLength();

    // match length
    if ((lrowlength != rrowlength) || (lfamlength != rfamlength) || (lqlength != rqlength)) {
      return false;
    }

    // match row
    if (!Bytes.equals(left.getRowArray(), left.getRowOffset(), lrowlength, right.getRowArray(),
        right.getRowOffset(), rrowlength)) {
      return false;
    }
    //match family
    if (!Bytes.equals(left.getFamilyArray(), left.getFamilyOffset(), lfamlength,
        right.getFamilyArray(), right.getFamilyOffset(), rfamlength)) {
      return false;
    }
    //match qualifier
    return Bytes.equals(left.getQualifierArray(), left.getQualifierOffset(),
        lqlength, right.getQualifierArray(), right.getQualifierOffset(),
        rqlength);
  }

  /**
   * Compares the cell's qualifier with the given byte[]
   * @param left the cell for which the qualifier has to be compared
   * @param right the byte[] having the qualifier
   * @param rOffset the offset of the qualifier
   * @param rLength the length of the qualifier
   * @return greater than 0 if left cell's qualifier is bigger than byte[], lesser than 0 if left
   *         cell's qualifier is lesser than byte[] and 0 otherwise
   */
  public final static int compareQualifiers(Cell left, byte[] right, int rOffset, int rLength) {
    if (left instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.compareTo(((ByteBufferExtendedCell) left).getQualifierByteBuffer(),
          ((ByteBufferExtendedCell) left).getQualifierPosition(),
          left.getQualifierLength(), right, rOffset, rLength);
    }
    return Bytes.compareTo(left.getQualifierArray(), left.getQualifierOffset(),
        left.getQualifierLength(), right, rOffset, rLength);
  }

  /**
   * Used when a cell needs to be compared with a key byte[] such as cases of finding the index from
   * the index block, bloom keys from the bloom blocks This byte[] is expected to be serialized in
   * the KeyValue serialization format If the KeyValue (Cell's) serialization format changes this
   * method cannot be used.
   * @param comparator the cell comparator
   * @param left the cell to be compared
   * @param key the serialized key part of a KeyValue
   * @param offset the offset in the key byte[]
   * @param length the length of the key byte[]
   * @return an int greater than 0 if left is greater than right lesser than 0 if left is lesser
   *         than right equal to 0 if left is equal to right
   * @deprecated As of HBase-2.0. Will be removed in HBase-3.0
   */
  @InterfaceAudience.Private
  @Deprecated
  public static final int compare(CellComparator comparator, Cell left, byte[] key, int offset,
                                  int length) {
    // row
    short rrowlength = Bytes.toShort(key, offset);
    int c = comparator.compareRows(left, key, offset + Bytes.SIZEOF_SHORT, rrowlength);
    if (c != 0) return c;

    // Compare the rest of the two KVs without making any assumptions about
    // the common prefix. This function will not compare rows anyway, so we
    // don't need to tell it that the common prefix includes the row.
    return PrivateCellUtil.compareWithoutRow(comparator, left, key, offset, length, rrowlength);
  }

  /**
   * Compares the cell's family with the given byte[]
   * @param left the cell for which the family has to be compared
   * @param right the byte[] having the family
   * @param roffset the offset of the family
   * @param rlength the length of the family
   * @return greater than 0 if left cell's family is bigger than byte[], lesser than 0 if left
   *         cell's family is lesser than byte[] and 0 otherwise
   */
  public final static int compareFamilies(Cell left, byte[] right, int roffset, int rlength) {
    if (left instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.compareTo(((ByteBufferExtendedCell) left).getFamilyByteBuffer(),
          ((ByteBufferExtendedCell) left).getFamilyPosition(), left.getFamilyLength(), right, roffset,
          rlength);
    }
    return Bytes.compareTo(left.getFamilyArray(), left.getFamilyOffset(), left.getFamilyLength(),
        right, roffset, rlength);
  }

  /**
   * Compares the cell's column (family and qualifier) with the given byte[]
   * @param left the cell for which the column has to be compared
   * @param right the byte[] having the column
   * @param rfoffset the offset of the family
   * @param rflength the length of the family
   * @param rqoffset the offset of the qualifier
   * @param rqlength the length of the qualifier
   * @return greater than 0 if left cell's column is bigger than byte[], lesser than 0 if left
   *         cell's column is lesser than byte[] and 0 otherwise
   */
  public final static int compareColumns(Cell left, byte[] right, int rfoffset, int rflength,
                                         int rqoffset, int rqlength) {
    int diff = compareFamilies(left, right, rfoffset, rflength);
    if (diff != 0) return diff;
    return compareQualifiers(left, right, rqoffset, rqlength);
  }
}
