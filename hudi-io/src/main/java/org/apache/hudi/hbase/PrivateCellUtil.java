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

import static org.apache.hudi.hbase.HConstants.EMPTY_BYTE_ARRAY;
import static org.apache.hudi.hbase.Tag.TAG_LENGTH_SIZE;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.apache.hudi.hbase.filter.ByteArrayComparable;
import org.apache.hudi.hbase.io.TagCompressionContext;
import org.apache.hudi.hbase.io.util.Dictionary;
import org.apache.hudi.hbase.io.util.StreamUtils;
import org.apache.hudi.hbase.util.ByteBufferUtils;
import org.apache.hudi.hbase.util.ByteRange;
import org.apache.hudi.hbase.util.Bytes;
import org.apache.hudi.hbase.util.ClassSize;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Utility methods helpful slinging {@link Cell} instances. It has more powerful and
 * rich set of APIs than those in {@link CellUtil} for internal usage.
 */
@InterfaceAudience.Private
public final class PrivateCellUtil {

  /**
   * Private constructor to keep this class from being instantiated.
   */
  private PrivateCellUtil() {
  }

  /******************* ByteRange *******************************/

  public static ByteRange fillRowRange(Cell cell, ByteRange range) {
    return range.set(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
  }

  public static ByteRange fillFamilyRange(Cell cell, ByteRange range) {
    return range.set(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
  }

  public static ByteRange fillQualifierRange(Cell cell, ByteRange range) {
    return range.set(cell.getQualifierArray(), cell.getQualifierOffset(),
        cell.getQualifierLength());
  }

  public static ByteRange fillValueRange(Cell cell, ByteRange range) {
    return range.set(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
  }

  public static ByteRange fillTagRange(Cell cell, ByteRange range) {
    return range.set(cell.getTagsArray(), cell.getTagsOffset(), cell.getTagsLength());
  }

  /********************* misc *************************************/

  public static byte getRowByte(Cell cell, int index) {
    if (cell instanceof ByteBufferExtendedCell) {
      return ((ByteBufferExtendedCell) cell).getRowByteBuffer()
          .get(((ByteBufferExtendedCell) cell).getRowPosition() + index);
    }
    return cell.getRowArray()[cell.getRowOffset() + index];
  }

  public static byte getQualifierByte(Cell cell, int index) {
    if (cell instanceof ByteBufferExtendedCell) {
      return ((ByteBufferExtendedCell) cell).getQualifierByteBuffer()
          .get(((ByteBufferExtendedCell) cell).getQualifierPosition() + index);
    }
    return cell.getQualifierArray()[cell.getQualifierOffset() + index];
  }

  public static ByteBuffer getValueBufferShallowCopy(Cell cell) {
    ByteBuffer buffer =
        ByteBuffer.wrap(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
    return buffer;
  }

  /**
   * @return A new cell which is having the extra tags also added to it.
   */
  public static Cell createCell(Cell cell, List<Tag> tags) {
    return createCell(cell, TagUtil.fromList(tags));
  }

  /**
   * @return A new cell which is having the extra tags also added to it.
   */
  public static Cell createCell(Cell cell, byte[] tags) {
    if (cell instanceof ByteBufferExtendedCell) {
      return new TagRewriteByteBufferExtendedCell((ByteBufferExtendedCell) cell, tags);
    }
    return new TagRewriteCell(cell, tags);
  }

  public static Cell createCell(Cell cell, byte[] value, byte[] tags) {
    if (cell instanceof ByteBufferExtendedCell) {
      return new ValueAndTagRewriteByteBufferExtendedCell((ByteBufferExtendedCell) cell,
          value, tags);
    }
    return new ValueAndTagRewriteCell(cell, value, tags);
  }

  /**
   * This can be used when a Cell has to change with addition/removal of one or more tags. This is
   * an efficient way to do so in which only the tags bytes part need to recreated and copied. All
   * other parts, refer to the original Cell.
   */
  static class TagRewriteCell implements ExtendedCell {
    protected Cell cell;
    protected byte[] tags;
    private static final int HEAP_SIZE_OVERHEAD = ClassSize.OBJECT + 2 * ClassSize.REFERENCE;

    /**
     * @param cell The original Cell which it rewrites
     * @param tags the tags bytes. The array suppose to contain the tags bytes alone.
     */
    public TagRewriteCell(Cell cell, byte[] tags) {
      assert cell instanceof ExtendedCell;
      assert tags != null;
      this.cell = cell;
      this.tags = tags;
      // tag offset will be treated as 0 and length this.tags.length
      if (this.cell instanceof TagRewriteCell) {
        // Cleaning the ref so that the byte[] can be GCed
        ((TagRewriteCell) this.cell).tags = null;
      }
    }

    @Override
    public byte[] getRowArray() {
      return cell.getRowArray();
    }

    @Override
    public int getRowOffset() {
      return cell.getRowOffset();
    }

    @Override
    public short getRowLength() {
      return cell.getRowLength();
    }

    @Override
    public byte[] getFamilyArray() {
      return cell.getFamilyArray();
    }

    @Override
    public int getFamilyOffset() {
      return cell.getFamilyOffset();
    }

    @Override
    public byte getFamilyLength() {
      return cell.getFamilyLength();
    }

    @Override
    public byte[] getQualifierArray() {
      return cell.getQualifierArray();
    }

    @Override
    public int getQualifierOffset() {
      return cell.getQualifierOffset();
    }

    @Override
    public int getQualifierLength() {
      return cell.getQualifierLength();
    }

    @Override
    public long getTimestamp() {
      return cell.getTimestamp();
    }

    @Override
    public byte getTypeByte() {
      return cell.getTypeByte();
    }

    @Override
    public long getSequenceId() {
      return cell.getSequenceId();
    }

    @Override
    public byte[] getValueArray() {
      return cell.getValueArray();
    }

    @Override
    public int getValueOffset() {
      return cell.getValueOffset();
    }

    @Override
    public int getValueLength() {
      return cell.getValueLength();
    }

    @Override
    public byte[] getTagsArray() {
      return this.tags;
    }

    @Override
    public int getTagsOffset() {
      return 0;
    }

    @Override
    public int getTagsLength() {
      if (null == this.tags) {
        // Nulled out tags array optimization in constructor
        return 0;
      }
      return this.tags.length;
    }

    @Override
    public long heapSize() {
      long sum = HEAP_SIZE_OVERHEAD + cell.heapSize();
      if (this.tags != null) {
        sum += ClassSize.sizeOf(this.tags);
      }
      return sum;
    }

    @Override
    public void setTimestamp(long ts) throws IOException {
      // The incoming cell is supposed to be ExtendedCell type.
      PrivateCellUtil.setTimestamp(cell, ts);
    }

    @Override
    public void setTimestamp(byte[] ts) throws IOException {
      // The incoming cell is supposed to be ExtendedCell type.
      PrivateCellUtil.setTimestamp(cell, ts);
    }

    @Override
    public void setSequenceId(long seqId) throws IOException {
      // The incoming cell is supposed to be ExtendedCell type.
      PrivateCellUtil.setSequenceId(cell, seqId);
    }

    @Override
    public int write(OutputStream out, boolean withTags) throws IOException {
      int len = ((ExtendedCell) this.cell).write(out, false);
      if (withTags && this.tags != null) {
        // Write the tagsLength 2 bytes
        out.write((byte) (0xff & (this.tags.length >> 8)));
        out.write((byte) (0xff & this.tags.length));
        out.write(this.tags);
        len += KeyValue.TAGS_LENGTH_SIZE + this.tags.length;
      }
      return len;
    }

    @Override
    public int getSerializedSize(boolean withTags) {
      int len = ((ExtendedCell) this.cell).getSerializedSize(false);
      if (withTags && this.tags != null) {
        len += KeyValue.TAGS_LENGTH_SIZE + this.tags.length;
      }
      return len;
    }

    @Override
    public void write(ByteBuffer buf, int offset) {
      offset = KeyValueUtil.appendTo(this.cell, buf, offset, false);
      int tagsLen = this.tags == null ? 0 : this.tags.length;
      if (tagsLen > 0) {
        offset = ByteBufferUtils.putAsShort(buf, offset, tagsLen);
        ByteBufferUtils.copyFromArrayToBuffer(buf, offset, this.tags, 0, tagsLen);
      }
    }

    @Override
    public ExtendedCell deepClone() {
      Cell clonedBaseCell = ((ExtendedCell) this.cell).deepClone();
      return new TagRewriteCell(clonedBaseCell, this.tags);
    }
  }

  static class TagRewriteByteBufferExtendedCell extends ByteBufferExtendedCell {

    protected ByteBufferExtendedCell cell;
    protected byte[] tags;
    private static final int HEAP_SIZE_OVERHEAD = ClassSize.OBJECT + 2 * ClassSize.REFERENCE;

    /**
     * @param cell The original ByteBufferExtendedCell which it rewrites
     * @param tags the tags bytes. The array suppose to contain the tags bytes alone.
     */
    public TagRewriteByteBufferExtendedCell(ByteBufferExtendedCell cell, byte[] tags) {
      assert tags != null;
      this.cell = cell;
      this.tags = tags;
      // tag offset will be treated as 0 and length this.tags.length
      if (this.cell instanceof TagRewriteByteBufferExtendedCell) {
        // Cleaning the ref so that the byte[] can be GCed
        ((TagRewriteByteBufferExtendedCell) this.cell).tags = null;
      }
    }

    @Override
    public byte[] getRowArray() {
      return this.cell.getRowArray();
    }

    @Override
    public int getRowOffset() {
      return this.cell.getRowOffset();
    }

    @Override
    public short getRowLength() {
      return this.cell.getRowLength();
    }

    @Override
    public byte[] getFamilyArray() {
      return this.cell.getFamilyArray();
    }

    @Override
    public int getFamilyOffset() {
      return this.cell.getFamilyOffset();
    }

    @Override
    public byte getFamilyLength() {
      return this.cell.getFamilyLength();
    }

    @Override
    public byte[] getQualifierArray() {
      return this.cell.getQualifierArray();
    }

    @Override
    public int getQualifierOffset() {
      return this.cell.getQualifierOffset();
    }

    @Override
    public int getQualifierLength() {
      return this.cell.getQualifierLength();
    }

    @Override
    public long getTimestamp() {
      return this.cell.getTimestamp();
    }

    @Override
    public byte getTypeByte() {
      return this.cell.getTypeByte();
    }

    @Override
    public long getSequenceId() {
      return this.cell.getSequenceId();
    }

    @Override
    public byte[] getValueArray() {
      return this.cell.getValueArray();
    }

    @Override
    public int getValueOffset() {
      return this.cell.getValueOffset();
    }

    @Override
    public int getValueLength() {
      return this.cell.getValueLength();
    }

    @Override
    public byte[] getTagsArray() {
      return this.tags;
    }

    @Override
    public int getTagsOffset() {
      return 0;
    }

    @Override
    public int getTagsLength() {
      if (null == this.tags) {
        // Nulled out tags array optimization in constructor
        return 0;
      }
      return this.tags.length;
    }

    @Override
    public void setSequenceId(long seqId) throws IOException {
      PrivateCellUtil.setSequenceId(this.cell, seqId);
    }

    @Override
    public void setTimestamp(long ts) throws IOException {
      PrivateCellUtil.setTimestamp(this.cell, ts);
    }

    @Override
    public void setTimestamp(byte[] ts) throws IOException {
      PrivateCellUtil.setTimestamp(this.cell, ts);
    }

    @Override
    public long heapSize() {
      long sum = HEAP_SIZE_OVERHEAD + cell.heapSize();
      // this.tags is on heap byte[]
      if (this.tags != null) {
        sum += ClassSize.sizeOf(this.tags);
      }
      return sum;
    }

    @Override
    public int write(OutputStream out, boolean withTags) throws IOException {
      int len = ((ExtendedCell) this.cell).write(out, false);
      if (withTags && this.tags != null) {
        // Write the tagsLength 2 bytes
        out.write((byte) (0xff & (this.tags.length >> 8)));
        out.write((byte) (0xff & this.tags.length));
        out.write(this.tags);
        len += KeyValue.TAGS_LENGTH_SIZE + this.tags.length;
      }
      return len;
    }

    @Override
    public int getSerializedSize(boolean withTags) {
      int len = ((ExtendedCell) this.cell).getSerializedSize(false);
      if (withTags && this.tags != null) {
        len += KeyValue.TAGS_LENGTH_SIZE + this.tags.length;
      }
      return len;
    }

    @Override
    public void write(ByteBuffer buf, int offset) {
      offset = KeyValueUtil.appendTo(this.cell, buf, offset, false);
      int tagsLen = this.tags == null ? 0 : this.tags.length;
      if (tagsLen > 0) {
        offset = ByteBufferUtils.putAsShort(buf, offset, tagsLen);
        ByteBufferUtils.copyFromArrayToBuffer(buf, offset, this.tags, 0, tagsLen);
      }
    }

    @Override
    public ExtendedCell deepClone() {
      Cell clonedBaseCell = ((ExtendedCell) this.cell).deepClone();
      if (clonedBaseCell instanceof ByteBufferExtendedCell) {
        return new TagRewriteByteBufferExtendedCell((ByteBufferExtendedCell) clonedBaseCell,
            this.tags);
      }
      return new TagRewriteCell(clonedBaseCell, this.tags);
    }

    @Override
    public ByteBuffer getRowByteBuffer() {
      return this.cell.getRowByteBuffer();
    }

    @Override
    public int getRowPosition() {
      return this.cell.getRowPosition();
    }

    @Override
    public ByteBuffer getFamilyByteBuffer() {
      return this.cell.getFamilyByteBuffer();
    }

    @Override
    public int getFamilyPosition() {
      return this.cell.getFamilyPosition();
    }

    @Override
    public ByteBuffer getQualifierByteBuffer() {
      return this.cell.getQualifierByteBuffer();
    }

    @Override
    public int getQualifierPosition() {
      return this.cell.getQualifierPosition();
    }

    @Override
    public ByteBuffer getValueByteBuffer() {
      return this.cell.getValueByteBuffer();
    }

    @Override
    public int getValuePosition() {
      return this.cell.getValuePosition();
    }

    @Override
    public ByteBuffer getTagsByteBuffer() {
      return this.tags == null ? HConstants.EMPTY_BYTE_BUFFER : ByteBuffer.wrap(this.tags);
    }

    @Override
    public int getTagsPosition() {
      return 0;
    }
  }

  static class ValueAndTagRewriteCell extends TagRewriteCell {

    protected byte[] value;

    public ValueAndTagRewriteCell(Cell cell, byte[] value, byte[] tags) {
      super(cell, tags);
      this.value = value;
    }

    @Override
    public byte[] getValueArray() {
      return this.value;
    }

    @Override
    public int getValueOffset() {
      return 0;
    }

    @Override
    public int getValueLength() {
      return this.value == null ? 0 : this.value.length;
    }

    @Override
    public long heapSize() {
      long sum = ClassSize.REFERENCE + super.heapSize();
      if (this.value != null) {
        sum += ClassSize.sizeOf(this.value);
      }
      return sum;
    }

    @Override
    public int write(OutputStream out, boolean withTags) throws IOException {
      return write(out, withTags, this.cell, this.value, this.tags);
    }

    /**
     * Made into a static method so as to reuse the logic within
     * ValueAndTagRewriteByteBufferExtendedCell
     */
    static int write(OutputStream out, boolean withTags, Cell cell, byte[] value, byte[] tags)
        throws IOException {
      int valLen = value == null ? 0 : value.length;
      ByteBufferUtils.putInt(out, KeyValueUtil.keyLength(cell));// Key length
      ByteBufferUtils.putInt(out, valLen);// Value length
      int len = 2 * Bytes.SIZEOF_INT;
      len += writeFlatKey(cell, out);// Key
      if (valLen > 0) {
        out.write(value);// Value
      }
      len += valLen;
      if (withTags && tags != null) {
        // Write the tagsLength 2 bytes
        out.write((byte) (0xff & (tags.length >> 8)));
        out.write((byte) (0xff & tags.length));
        out.write(tags);
        len += KeyValue.TAGS_LENGTH_SIZE + tags.length;
      }
      return len;
    }

    @Override
    public int getSerializedSize(boolean withTags) {
      return super.getSerializedSize(withTags) - this.cell.getValueLength() + this.value.length;
    }

    @Override
    public void write(ByteBuffer buf, int offset) {
      write(buf, offset, this.cell, this.value, this.tags);
    }

    /**
     * Made into a static method so as to reuse the logic
     * within ValueAndTagRewriteByteBufferExtendedCell
     */
    static void write(ByteBuffer buf, int offset, Cell cell, byte[] value, byte[] tags) {
      offset = ByteBufferUtils.putInt(buf, offset, KeyValueUtil.keyLength(cell));// Key length
      offset = ByteBufferUtils.putInt(buf, offset, value.length);// Value length
      offset = KeyValueUtil.appendKeyTo(cell, buf, offset);
      ByteBufferUtils.copyFromArrayToBuffer(buf, offset, value, 0, value.length);
      offset += value.length;
      int tagsLen = tags == null ? 0 : tags.length;
      if (tagsLen > 0) {
        offset = ByteBufferUtils.putAsShort(buf, offset, tagsLen);
        ByteBufferUtils.copyFromArrayToBuffer(buf, offset, tags, 0, tagsLen);
      }
    }

    @Override
    public ExtendedCell deepClone() {
      Cell clonedBaseCell = ((ExtendedCell) this.cell).deepClone();
      return new ValueAndTagRewriteCell(clonedBaseCell, this.value, this.tags);
    }
  }

  static class ValueAndTagRewriteByteBufferExtendedCell extends TagRewriteByteBufferExtendedCell {

    protected byte[] value;

    public ValueAndTagRewriteByteBufferExtendedCell(ByteBufferExtendedCell cell,
                                                    byte[] value, byte[] tags) {
      super(cell, tags);
      this.value = value;
    }

    @Override
    public byte[] getValueArray() {
      return this.value;
    }

    @Override
    public int getValueOffset() {
      return 0;
    }

    @Override
    public int getValueLength() {
      return this.value == null ? 0 : this.value.length;
    }

    @Override
    public ByteBuffer getValueByteBuffer() {
      return ByteBuffer.wrap(this.value);
    }

    @Override
    public int getValuePosition() {
      return 0;
    }

    @Override
    public long heapSize() {
      long sum = ClassSize.REFERENCE + super.heapSize();
      if (this.value != null) {
        sum += ClassSize.sizeOf(this.value);
      }
      return sum;
    }

    @Override
    public int write(OutputStream out, boolean withTags) throws IOException {
      return ValueAndTagRewriteCell.write(out, withTags, this.cell, this.value, this.tags);
    }

    @Override
    public int getSerializedSize(boolean withTags) {
      return super.getSerializedSize(withTags) - this.cell.getValueLength() + this.value.length;
    }

    @Override
    public void write(ByteBuffer buf, int offset) {
      ValueAndTagRewriteCell.write(buf, offset, this.cell, this.value, this.tags);
    }

    @Override
    public ExtendedCell deepClone() {
      Cell clonedBaseCell = this.cell.deepClone();
      if (clonedBaseCell instanceof ByteBufferExtendedCell) {
        return new ValueAndTagRewriteByteBufferExtendedCell(
            (ByteBufferExtendedCell) clonedBaseCell, this.value, this.tags);
      }
      return new ValueAndTagRewriteCell(clonedBaseCell, this.value, this.tags);
    }
  }

  public static boolean matchingRows(final Cell left, final byte[] buf, final int offset,
                                     final int length) {
    if (left instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.equals(((ByteBufferExtendedCell) left).getRowByteBuffer(),
          ((ByteBufferExtendedCell) left).getRowPosition(), left.getRowLength(),
          buf, offset, length);
    }
    return Bytes.equals(left.getRowArray(), left.getRowOffset(), left.getRowLength(), buf, offset,
        length);
  }

  public static boolean matchingFamily(final Cell left, final byte[] buf, final int offset,
                                       final int length) {
    if (left instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.equals(((ByteBufferExtendedCell) left).getFamilyByteBuffer(),
          ((ByteBufferExtendedCell) left).getFamilyPosition(), left.getFamilyLength(),
          buf, offset, length);
    }
    return Bytes.equals(left.getFamilyArray(), left.getFamilyOffset(), left.getFamilyLength(), buf,
        offset, length);
  }

  /**
   * Finds if the qualifier part of the cell and the KV serialized byte[] are equal
   * @param left the cell with which we need to match the qualifier
   * @param buf the serialized keyvalue format byte[]
   * @param offset the offset of the qualifier in the byte[]
   * @param length the length of the qualifier in the byte[]
   * @return true if the qualifier matches, false otherwise
   */
  public static boolean matchingQualifier(final Cell left, final byte[] buf, final int offset,
                                          final int length) {
    if (buf == null) {
      return left.getQualifierLength() == 0;
    }
    if (left instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.equals(((ByteBufferExtendedCell) left).getQualifierByteBuffer(),
          ((ByteBufferExtendedCell) left).getQualifierPosition(), left.getQualifierLength(),
          buf, offset, length);
    }
    return Bytes.equals(left.getQualifierArray(), left.getQualifierOffset(),
        left.getQualifierLength(), buf, offset, length);
  }

  /**
   * Finds if the start of the qualifier part of the Cell matches <code>buf</code>
   * @param left the cell with which we need to match the qualifier
   * @param startsWith the serialized keyvalue format byte[]
   * @return true if the qualifier have same staring characters, false otherwise
   */
  public static boolean qualifierStartsWith(final Cell left, final byte[] startsWith) {
    if (startsWith == null || startsWith.length == 0) {
      throw new IllegalArgumentException("Cannot pass an empty startsWith");
    }
    if (left.getQualifierLength() < startsWith.length) {
      return false;
    }
    if (left instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.equals(((ByteBufferExtendedCell) left).getQualifierByteBuffer(),
          ((ByteBufferExtendedCell) left).getQualifierPosition(), startsWith.length,
          startsWith, 0, startsWith.length);
    }
    return Bytes.equals(left.getQualifierArray(), left.getQualifierOffset(),
        startsWith.length, startsWith, 0, startsWith.length);
  }

  public static boolean matchingColumn(final Cell left, final byte[] fam, final int foffset,
                                       final int flength, final byte[] qual, final int qoffset, final int qlength) {
    if (!matchingFamily(left, fam, foffset, flength)) {
      return false;
    }
    return matchingQualifier(left, qual, qoffset, qlength);
  }

  public static boolean matchingValue(final Cell left, final Cell right, int lvlength,
                                      int rvlength) {
    if (left instanceof ByteBufferExtendedCell && right instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.equals(((ByteBufferExtendedCell) left).getValueByteBuffer(),
          ((ByteBufferExtendedCell) left).getValuePosition(), lvlength,
          ((ByteBufferExtendedCell) right).getValueByteBuffer(),
          ((ByteBufferExtendedCell) right).getValuePosition(), rvlength);
    }
    if (left instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.equals(((ByteBufferExtendedCell) left).getValueByteBuffer(),
          ((ByteBufferExtendedCell) left).getValuePosition(), lvlength, right.getValueArray(),
          right.getValueOffset(), rvlength);
    }
    if (right instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.equals(((ByteBufferExtendedCell) right).getValueByteBuffer(),
          ((ByteBufferExtendedCell) right).getValuePosition(), rvlength, left.getValueArray(),
          left.getValueOffset(), lvlength);
    }
    return Bytes
        .equals(left.getValueArray(), left.getValueOffset(), lvlength, right.getValueArray(),
            right.getValueOffset(), rvlength);
  }

  public static boolean matchingType(Cell a, Cell b) {
    return a.getTypeByte() == b.getTypeByte();
  }

  public static boolean matchingTags(final Cell left, final Cell right, int llength,
                                     int rlength) {
    if (left instanceof ByteBufferExtendedCell && right instanceof ByteBufferExtendedCell) {
      ByteBufferExtendedCell leftBBCell = (ByteBufferExtendedCell) left;
      ByteBufferExtendedCell rightBBCell = (ByteBufferExtendedCell) right;
      return ByteBufferUtils.equals(
          leftBBCell.getTagsByteBuffer(), leftBBCell.getTagsPosition(), llength,
          rightBBCell.getTagsByteBuffer(),rightBBCell.getTagsPosition(), rlength);
    }
    if (left instanceof ByteBufferExtendedCell) {
      ByteBufferExtendedCell leftBBCell = (ByteBufferExtendedCell) left;
      return ByteBufferUtils.equals(
          leftBBCell.getTagsByteBuffer(), leftBBCell.getTagsPosition(), llength,
          right.getTagsArray(), right.getTagsOffset(), rlength);
    }
    if (right instanceof ByteBufferExtendedCell) {
      ByteBufferExtendedCell rightBBCell = (ByteBufferExtendedCell) right;
      return ByteBufferUtils.equals(
          rightBBCell.getTagsByteBuffer(), rightBBCell.getTagsPosition(), rlength,
          left.getTagsArray(), left.getTagsOffset(), llength);
    }
    return Bytes.equals(left.getTagsArray(), left.getTagsOffset(), llength,
        right.getTagsArray(), right.getTagsOffset(), rlength);
  }

  /**
   * @return True if a delete type, a {@link KeyValue.Type#Delete} or a {KeyValue.Type#DeleteFamily}
   *         or a {@link KeyValue.Type#DeleteColumn} KeyValue type.
   */
  public static boolean isDelete(final byte type) {
    return KeyValue.Type.Delete.getCode() <= type && type <= KeyValue.Type.DeleteFamily.getCode();
  }

  /**
   * @return True if this cell is a {@link KeyValue.Type#Delete} type.
   */
  public static boolean isDeleteType(Cell cell) {
    return cell.getTypeByte() == KeyValue.Type.Delete.getCode();
  }

  public static boolean isDeleteFamily(final Cell cell) {
    return cell.getTypeByte() == KeyValue.Type.DeleteFamily.getCode();
  }

  public static boolean isDeleteFamilyVersion(final Cell cell) {
    return cell.getTypeByte() == KeyValue.Type.DeleteFamilyVersion.getCode();
  }

  public static boolean isDeleteColumns(final Cell cell) {
    return cell.getTypeByte() == KeyValue.Type.DeleteColumn.getCode();
  }

  public static boolean isDeleteColumnVersion(final Cell cell) {
    return cell.getTypeByte() == KeyValue.Type.Delete.getCode();
  }

  /**
   * @return True if this cell is a delete family or column type.
   */
  public static boolean isDeleteColumnOrFamily(Cell cell) {
    int t = cell.getTypeByte();
    return t == KeyValue.Type.DeleteColumn.getCode() || t == KeyValue.Type.DeleteFamily.getCode();
  }

  public static byte[] cloneTags(Cell cell) {
    byte[] output = new byte[cell.getTagsLength()];
    copyTagsTo(cell, output, 0);
    return output;
  }

  /**
   * Copies the tags info into the tag portion of the cell
   * @param cell
   * @param destination
   * @param destinationOffset
   * @return position after tags
   */
  public static int copyTagsTo(Cell cell, byte[] destination, int destinationOffset) {
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
   * @return the position after tags
   */
  public static int copyTagsTo(Cell cell, ByteBuffer destination, int destinationOffset) {
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

  /**
   * @param cell The Cell
   * @return Tags in the given Cell as a List
   */
  public static List<Tag> getTags(Cell cell) {
    List<Tag> tags = new ArrayList<>();
    Iterator<Tag> tagsItr = tagsIterator(cell);
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
   */
  public static Optional<Tag> getTag(Cell cell, byte type) {
    boolean bufferBacked = cell instanceof ByteBufferExtendedCell;
    int length = cell.getTagsLength();
    int offset =
        bufferBacked ? ((ByteBufferExtendedCell) cell).getTagsPosition() : cell.getTagsOffset();
    int pos = offset;
    while (pos < offset + length) {
      int tagLen;
      if (bufferBacked) {
        ByteBuffer tagsBuffer = ((ByteBufferExtendedCell) cell).getTagsByteBuffer();
        tagLen = ByteBufferUtils.readAsInt(tagsBuffer, pos, TAG_LENGTH_SIZE);
        if (ByteBufferUtils.toByte(tagsBuffer, pos + TAG_LENGTH_SIZE) == type) {
          return Optional.of(new ByteBufferTag(tagsBuffer, pos, tagLen + TAG_LENGTH_SIZE));
        }
      } else {
        tagLen = Bytes.readAsInt(cell.getTagsArray(), pos, TAG_LENGTH_SIZE);
        if (cell.getTagsArray()[pos + TAG_LENGTH_SIZE] == type) {
          return Optional
              .of(new ArrayBackedTag(cell.getTagsArray(), pos, tagLen + TAG_LENGTH_SIZE));
        }
      }
      pos += TAG_LENGTH_SIZE + tagLen;
    }
    return Optional.empty();
  }

  /**
   * Util method to iterate through the tags in the given cell.
   * @param cell The Cell over which tags iterator is needed.
   * @return iterator for the tags
   */
  public static Iterator<Tag> tagsIterator(final Cell cell) {
    final int tagsLength = cell.getTagsLength();
    // Save an object allocation where we can
    if (tagsLength == 0) {
      return TagUtil.EMPTY_TAGS_ITR;
    }
    if (cell instanceof ByteBufferExtendedCell) {
      return tagsIterator(((ByteBufferExtendedCell) cell).getTagsByteBuffer(),
          ((ByteBufferExtendedCell) cell).getTagsPosition(), tagsLength);
    }
    return CellUtil.tagsIterator(cell.getTagsArray(), cell.getTagsOffset(), cell.getTagsLength());
  }

  public static Iterator<Tag> tagsIterator(final ByteBuffer tags, final int offset,
                                           final int length) {
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
          int curTagLen = ByteBufferUtils.readAsInt(tags, this.pos, Tag.TAG_LENGTH_SIZE);
          Tag tag = new ByteBufferTag(tags, pos, curTagLen + Tag.TAG_LENGTH_SIZE);
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
   * Returns true if the first range start1...end1 overlaps with the second range start2...end2,
   * assuming the byte arrays represent row keys
   */
  public static boolean overlappingKeys(final byte[] start1, final byte[] end1, final byte[] start2,
                                        final byte[] end2) {
    return (end2.length == 0 || start1.length == 0 || Bytes.compareTo(start1, end2) < 0)
        && (end1.length == 0 || start2.length == 0 || Bytes.compareTo(start2, end1) < 0);
  }

  /**
   * Write rowkey excluding the common part.
   * @param cell
   * @param rLen
   * @param commonPrefix
   * @param out
   * @throws IOException
   */
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
      writeRowSkippingBytes(out, cell, rLen, commonPrefix);
    }
  }

  /**
   * Writes the row from the given cell to the output stream excluding the common prefix
   * @param out The dataoutputstream to which the data has to be written
   * @param cell The cell whose contents has to be written
   * @param rlength the row length
   * @throws IOException
   */
  public static void writeRowSkippingBytes(DataOutputStream out, Cell cell, short rlength,
                                           int commonPrefix) throws IOException {
    if (cell instanceof ByteBufferExtendedCell) {
      ByteBufferUtils
          .copyBufferToStream((DataOutput) out, ((ByteBufferExtendedCell) cell).getRowByteBuffer(),
              ((ByteBufferExtendedCell) cell).getRowPosition() + commonPrefix,
              rlength - commonPrefix);
    } else {
      out.write(cell.getRowArray(), cell.getRowOffset() + commonPrefix, rlength - commonPrefix);
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
   */
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

  /**
   * Used to compare two cells based on the column hint provided. This is specifically used when we
   * need to optimize the seeks based on the next indexed key. This is an advanced usage API
   * specifically needed for some optimizations.
   * @param nextIndexedCell the next indexed cell
   * @param currentCell the cell to be compared
   * @param foff the family offset of the currentCell
   * @param flen the family length of the currentCell
   * @param colHint the column hint provided - could be null
   * @param coff the offset of the column hint if provided, if not offset of the currentCell's
   *          qualifier
   * @param clen the length of the column hint if provided, if not length of the currentCell's
   *          qualifier
   * @param ts the timestamp to be seeked
   * @param type the type to be seeked
   * @return an int based on the given column hint TODO : To be moved out of here because this is a
   *         special API used in scan optimization.
   */
  // compare a key against row/fam/qual/ts/type
  public static final int compareKeyBasedOnColHint(CellComparator comparator, Cell nextIndexedCell,
                                                   Cell currentCell, int foff, int flen, byte[] colHint, int coff, int clen, long ts,
                                                   byte type) {
    int compare = comparator.compareRows(nextIndexedCell, currentCell);
    if (compare != 0) {
      return compare;
    }
    // If the column is not specified, the "minimum" key type appears the
    // latest in the sorted order, regardless of the timestamp. This is used
    // for specifying the last key/value in a given row, because there is no
    // "lexicographically last column" (it would be infinitely long). The
    // "maximum" key type does not need this behavior.
    if (nextIndexedCell.getFamilyLength() + nextIndexedCell.getQualifierLength() == 0
        && nextIndexedCell.getTypeByte() == KeyValue.Type.Minimum.getCode()) {
      // left is "bigger", i.e. it appears later in the sorted order
      return 1;
    }
    if (flen + clen == 0 && type == KeyValue.Type.Minimum.getCode()) {
      return -1;
    }

    compare = comparator.compareFamilies(nextIndexedCell, currentCell);
    if (compare != 0) {
      return compare;
    }
    if (colHint == null) {
      compare = comparator.compareQualifiers(nextIndexedCell, currentCell);
    } else {
      compare = CellUtil.compareQualifiers(nextIndexedCell, colHint, coff, clen);
    }
    if (compare != 0) {
      return compare;
    }
    // Next compare timestamps.
    compare = comparator.compareTimestamps(nextIndexedCell.getTimestamp(), ts);
    if (compare != 0) {
      return compare;
    }

    // Compare types. Let the delete types sort ahead of puts; i.e. types
    // of higher numbers sort before those of lesser numbers. Maximum (255)
    // appears ahead of everything, and minimum (0) appears after
    // everything.
    return (0xff & type) - (0xff & nextIndexedCell.getTypeByte());
  }

  /**
   * Compares only the key portion of a cell. It does not include the sequence id/mvcc of the cell
   * @param left
   * @param right
   * @return an int greater than 0 if left &gt; than right lesser than 0 if left &lt; than right
   *         equal to 0 if left is equal to right
   */
  public static final int compareKeyIgnoresMvcc(CellComparator comparator, Cell left, Cell right) {
    return ((CellComparatorImpl) comparator).compare(left, right, true);
  }

  /**
   * Compare cell's row against given comparator
   * @param cell the cell to use for comparison
   * @param comparator the {@link CellComparator} to use for comparison
   * @return result comparing cell's row
   */
  public static int compareRow(Cell cell, ByteArrayComparable comparator) {
    if (cell instanceof ByteBufferExtendedCell) {
      return comparator.compareTo(((ByteBufferExtendedCell) cell).getRowByteBuffer(),
          ((ByteBufferExtendedCell) cell).getRowPosition(), cell.getRowLength());
    }
    return comparator.compareTo(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
  }

  /**
   * Compare cell's column family against given comparator
   * @param cell the cell to use for comparison
   * @param comparator the {@link CellComparator} to use for comparison
   * @return result comparing cell's column family
   */
  public static int compareFamily(Cell cell, ByteArrayComparable comparator) {
    if (cell instanceof ByteBufferExtendedCell) {
      return comparator.compareTo(((ByteBufferExtendedCell) cell).getFamilyByteBuffer(),
          ((ByteBufferExtendedCell) cell).getFamilyPosition(), cell.getFamilyLength());
    }
    return comparator.compareTo(cell.getFamilyArray(), cell.getFamilyOffset(),
        cell.getFamilyLength());
  }

  /**
   * Compare cell's qualifier against given comparator
   * @param cell the cell to use for comparison
   * @param comparator the {@link CellComparator} to use for comparison
   * @return result comparing cell's qualifier
   */
  public static int compareQualifier(Cell cell, ByteArrayComparable comparator) {
    if (cell instanceof ByteBufferExtendedCell) {
      return comparator.compareTo(((ByteBufferExtendedCell) cell).getQualifierByteBuffer(),
          ((ByteBufferExtendedCell) cell).getQualifierPosition(), cell.getQualifierLength());
    }
    return comparator.compareTo(cell.getQualifierArray(), cell.getQualifierOffset(),
        cell.getQualifierLength());
  }

  public static Cell.Type toType(byte type) {
    KeyValue.Type codeToType = KeyValue.Type.codeToType(type);
    switch (codeToType) {
      case Put: return Cell.Type.Put;
      case Delete: return Cell.Type.Delete;
      case DeleteColumn: return Cell.Type.DeleteColumn;
      case DeleteFamily: return Cell.Type.DeleteFamily;
      case DeleteFamilyVersion: return Cell.Type.DeleteFamilyVersion;
      default: throw new UnsupportedOperationException("Invalid type of cell "+type);
    }
  }

  public static KeyValue.Type toTypeByte(Cell.Type type) {
    switch (type) {
      case Put: return KeyValue.Type.Put;
      case Delete: return KeyValue.Type.Delete;
      case DeleteColumn: return KeyValue.Type.DeleteColumn;
      case DeleteFamilyVersion: return KeyValue.Type.DeleteFamilyVersion;
      case DeleteFamily: return KeyValue.Type.DeleteFamily;
      default: throw new UnsupportedOperationException("Unsupported data type:" + type);
    }
  }

  /**
   * Compare cell's value against given comparator
   * @param cell the cell to use for comparison
   * @param comparator the {@link CellComparator} to use for comparison
   * @return result comparing cell's value
   */
  public static int compareValue(Cell cell, ByteArrayComparable comparator) {
    if (cell instanceof ByteBufferExtendedCell) {
      return comparator.compareTo(((ByteBufferExtendedCell) cell).getValueByteBuffer(),
          ((ByteBufferExtendedCell) cell).getValuePosition(), cell.getValueLength());
    }
    return comparator.compareTo(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
  }

  /**
   * These cells are used in reseeks/seeks to improve the read performance. They are not real cells
   * that are returned back to the clients
   */
  private static abstract class EmptyCell implements ExtendedCell {

    @Override
    public void setSequenceId(long seqId) {
      // Fake cells don't need seqId, so leaving it as a noop.
    }

    @Override
    public void setTimestamp(long ts) {
      // Fake cells can't be changed timestamp, so leaving it as a noop.
    }

    @Override
    public void setTimestamp(byte[] ts) {
      // Fake cells can't be changed timestamp, so leaving it as a noop.
    }

    @Override
    public byte[] getRowArray() {
      return EMPTY_BYTE_ARRAY;
    }

    @Override
    public int getRowOffset() {
      return 0;
    }

    @Override
    public short getRowLength() {
      return 0;
    }

    @Override
    public byte[] getFamilyArray() {
      return EMPTY_BYTE_ARRAY;
    }

    @Override
    public int getFamilyOffset() {
      return 0;
    }

    @Override
    public byte getFamilyLength() {
      return 0;
    }

    @Override
    public byte[] getQualifierArray() {
      return EMPTY_BYTE_ARRAY;
    }

    @Override
    public int getQualifierOffset() {
      return 0;
    }

    @Override
    public int getQualifierLength() {
      return 0;
    }

    @Override
    public long getSequenceId() {
      return 0;
    }

    @Override
    public byte[] getValueArray() {
      return EMPTY_BYTE_ARRAY;
    }

    @Override
    public int getValueOffset() {
      return 0;
    }

    @Override
    public int getValueLength() {
      return 0;
    }

    @Override
    public byte[] getTagsArray() {
      return EMPTY_BYTE_ARRAY;
    }

    @Override
    public int getTagsOffset() {
      return 0;
    }

    @Override
    public int getTagsLength() {
      return 0;
    }
  }

  /**
   * These cells are used in reseeks/seeks to improve the read performance. They are not real cells
   * that are returned back to the clients
   */
  private static abstract class EmptyByteBufferExtendedCell extends ByteBufferExtendedCell {

    @Override
    public void setSequenceId(long seqId) {
      // Fake cells don't need seqId, so leaving it as a noop.
    }

    @Override
    public void setTimestamp(long ts) {
      // Fake cells can't be changed timestamp, so leaving it as a noop.
    }

    @Override
    public void setTimestamp(byte[] ts) {
      // Fake cells can't be changed timestamp, so leaving it as a noop.
    }

    @Override
    public byte[] getRowArray() {
      return CellUtil.cloneRow(this);
    }

    @Override
    public int getRowOffset() {
      return 0;
    }

    @Override
    public short getRowLength() {
      return 0;
    }

    @Override
    public byte[] getFamilyArray() {
      return CellUtil.cloneFamily(this);
    }

    @Override
    public int getFamilyOffset() {
      return 0;
    }

    @Override
    public byte getFamilyLength() {
      return 0;
    }

    @Override
    public byte[] getQualifierArray() {
      return CellUtil.cloneQualifier(this);
    }

    @Override
    public int getQualifierOffset() {
      return 0;
    }

    @Override
    public int getQualifierLength() {
      return 0;
    }

    @Override
    public long getSequenceId() {
      return 0;
    }

    @Override
    public byte[] getValueArray() {
      return CellUtil.cloneValue(this);
    }

    @Override
    public int getValueOffset() {
      return 0;
    }

    @Override
    public int getValueLength() {
      return 0;
    }

    @Override
    public byte[] getTagsArray() {
      return CellUtil.cloneTags(this);
    }

    @Override
    public int getTagsOffset() {
      return 0;
    }

    @Override
    public int getTagsLength() {
      return 0;
    }

    @Override
    public ByteBuffer getRowByteBuffer() {
      return HConstants.EMPTY_BYTE_BUFFER;
    }

    @Override
    public int getRowPosition() {
      return 0;
    }

    @Override
    public ByteBuffer getFamilyByteBuffer() {
      return HConstants.EMPTY_BYTE_BUFFER;
    }

    @Override
    public int getFamilyPosition() {
      return 0;
    }

    @Override
    public ByteBuffer getQualifierByteBuffer() {
      return HConstants.EMPTY_BYTE_BUFFER;
    }

    @Override
    public int getQualifierPosition() {
      return 0;
    }

    @Override
    public ByteBuffer getTagsByteBuffer() {
      return HConstants.EMPTY_BYTE_BUFFER;
    }

    @Override
    public int getTagsPosition() {
      return 0;
    }

    @Override
    public ByteBuffer getValueByteBuffer() {
      return HConstants.EMPTY_BYTE_BUFFER;
    }

    @Override
    public int getValuePosition() {
      return 0;
    }
  }

  private static class FirstOnRowCell extends EmptyCell {
    private static final int FIXED_HEAPSIZE =
        ClassSize.OBJECT // object
            + ClassSize.REFERENCE // row array
            + Bytes.SIZEOF_INT // row offset
            + Bytes.SIZEOF_SHORT;  // row length
    private final byte[] rowArray;
    private final int roffset;
    private final short rlength;

    public FirstOnRowCell(final byte[] row, int roffset, short rlength) {
      this.rowArray = row;
      this.roffset = roffset;
      this.rlength = rlength;
    }

    @Override
    public long heapSize() {
      return ClassSize.align(FIXED_HEAPSIZE)
          // array overhead
          + (rlength == 0 ? ClassSize.sizeOfByteArray(rlength) : rlength);
    }

    @Override
    public byte[] getRowArray() {
      return this.rowArray;
    }

    @Override
    public int getRowOffset() {
      return this.roffset;
    }

    @Override
    public short getRowLength() {
      return this.rlength;
    }

    @Override
    public long getTimestamp() {
      return HConstants.LATEST_TIMESTAMP;
    }

    @Override
    public byte getTypeByte() {
      return KeyValue.Type.Maximum.getCode();
    }

    @Override
    public Type getType() {
      throw new UnsupportedOperationException();
    }
  }

  private static class FirstOnRowByteBufferExtendedCell extends EmptyByteBufferExtendedCell {
    private static final int FIXED_OVERHEAD =
        ClassSize.OBJECT // object
            + ClassSize.REFERENCE // row buffer
            + Bytes.SIZEOF_INT // row offset
            + Bytes.SIZEOF_SHORT; // row length
    private final ByteBuffer rowBuff;
    private final int roffset;
    private final short rlength;

    public FirstOnRowByteBufferExtendedCell(final ByteBuffer row, int roffset, short rlength) {
      this.rowBuff = row;
      this.roffset = roffset;
      this.rlength = rlength;
    }

    @Override
    public long heapSize() {
      if (this.rowBuff.hasArray()) {
        return ClassSize.align(FIXED_OVERHEAD + rlength);
      }
      return ClassSize.align(FIXED_OVERHEAD);
    }

    @Override
    public ByteBuffer getRowByteBuffer() {
      return this.rowBuff;
    }

    @Override
    public int getRowPosition() {
      return this.roffset;
    }

    @Override
    public short getRowLength() {
      return this.rlength;
    }

    @Override
    public long getTimestamp() {
      return HConstants.LATEST_TIMESTAMP;
    }

    @Override
    public byte getTypeByte() {
      return KeyValue.Type.Maximum.getCode();
    }

    @Override
    public Type getType() {
      throw new UnsupportedOperationException();
    }
  }

  private static class LastOnRowByteBufferExtendedCell extends EmptyByteBufferExtendedCell {
    private static final int FIXED_OVERHEAD =
        ClassSize.OBJECT // object
            + ClassSize.REFERENCE // rowBuff
            + Bytes.SIZEOF_INT // roffset
            + Bytes.SIZEOF_SHORT; // rlength
    private final ByteBuffer rowBuff;
    private final int roffset;
    private final short rlength;

    public LastOnRowByteBufferExtendedCell(final ByteBuffer row, int roffset, short rlength) {
      this.rowBuff = row;
      this.roffset = roffset;
      this.rlength = rlength;
    }

    @Override
    public long heapSize() {
      if (this.rowBuff.hasArray()) {
        return ClassSize.align(FIXED_OVERHEAD + rlength);
      }
      return ClassSize.align(FIXED_OVERHEAD);
    }

    @Override
    public ByteBuffer getRowByteBuffer() {
      return this.rowBuff;
    }

    @Override
    public int getRowPosition() {
      return this.roffset;
    }

    @Override
    public short getRowLength() {
      return this.rlength;
    }

    @Override
    public long getTimestamp() {
      return HConstants.OLDEST_TIMESTAMP;
    }

    @Override
    public byte getTypeByte() {
      return KeyValue.Type.Minimum.getCode();
    }

    @Override
    public Type getType() {
      throw new UnsupportedOperationException();
    }
  }

  private static class FirstOnRowColByteBufferExtendedCell
      extends FirstOnRowByteBufferExtendedCell {
    private static final int FIXED_OVERHEAD =
        FirstOnRowByteBufferExtendedCell.FIXED_OVERHEAD
            + ClassSize.REFERENCE * 2 // family buffer and column buffer
            + Bytes.SIZEOF_INT * 3 // famOffset, colOffset, colLength
            + Bytes.SIZEOF_BYTE; // famLength
    private final ByteBuffer famBuff;
    private final int famOffset;
    private final byte famLength;
    private final ByteBuffer colBuff;
    private final int colOffset;
    private final int colLength;

    public FirstOnRowColByteBufferExtendedCell(final ByteBuffer row, int roffset, short rlength,
                                               final ByteBuffer famBuff, final int famOffset, final byte famLength, final ByteBuffer col,
                                               final int colOffset, final int colLength) {
      super(row, roffset, rlength);
      this.famBuff = famBuff;
      this.famOffset = famOffset;
      this.famLength = famLength;
      this.colBuff = col;
      this.colOffset = colOffset;
      this.colLength = colLength;
    }

    @Override
    public long heapSize() {
      if (famBuff.hasArray() && colBuff.hasArray()) {
        return ClassSize.align(FIXED_OVERHEAD + famLength + colLength);
      } else if (famBuff.hasArray()) {
        return ClassSize.align(FIXED_OVERHEAD + famLength);
      } else if (colBuff.hasArray()) {
        return ClassSize.align(FIXED_OVERHEAD + colLength);
      } else {
        return ClassSize.align(FIXED_OVERHEAD);
      }
    }

    @Override
    public ByteBuffer getFamilyByteBuffer() {
      return this.famBuff;
    }

    @Override
    public int getFamilyPosition() {
      return this.famOffset;
    }

    @Override
    public byte getFamilyLength() {
      return famLength;
    }

    @Override
    public ByteBuffer getQualifierByteBuffer() {
      return this.colBuff;
    }

    @Override
    public int getQualifierPosition() {
      return this.colOffset;
    }

    @Override
    public int getQualifierLength() {
      return this.colLength;
    }
  }

  private static class FirstOnRowColCell extends FirstOnRowCell {
    private static final long FIXED_HEAPSIZE =
        FirstOnRowCell.FIXED_HEAPSIZE
            + Bytes.SIZEOF_BYTE // flength
            + Bytes.SIZEOF_INT * 3 // foffset, qoffset, qlength
            + ClassSize.REFERENCE * 2; // fArray, qArray
    private final byte[] fArray;
    private final int foffset;
    private final byte flength;
    private final byte[] qArray;
    private final int qoffset;
    private final int qlength;

    public FirstOnRowColCell(byte[] rArray, int roffset, short rlength, byte[] fArray, int foffset,
                             byte flength, byte[] qArray, int qoffset, int qlength) {
      super(rArray, roffset, rlength);
      this.fArray = fArray;
      this.foffset = foffset;
      this.flength = flength;
      this.qArray = qArray;
      this.qoffset = qoffset;
      this.qlength = qlength;
    }

    @Override
    public long heapSize() {
      return ClassSize.align(FIXED_HEAPSIZE)
          // array overhead
          + (flength == 0 ? ClassSize.sizeOfByteArray(flength) : flength)
          + (qlength == 0 ? ClassSize.sizeOfByteArray(qlength) : qlength);
    }

    @Override
    public byte[] getFamilyArray() {
      return this.fArray;
    }

    @Override
    public int getFamilyOffset() {
      return this.foffset;
    }

    @Override
    public byte getFamilyLength() {
      return this.flength;
    }

    @Override
    public byte[] getQualifierArray() {
      return this.qArray;
    }

    @Override
    public int getQualifierOffset() {
      return this.qoffset;
    }

    @Override
    public int getQualifierLength() {
      return this.qlength;
    }
  }

  private static class FirstOnRowColTSCell extends FirstOnRowColCell {
    private static final long FIXED_HEAPSIZE =
        FirstOnRowColCell.FIXED_HEAPSIZE
            + Bytes.SIZEOF_LONG; // ts
    private long ts;

    public FirstOnRowColTSCell(byte[] rArray, int roffset, short rlength, byte[] fArray,
                               int foffset, byte flength, byte[] qArray, int qoffset, int qlength, long ts) {
      super(rArray, roffset, rlength, fArray, foffset, flength, qArray, qoffset, qlength);
      this.ts = ts;
    }

    @Override
    public long getTimestamp() {
      return this.ts;
    }

    @Override
    public long heapSize() {
      return ClassSize.align(FIXED_HEAPSIZE);
    }
  }

  private static class FirstOnRowColTSByteBufferExtendedCell
      extends FirstOnRowColByteBufferExtendedCell {
    private static final int FIXED_OVERHEAD =
        FirstOnRowColByteBufferExtendedCell.FIXED_OVERHEAD
            + Bytes.SIZEOF_LONG; // ts
    private long ts;

    public FirstOnRowColTSByteBufferExtendedCell(ByteBuffer rBuffer, int roffset, short rlength,
                                                 ByteBuffer fBuffer, int foffset, byte flength, ByteBuffer qBuffer, int qoffset, int qlength,
                                                 long ts) {
      super(rBuffer, roffset, rlength, fBuffer, foffset, flength, qBuffer, qoffset, qlength);
      this.ts = ts;
    }

    @Override
    public long getTimestamp() {
      return this.ts;
    }

    @Override
    public long heapSize() {
      return ClassSize.align(FIXED_OVERHEAD + super.heapSize());
    }
  }

  private static class LastOnRowCell extends EmptyCell {
    private static final int FIXED_OVERHEAD =
        ClassSize.OBJECT // object
            + ClassSize.REFERENCE // row array
            + Bytes.SIZEOF_INT // row offset
            + Bytes.SIZEOF_SHORT; // row length
    private final byte[] rowArray;
    private final int roffset;
    private final short rlength;

    public LastOnRowCell(byte[] row, int roffset, short rlength) {
      this.rowArray = row;
      this.roffset = roffset;
      this.rlength = rlength;
    }

    @Override
    public long heapSize() {
      return ClassSize.align(FIXED_OVERHEAD)
          // array overhead
          + (rlength == 0 ? ClassSize.sizeOfByteArray(rlength) : rlength);
    }

    @Override
    public byte[] getRowArray() {
      return this.rowArray;
    }

    @Override
    public int getRowOffset() {
      return this.roffset;
    }

    @Override
    public short getRowLength() {
      return this.rlength;
    }

    @Override
    public long getTimestamp() {
      return HConstants.OLDEST_TIMESTAMP;
    }

    @Override
    public byte getTypeByte() {
      return KeyValue.Type.Minimum.getCode();
    }

    @Override
    public Type getType() {
      throw new UnsupportedOperationException();
    }
  }

  private static class LastOnRowColCell extends LastOnRowCell {
    private static final long FIXED_OVERHEAD = LastOnRowCell.FIXED_OVERHEAD
        + ClassSize.REFERENCE * 2 // fArray and qArray
        + Bytes.SIZEOF_INT * 3 // foffset, qoffset, qlength
        + Bytes.SIZEOF_BYTE; // flength
    private final byte[] fArray;
    private final int foffset;
    private final byte flength;
    private final byte[] qArray;
    private final int qoffset;
    private final int qlength;

    public LastOnRowColCell(byte[] rArray, int roffset, short rlength, byte[] fArray, int foffset,
                            byte flength, byte[] qArray, int qoffset, int qlength) {
      super(rArray, roffset, rlength);
      this.fArray = fArray;
      this.foffset = foffset;
      this.flength = flength;
      this.qArray = qArray;
      this.qoffset = qoffset;
      this.qlength = qlength;
    }

    @Override
    public long heapSize() {
      return ClassSize.align(FIXED_OVERHEAD)
          // array overhead
          + (flength == 0 ? ClassSize.sizeOfByteArray(flength) : flength)
          + (qlength == 0 ? ClassSize.sizeOfByteArray(qlength) : qlength);
    }

    @Override
    public byte[] getFamilyArray() {
      return this.fArray;
    }

    @Override
    public int getFamilyOffset() {
      return this.foffset;
    }

    @Override
    public byte getFamilyLength() {
      return this.flength;
    }

    @Override
    public byte[] getQualifierArray() {
      return this.qArray;
    }

    @Override
    public int getQualifierOffset() {
      return this.qoffset;
    }

    @Override
    public int getQualifierLength() {
      return this.qlength;
    }
  }

  private static class LastOnRowColByteBufferExtendedCell extends LastOnRowByteBufferExtendedCell {
    private static final int FIXED_OVERHEAD =
        LastOnRowByteBufferExtendedCell.FIXED_OVERHEAD
            + ClassSize.REFERENCE * 2 // fBuffer and qBuffer
            + Bytes.SIZEOF_INT * 3 // foffset, qoffset, qlength
            + Bytes.SIZEOF_BYTE; // flength
    private final ByteBuffer fBuffer;
    private final int foffset;
    private final byte flength;
    private final ByteBuffer qBuffer;
    private final int qoffset;
    private final int qlength;

    public LastOnRowColByteBufferExtendedCell(ByteBuffer rBuffer, int roffset, short rlength,
                                              ByteBuffer fBuffer, int foffset, byte flength, ByteBuffer qBuffer, int qoffset,
                                              int qlength) {
      super(rBuffer, roffset, rlength);
      this.fBuffer = fBuffer;
      this.foffset = foffset;
      this.flength = flength;
      this.qBuffer = qBuffer;
      this.qoffset = qoffset;
      this.qlength = qlength;
    }

    @Override
    public long heapSize() {
      if (fBuffer.hasArray() && qBuffer.hasArray()) {
        return ClassSize.align(FIXED_OVERHEAD + flength + qlength);
      } else if (fBuffer.hasArray()) {
        return ClassSize.align(FIXED_OVERHEAD + flength);
      } else if (qBuffer.hasArray()) {
        return ClassSize.align(FIXED_OVERHEAD + qlength);
      } else {
        return ClassSize.align(FIXED_OVERHEAD);
      }
    }

    @Override
    public ByteBuffer getFamilyByteBuffer() {
      return this.fBuffer;
    }

    @Override
    public int getFamilyPosition() {
      return this.foffset;
    }

    @Override
    public byte getFamilyLength() {
      return this.flength;
    }

    @Override
    public ByteBuffer getQualifierByteBuffer() {
      return this.qBuffer;
    }

    @Override
    public int getQualifierPosition() {
      return this.qoffset;
    }

    @Override
    public int getQualifierLength() {
      return this.qlength;
    }
  }

  private static class FirstOnRowDeleteFamilyCell extends EmptyCell {
    private static final int FIXED_OVERHEAD =
        ClassSize.OBJECT // object
            + ClassSize.REFERENCE * 2 // fBuffer and qBuffer
            + Bytes.SIZEOF_INT * 3 // foffset, qoffset, qlength
            + Bytes.SIZEOF_BYTE; // flength
    private final byte[] row;
    private final byte[] fam;

    public FirstOnRowDeleteFamilyCell(byte[] row, byte[] fam) {
      this.row = row;
      this.fam = fam;
    }

    @Override
    public long heapSize() {
      return ClassSize.align(FIXED_OVERHEAD)
          // array overhead
          + (getRowLength() == 0 ? ClassSize.sizeOfByteArray(getRowLength()) : getRowLength())
          + (getFamilyLength() == 0 ?
          ClassSize.sizeOfByteArray(getFamilyLength()) : getFamilyLength());
    }

    @Override
    public byte[] getRowArray() {
      return this.row;
    }

    @Override
    public short getRowLength() {
      return (short) this.row.length;
    }

    @Override
    public byte[] getFamilyArray() {
      return this.fam;
    }

    @Override
    public byte getFamilyLength() {
      return (byte) this.fam.length;
    }

    @Override
    public long getTimestamp() {
      return HConstants.LATEST_TIMESTAMP;
    }

    @Override
    public byte getTypeByte() {
      return KeyValue.Type.DeleteFamily.getCode();
    }

    @Override
    public Type getType() {
      return Type.DeleteFamily;
    }
  }

  /**
   * Writes the Cell's key part as it would have serialized in a KeyValue. The format is &lt;2 bytes
   * rk len&gt;&lt;rk&gt;&lt;1 byte cf len&gt;&lt;cf&gt;&lt;qualifier&gt;&lt;8 bytes
   * timestamp&gt;&lt;1 byte type&gt;
   * @param cell
   * @param out
   * @throws IOException
   */
  public static void writeFlatKey(Cell cell, DataOutput out) throws IOException {
    short rowLen = cell.getRowLength();
    byte fLen = cell.getFamilyLength();
    int qLen = cell.getQualifierLength();
    // Using just one if/else loop instead of every time checking before writing every
    // component of cell
    if (cell instanceof ByteBufferExtendedCell) {
      out.writeShort(rowLen);
      ByteBufferUtils.copyBufferToStream(out, ((ByteBufferExtendedCell) cell).getRowByteBuffer(),
          ((ByteBufferExtendedCell) cell).getRowPosition(), rowLen);
      out.writeByte(fLen);
      ByteBufferUtils.copyBufferToStream(out, ((ByteBufferExtendedCell) cell).getFamilyByteBuffer(),
          ((ByteBufferExtendedCell) cell).getFamilyPosition(), fLen);
      ByteBufferUtils
          .copyBufferToStream(out, ((ByteBufferExtendedCell) cell).getQualifierByteBuffer(),
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
   * Deep clones the given cell if the cell supports deep cloning
   * @param cell the cell to be cloned
   * @return the cloned cell
   * @throws CloneNotSupportedException
   */
  public static Cell deepClone(Cell cell) throws CloneNotSupportedException {
    if (cell instanceof ExtendedCell) {
      return ((ExtendedCell) cell).deepClone();
    }
    throw new CloneNotSupportedException();
  }

  /**
   * Writes the cell to the given OutputStream
   * @param cell the cell to be written
   * @param out the outputstream
   * @param withTags if tags are to be written or not
   * @return the total bytes written
   * @throws IOException
   */
  public static int writeCell(Cell cell, OutputStream out, boolean withTags) throws IOException {
    if (cell instanceof ExtendedCell) {
      return ((ExtendedCell) cell).write(out, withTags);
    } else {
      ByteBufferUtils.putInt(out, estimatedSerializedSizeOfKey(cell));
      ByteBufferUtils.putInt(out, cell.getValueLength());
      writeFlatKey(cell, out);
      writeValue(out, cell, cell.getValueLength());
      int tagsLength = cell.getTagsLength();
      if (withTags) {
        byte[] len = new byte[Bytes.SIZEOF_SHORT];
        Bytes.putAsShort(len, 0, tagsLength);
        out.write(len);
        if (tagsLength > 0) {
          writeTags(out, cell, tagsLength);
        }
      }
      int lenWritten = (2 * Bytes.SIZEOF_INT) + estimatedSerializedSizeOfKey(cell)
          + cell.getValueLength();
      if (withTags) {
        lenWritten += Bytes.SIZEOF_SHORT + tagsLength;
      }
      return lenWritten;
    }
  }

  /**
   * Writes a cell to the buffer at the given offset
   * @param cell the cell to be written
   * @param buf the buffer to which the cell has to be wrriten
   * @param offset the offset at which the cell should be written
   */
  public static void writeCellToBuffer(Cell cell, ByteBuffer buf, int offset) {
    if (cell instanceof ExtendedCell) {
      ((ExtendedCell) cell).write(buf, offset);
    } else {
      // Using the KVUtil
      byte[] bytes = KeyValueUtil.copyToNewByteArray(cell);
      ByteBufferUtils.copyFromArrayToBuffer(buf, offset, bytes, 0, bytes.length);
    }
  }

  public static int writeFlatKey(Cell cell, OutputStream out) throws IOException {
    short rowLen = cell.getRowLength();
    byte fLen = cell.getFamilyLength();
    int qLen = cell.getQualifierLength();
    // Using just one if/else loop instead of every time checking before writing every
    // component of cell
    if (cell instanceof ByteBufferExtendedCell) {
      StreamUtils.writeShort(out, rowLen);
      ByteBufferUtils.copyBufferToStream(out, ((ByteBufferExtendedCell) cell).getRowByteBuffer(),
          ((ByteBufferExtendedCell) cell).getRowPosition(), rowLen);
      out.write(fLen);
      ByteBufferUtils.copyBufferToStream(out, ((ByteBufferExtendedCell) cell).getFamilyByteBuffer(),
          ((ByteBufferExtendedCell) cell).getFamilyPosition(), fLen);
      ByteBufferUtils
          .copyBufferToStream(out, ((ByteBufferExtendedCell) cell).getQualifierByteBuffer(),
              ((ByteBufferExtendedCell) cell).getQualifierPosition(), qLen);
    } else {
      StreamUtils.writeShort(out, rowLen);
      out.write(cell.getRowArray(), cell.getRowOffset(), rowLen);
      out.write(fLen);
      out.write(cell.getFamilyArray(), cell.getFamilyOffset(), fLen);
      out.write(cell.getQualifierArray(), cell.getQualifierOffset(), qLen);
    }
    StreamUtils.writeLong(out, cell.getTimestamp());
    out.write(cell.getTypeByte());
    return Bytes.SIZEOF_SHORT + rowLen + Bytes.SIZEOF_BYTE + fLen + qLen + Bytes.SIZEOF_LONG
        + Bytes.SIZEOF_BYTE;
  }

  /**
   * Sets the given seqId to the cell. Marked as audience Private as of 1.2.0. Setting a Cell
   * sequenceid is an internal implementation detail not for general public use.
   * @param cell
   * @param seqId
   * @throws IOException when the passed cell is not of type {@link ExtendedCell}
   */
  public static void setSequenceId(Cell cell, long seqId) throws IOException {
    if (cell instanceof ExtendedCell) {
      ((ExtendedCell) cell).setSequenceId(seqId);
    } else {
      throw new IOException(new UnsupportedOperationException(
          "Cell is not of type " + ExtendedCell.class.getName()));
    }
  }

  /**
   * Sets the given timestamp to the cell.
   * @param cell
   * @param ts
   * @throws IOException when the passed cell is not of type {@link ExtendedCell}
   */
  public static void setTimestamp(Cell cell, long ts) throws IOException {
    if (cell instanceof ExtendedCell) {
      ((ExtendedCell) cell).setTimestamp(ts);
    } else {
      throw new IOException(new UnsupportedOperationException(
          "Cell is not of type " + ExtendedCell.class.getName()));
    }
  }

  /**
   * Sets the given timestamp to the cell.
   * @param cell
   * @param ts buffer containing the timestamp value
   * @throws IOException when the passed cell is not of type {@link ExtendedCell}
   */
  public static void setTimestamp(Cell cell, byte[] ts) throws IOException {
    if (cell instanceof ExtendedCell) {
      ((ExtendedCell) cell).setTimestamp(ts);
    } else {
      throw new IOException(new UnsupportedOperationException(
          "Cell is not of type " + ExtendedCell.class.getName()));
    }
  }

  /**
   * Sets the given timestamp to the cell iff current timestamp is
   * {@link HConstants#LATEST_TIMESTAMP}.
   * @param cell
   * @param ts
   * @return True if cell timestamp is modified.
   * @throws IOException when the passed cell is not of type {@link ExtendedCell}
   */
  public static boolean updateLatestStamp(Cell cell, long ts) throws IOException {
    if (cell.getTimestamp() == HConstants.LATEST_TIMESTAMP) {
      setTimestamp(cell, ts);
      return true;
    }
    return false;
  }

  /**
   * Sets the given timestamp to the cell iff current timestamp is
   * {@link HConstants#LATEST_TIMESTAMP}.
   * @param cell
   * @param ts buffer containing the timestamp value
   * @return True if cell timestamp is modified.
   * @throws IOException when the passed cell is not of type {@link ExtendedCell}
   */
  public static boolean updateLatestStamp(Cell cell, byte[] ts) throws IOException {
    if (cell.getTimestamp() == HConstants.LATEST_TIMESTAMP) {
      setTimestamp(cell, ts);
      return true;
    }
    return false;
  }

  /**
   * Writes the row from the given cell to the output stream
   * @param out The outputstream to which the data has to be written
   * @param cell The cell whose contents has to be written
   * @param rlength the row length
   * @throws IOException
   */
  public static void writeRow(OutputStream out, Cell cell, short rlength) throws IOException {
    if (cell instanceof ByteBufferExtendedCell) {
      ByteBufferUtils.copyBufferToStream(out, ((ByteBufferExtendedCell) cell).getRowByteBuffer(),
          ((ByteBufferExtendedCell) cell).getRowPosition(), rlength);
    } else {
      out.write(cell.getRowArray(), cell.getRowOffset(), rlength);
    }
  }

  /**
   * Writes the family from the given cell to the output stream
   * @param out The outputstream to which the data has to be written
   * @param cell The cell whose contents has to be written
   * @param flength the family length
   * @throws IOException
   */
  public static void writeFamily(OutputStream out, Cell cell, byte flength) throws IOException {
    if (cell instanceof ByteBufferExtendedCell) {
      ByteBufferUtils.copyBufferToStream(out, ((ByteBufferExtendedCell) cell).getFamilyByteBuffer(),
          ((ByteBufferExtendedCell) cell).getFamilyPosition(), flength);
    } else {
      out.write(cell.getFamilyArray(), cell.getFamilyOffset(), flength);
    }
  }

  /**
   * Writes the qualifier from the given cell to the output stream
   * @param out The outputstream to which the data has to be written
   * @param cell The cell whose contents has to be written
   * @param qlength the qualifier length
   * @throws IOException
   */
  public static void writeQualifier(OutputStream out, Cell cell, int qlength) throws IOException {
    if (cell instanceof ByteBufferExtendedCell) {
      ByteBufferUtils
          .copyBufferToStream(out, ((ByteBufferExtendedCell) cell).getQualifierByteBuffer(),
              ((ByteBufferExtendedCell) cell).getQualifierPosition(), qlength);
    } else {
      out.write(cell.getQualifierArray(), cell.getQualifierOffset(), qlength);
    }
  }

  /**
   * Writes the qualifier from the given cell to the output stream excluding the common prefix
   * @param out The dataoutputstream to which the data has to be written
   * @param cell The cell whose contents has to be written
   * @param qlength the qualifier length
   * @throws IOException
   */
  public static void writeQualifierSkippingBytes(DataOutputStream out, Cell cell, int qlength,
                                                 int commonPrefix) throws IOException {
    if (cell instanceof ByteBufferExtendedCell) {
      ByteBufferUtils.copyBufferToStream((DataOutput) out,
          ((ByteBufferExtendedCell) cell).getQualifierByteBuffer(),
          ((ByteBufferExtendedCell) cell).getQualifierPosition() + commonPrefix,
          qlength - commonPrefix);
    } else {
      out.write(cell.getQualifierArray(), cell.getQualifierOffset() + commonPrefix,
          qlength - commonPrefix);
    }
  }

  /**
   * Writes the value from the given cell to the output stream
   * @param out The outputstream to which the data has to be written
   * @param cell The cell whose contents has to be written
   * @param vlength the value length
   * @throws IOException
   */
  public static void writeValue(OutputStream out, Cell cell, int vlength) throws IOException {
    if (cell instanceof ByteBufferExtendedCell) {
      ByteBufferUtils.copyBufferToStream(out, ((ByteBufferExtendedCell) cell).getValueByteBuffer(),
          ((ByteBufferExtendedCell) cell).getValuePosition(), vlength);
    } else {
      out.write(cell.getValueArray(), cell.getValueOffset(), vlength);
    }
  }

  /**
   * Writes the tag from the given cell to the output stream
   * @param out The outputstream to which the data has to be written
   * @param cell The cell whose contents has to be written
   * @param tagsLength the tag length
   * @throws IOException
   */
  public static void writeTags(OutputStream out, Cell cell, int tagsLength) throws IOException {
    if (cell instanceof ByteBufferExtendedCell) {
      ByteBufferUtils.copyBufferToStream(out, ((ByteBufferExtendedCell) cell).getTagsByteBuffer(),
          ((ByteBufferExtendedCell) cell).getTagsPosition(), tagsLength);
    } else {
      out.write(cell.getTagsArray(), cell.getTagsOffset(), tagsLength);
    }
  }

  /**
   * special case for Cell.equals
   */
  public static boolean equalsIgnoreMvccVersion(Cell a, Cell b) {
    // row
    boolean res = CellUtil.matchingRows(a, b);
    if (!res) return res;

    // family
    res = CellUtil.matchingColumn(a, b);
    if (!res) return res;

    // timestamp: later sorts first
    if (!CellUtil.matchingTimestamp(a, b)) return false;

    // type
    int c = (0xff & b.getTypeByte()) - (0xff & a.getTypeByte());
    if (c != 0) return false;
    else return true;
  }

  /**
   * Converts the rowkey bytes of the given cell into an int value
   * @param cell
   * @return rowkey as int
   */
  public static int getRowAsInt(Cell cell) {
    if (cell instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.toInt(((ByteBufferExtendedCell) cell).getRowByteBuffer(),
          ((ByteBufferExtendedCell) cell).getRowPosition());
    }
    return Bytes.toInt(cell.getRowArray(), cell.getRowOffset());
  }

  /**
   * Converts the value bytes of the given cell into a long value
   * @param cell
   * @return value as long
   */
  public static long getValueAsLong(Cell cell) {
    if (cell instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.toLong(((ByteBufferExtendedCell) cell).getValueByteBuffer(),
          ((ByteBufferExtendedCell) cell).getValuePosition());
    }
    return Bytes.toLong(cell.getValueArray(), cell.getValueOffset());
  }

  /**
   * Converts the value bytes of the given cell into a int value
   * @param cell
   * @return value as int
   */
  public static int getValueAsInt(Cell cell) {
    if (cell instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.toInt(((ByteBufferExtendedCell) cell).getValueByteBuffer(),
          ((ByteBufferExtendedCell) cell).getValuePosition());
    }
    return Bytes.toInt(cell.getValueArray(), cell.getValueOffset());
  }

  /**
   * Converts the value bytes of the given cell into a double value
   * @param cell
   * @return value as double
   */
  public static double getValueAsDouble(Cell cell) {
    if (cell instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.toDouble(((ByteBufferExtendedCell) cell).getValueByteBuffer(),
          ((ByteBufferExtendedCell) cell).getValuePosition());
    }
    return Bytes.toDouble(cell.getValueArray(), cell.getValueOffset());
  }

  /**
   * Converts the value bytes of the given cell into a BigDecimal
   * @param cell
   * @return value as BigDecimal
   */
  public static BigDecimal getValueAsBigDecimal(Cell cell) {
    if (cell instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.toBigDecimal(((ByteBufferExtendedCell) cell).getValueByteBuffer(),
          ((ByteBufferExtendedCell) cell).getValuePosition(), cell.getValueLength());
    }
    return Bytes.toBigDecimal(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
  }

  /**
   * Compresses the tags to the given outputstream using the TagcompressionContext
   * @param out the outputstream to which the compression should happen
   * @param cell the cell which has tags
   * @param tagCompressionContext the TagCompressionContext
   * @throws IOException can throw IOException if the compression encounters issue
   */
  public static void compressTags(OutputStream out, Cell cell,
                                  TagCompressionContext tagCompressionContext) throws IOException {
    if (cell instanceof ByteBufferExtendedCell) {
      tagCompressionContext.compressTags(out, ((ByteBufferExtendedCell) cell).getTagsByteBuffer(),
          ((ByteBufferExtendedCell) cell).getTagsPosition(), cell.getTagsLength());
    } else {
      tagCompressionContext.compressTags(out, cell.getTagsArray(), cell.getTagsOffset(),
          cell.getTagsLength());
    }
  }

  public static void compressRow(OutputStream out, Cell cell, Dictionary dict) throws IOException {
    if (cell instanceof ByteBufferExtendedCell) {
      Dictionary.write(out, ((ByteBufferExtendedCell) cell).getRowByteBuffer(),
          ((ByteBufferExtendedCell) cell).getRowPosition(), cell.getRowLength(), dict);
    } else {
      Dictionary.write(out, cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(), dict);
    }
  }

  public static void compressFamily(OutputStream out, Cell cell, Dictionary dict)
      throws IOException {
    if (cell instanceof ByteBufferExtendedCell) {
      Dictionary.write(out, ((ByteBufferExtendedCell) cell).getFamilyByteBuffer(),
          ((ByteBufferExtendedCell) cell).getFamilyPosition(), cell.getFamilyLength(), dict);
    } else {
      Dictionary.write(out, cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
          dict);
    }
  }

  public static void compressQualifier(OutputStream out, Cell cell, Dictionary dict)
      throws IOException {
    if (cell instanceof ByteBufferExtendedCell) {
      Dictionary.write(out, ((ByteBufferExtendedCell) cell).getQualifierByteBuffer(),
          ((ByteBufferExtendedCell) cell).getQualifierPosition(), cell.getQualifierLength(), dict);
    } else {
      Dictionary.write(out, cell.getQualifierArray(), cell.getQualifierOffset(),
          cell.getQualifierLength(), dict);
    }
  }

  /**
   * Used when a cell needs to be compared with a key byte[] such as cases of finding the index from
   * the index block, bloom keys from the bloom blocks This byte[] is expected to be serialized in
   * the KeyValue serialization format If the KeyValue (Cell's) serialization format changes this
   * method cannot be used.
   * @param comparator the {@link CellComparator} to use for comparison
   * @param left the cell to be compared
   * @param key the serialized key part of a KeyValue
   * @param offset the offset in the key byte[]
   * @param length the length of the key byte[]
   * @return an int greater than 0 if left is greater than right lesser than 0 if left is lesser
   *         than right equal to 0 if left is equal to right
   */
  public static final int compare(CellComparator comparator, Cell left, byte[] key, int offset,
                                  int length) {
    // row
    short rrowlength = Bytes.toShort(key, offset);
    int c = comparator.compareRows(left, key, offset + Bytes.SIZEOF_SHORT, rrowlength);
    if (c != 0) return c;

    // Compare the rest of the two KVs without making any assumptions about
    // the common prefix. This function will not compare rows anyway, so we
    // don't need to tell it that the common prefix includes the row.
    return compareWithoutRow(comparator, left, key, offset, length, rrowlength);
  }

  /**
   * Compare columnFamily, qualifier, timestamp, and key type (everything except the row). This
   * method is used both in the normal comparator and the "same-prefix" comparator. Note that we are
   * assuming that row portions of both KVs have already been parsed and found identical, and we
   * don't validate that assumption here.
   * @param comparator the {@link CellComparator} to use for comparison
   * @param left the cell to be compared
   * @param right the serialized key part of a key-value
   * @param roffset the offset in the key byte[]
   * @param rlength the length of the key byte[]
   * @param rowlength the row length
   * @return greater than 0 if left cell is bigger, less than 0 if right cell is bigger, 0 if both
   *         cells are equal
   */
  static final int compareWithoutRow(CellComparator comparator, Cell left, byte[] right,
                                     int roffset, int rlength, short rowlength) {
    /***
     * KeyValue Format and commonLength:
     * |_keyLen_|_valLen_|_rowLen_|_rowKey_|_famiLen_|_fami_|_Quali_|....
     * ------------------|-------commonLength--------|--------------
     */
    int commonLength = KeyValue.ROW_LENGTH_SIZE + KeyValue.FAMILY_LENGTH_SIZE + rowlength;

    // commonLength + TIMESTAMP_TYPE_SIZE
    int commonLengthWithTSAndType = KeyValue.TIMESTAMP_TYPE_SIZE + commonLength;
    // ColumnFamily + Qualifier length.
    int lcolumnlength = left.getFamilyLength() + left.getQualifierLength();
    int rcolumnlength = rlength - commonLengthWithTSAndType;

    byte ltype = left.getTypeByte();
    byte rtype = right[roffset + (rlength - 1)];

    // If the column is not specified, the "minimum" key type appears the
    // latest in the sorted order, regardless of the timestamp. This is used
    // for specifying the last key/value in a given row, because there is no
    // "lexicographically last column" (it would be infinitely long). The
    // "maximum" key type does not need this behavior.
    if (lcolumnlength == 0 && ltype == KeyValue.Type.Minimum.getCode()) {
      // left is "bigger", i.e. it appears later in the sorted order
      return 1;
    }
    if (rcolumnlength == 0 && rtype == KeyValue.Type.Minimum.getCode()) {
      return -1;
    }

    int rfamilyoffset = commonLength + roffset;

    // Column family length.
    int lfamilylength = left.getFamilyLength();
    int rfamilylength = right[rfamilyoffset - 1];
    // If left family size is not equal to right family size, we need not
    // compare the qualifiers.
    boolean sameFamilySize = (lfamilylength == rfamilylength);
    if (!sameFamilySize) {
      // comparing column family is enough.
      return CellUtil.compareFamilies(left, right, rfamilyoffset, rfamilylength);
    }
    // Compare family & qualifier together.
    // Families are same. Compare on qualifiers.
    int comparison = CellUtil.compareColumns(left, right, rfamilyoffset, rfamilylength,
        rfamilyoffset + rfamilylength, (rcolumnlength - rfamilylength));
    if (comparison != 0) {
      return comparison;
    }

    // //
    // Next compare timestamps.
    long rtimestamp = Bytes.toLong(right, roffset + (rlength - KeyValue.TIMESTAMP_TYPE_SIZE));
    int compare = comparator.compareTimestamps(left.getTimestamp(), rtimestamp);
    if (compare != 0) {
      return compare;
    }

    // Compare types. Let the delete types sort ahead of puts; i.e. types
    // of higher numbers sort before those of lesser numbers. Maximum (255)
    // appears ahead of everything, and minimum (0) appears after
    // everything.
    return (0xff & rtype) - (0xff & ltype);
  }

  /**
   * @return An new cell is located following input cell. If both of type and timestamp are minimum,
   *         the input cell will be returned directly.
   */
  public static Cell createNextOnRowCol(Cell cell) {
    long ts = cell.getTimestamp();
    byte type = cell.getTypeByte();
    if (type != KeyValue.Type.Minimum.getCode()) {
      type = KeyValue.Type.values()[KeyValue.Type.codeToType(type).ordinal() - 1].getCode();
    } else if (ts != HConstants.OLDEST_TIMESTAMP) {
      ts = ts - 1;
      type = KeyValue.Type.Maximum.getCode();
    } else {
      return cell;
    }
    return createNextOnRowCol(cell, ts, type);
  }

  static Cell createNextOnRowCol(Cell cell, long ts, byte type) {
    if (cell instanceof ByteBufferExtendedCell) {
      return new LastOnRowColByteBufferExtendedCell(
          ((ByteBufferExtendedCell) cell).getRowByteBuffer(),
          ((ByteBufferExtendedCell) cell).getRowPosition(), cell.getRowLength(),
          ((ByteBufferExtendedCell) cell).getFamilyByteBuffer(),
          ((ByteBufferExtendedCell) cell).getFamilyPosition(), cell.getFamilyLength(),
          ((ByteBufferExtendedCell) cell).getQualifierByteBuffer(),
          ((ByteBufferExtendedCell) cell).getQualifierPosition(), cell.getQualifierLength()) {
        @Override
        public long getTimestamp() {
          return ts;
        }

        @Override
        public byte getTypeByte() {
          return type;
        }
      };
    }
    return new LastOnRowColCell(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
        cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
        cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()) {
      @Override
      public long getTimestamp() {
        return ts;
      }

      @Override
      public byte getTypeByte() {
        return type;
      }
    };
  }

  /**
   * Estimate based on keyvalue's serialization format in the RPC layer. Note that there is an extra
   * SIZEOF_INT added to the size here that indicates the actual length of the cell for cases where
   * cell's are serialized in a contiguous format (For eg in RPCs).
   * @param cell
   * @return Estimate of the <code>cell</code> size in bytes plus an extra SIZEOF_INT indicating the
   *         actual cell length.
   */
  public static int estimatedSerializedSizeOf(final Cell cell) {
    return cell.getSerializedSize() + Bytes.SIZEOF_INT;
  }

  /**
   * Calculates the serialized key size. We always serialize in the KeyValue's serialization format.
   * @param cell the cell for which the key size has to be calculated.
   * @return the key size
   */
  public static int estimatedSerializedSizeOfKey(final Cell cell) {
    if (cell instanceof KeyValue) return ((KeyValue) cell).getKeyLength();
    return cell.getRowLength() + cell.getFamilyLength() + cell.getQualifierLength()
        + KeyValue.KEY_INFRASTRUCTURE_SIZE;
  }

  /**
   * This method exists just to encapsulate how we serialize keys. To be replaced by a factory that
   * we query to figure what the Cell implementation is and then, what serialization engine to use
   * and further, how to serialize the key for inclusion in hfile index. TODO.
   * @param cell
   * @return The key portion of the Cell serialized in the old-school KeyValue way or null if passed
   *         a null <code>cell</code>
   */
  public static byte[] getCellKeySerializedAsKeyValueKey(final Cell cell) {
    if (cell == null) return null;
    byte[] b = new byte[KeyValueUtil.keyLength(cell)];
    KeyValueUtil.appendKeyTo(cell, b, 0);
    return b;
  }

  /**
   * Create a Cell that is smaller than all other possible Cells for the given Cell's row.
   * @param cell
   * @return First possible Cell on passed Cell's row.
   */
  public static Cell createFirstOnRow(final Cell cell) {
    if (cell instanceof ByteBufferExtendedCell) {
      return new FirstOnRowByteBufferExtendedCell(
          ((ByteBufferExtendedCell) cell).getRowByteBuffer(),
          ((ByteBufferExtendedCell) cell).getRowPosition(), cell.getRowLength());
    }
    return new FirstOnRowCell(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
  }

  public static Cell createFirstOnRow(final byte[] row, int roffset, short rlength) {
    return new FirstOnRowCell(row, roffset, rlength);
  }

  public static Cell createFirstOnRow(final byte[] row, final byte[] family, final byte[] col) {
    return createFirstOnRow(row, 0, (short) row.length, family, 0, (byte) family.length, col, 0,
        col.length);
  }

  public static Cell createFirstOnRow(final byte[] row, int roffset, short rlength,
                                      final byte[] family, int foffset, byte flength, final byte[] col, int coffset, int clength) {
    return new FirstOnRowColCell(row, roffset, rlength, family, foffset, flength, col, coffset,
        clength);
  }

  public static Cell createFirstOnRow(final byte[] row) {
    return createFirstOnRow(row, 0, (short) row.length);
  }

  public static Cell createFirstOnRowFamily(Cell cell, byte[] fArray, int foff, int flen) {
    if (cell instanceof ByteBufferExtendedCell) {
      return new FirstOnRowColByteBufferExtendedCell(
          ((ByteBufferExtendedCell) cell).getRowByteBuffer(),
          ((ByteBufferExtendedCell) cell).getRowPosition(), cell.getRowLength(),
          ByteBuffer.wrap(fArray), foff, (byte) flen, HConstants.EMPTY_BYTE_BUFFER, 0, 0);
    }
    return new FirstOnRowColCell(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
        fArray, foff, (byte) flen, HConstants.EMPTY_BYTE_ARRAY, 0, 0);
  }

  public static Cell createFirstOnRowCol(final Cell cell) {
    if (cell instanceof ByteBufferExtendedCell) {
      return new FirstOnRowColByteBufferExtendedCell(
          ((ByteBufferExtendedCell) cell).getRowByteBuffer(),
          ((ByteBufferExtendedCell) cell).getRowPosition(), cell.getRowLength(),
          HConstants.EMPTY_BYTE_BUFFER, 0, (byte) 0,
          ((ByteBufferExtendedCell) cell).getQualifierByteBuffer(),
          ((ByteBufferExtendedCell) cell).getQualifierPosition(), cell.getQualifierLength());
    }
    return new FirstOnRowColCell(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
        HConstants.EMPTY_BYTE_ARRAY, 0, (byte) 0, cell.getQualifierArray(),
        cell.getQualifierOffset(), cell.getQualifierLength());
  }

  public static Cell createFirstOnNextRow(final Cell cell) {
    byte[] nextRow = new byte[cell.getRowLength() + 1];
    CellUtil.copyRowTo(cell, nextRow, 0);
    nextRow[nextRow.length - 1] = 0;// maybe not necessary
    return new FirstOnRowCell(nextRow, 0, (short) nextRow.length);
  }

  /**
   * Create a Cell that is smaller than all other possible Cells for the given Cell's rk:cf and
   * passed qualifier.
   * @param cell
   * @param qArray
   * @param qoffest
   * @param qlength
   * @return Last possible Cell on passed Cell's rk:cf and passed qualifier.
   */
  public static Cell createFirstOnRowCol(final Cell cell, byte[] qArray, int qoffest, int qlength) {
    if (cell instanceof ByteBufferExtendedCell) {
      return new FirstOnRowColByteBufferExtendedCell(
          ((ByteBufferExtendedCell) cell).getRowByteBuffer(),
          ((ByteBufferExtendedCell) cell).getRowPosition(), cell.getRowLength(),
          ((ByteBufferExtendedCell) cell).getFamilyByteBuffer(),
          ((ByteBufferExtendedCell) cell).getFamilyPosition(), cell.getFamilyLength(),
          ByteBuffer.wrap(qArray), qoffest, qlength);
    }
    return new FirstOnRowColCell(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
        cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(), qArray, qoffest,
        qlength);
  }

  /**
   * Creates the first cell with the row/family/qualifier of this cell and the given timestamp. Uses
   * the "maximum" type that guarantees that the new cell is the lowest possible for this
   * combination of row, family, qualifier, and timestamp. This cell's own timestamp is ignored.
   * @param cell - cell
   * @param ts
   */
  public static Cell createFirstOnRowColTS(Cell cell, long ts) {
    if (cell instanceof ByteBufferExtendedCell) {
      return new FirstOnRowColTSByteBufferExtendedCell(
          ((ByteBufferExtendedCell) cell).getRowByteBuffer(),
          ((ByteBufferExtendedCell) cell).getRowPosition(), cell.getRowLength(),
          ((ByteBufferExtendedCell) cell).getFamilyByteBuffer(),
          ((ByteBufferExtendedCell) cell).getFamilyPosition(), cell.getFamilyLength(),
          ((ByteBufferExtendedCell) cell).getQualifierByteBuffer(),
          ((ByteBufferExtendedCell) cell).getQualifierPosition(), cell.getQualifierLength(), ts);
    }
    return new FirstOnRowColTSCell(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
        cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
        cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(), ts);
  }

  /**
   * Create a Cell that is larger than all other possible Cells for the given Cell's row.
   * @param cell
   * @return Last possible Cell on passed Cell's row.
   */
  public static Cell createLastOnRow(final Cell cell) {
    if (cell instanceof ByteBufferExtendedCell) {
      return new LastOnRowByteBufferExtendedCell(((ByteBufferExtendedCell) cell).getRowByteBuffer(),
          ((ByteBufferExtendedCell) cell).getRowPosition(), cell.getRowLength());
    }
    return new LastOnRowCell(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
  }

  public static Cell createLastOnRow(final byte[] row) {
    return new LastOnRowCell(row, 0, (short) row.length);
  }

  /**
   * Create a Cell that is larger than all other possible Cells for the given Cell's rk:cf:q. Used
   * in creating "fake keys" for the multi-column Bloom filter optimization to skip the row/column
   * we already know is not in the file.
   * @param cell
   * @return Last possible Cell on passed Cell's rk:cf:q.
   */
  public static Cell createLastOnRowCol(final Cell cell) {
    if (cell instanceof ByteBufferExtendedCell) {
      return new LastOnRowColByteBufferExtendedCell(
          ((ByteBufferExtendedCell) cell).getRowByteBuffer(),
          ((ByteBufferExtendedCell) cell).getRowPosition(), cell.getRowLength(),
          ((ByteBufferExtendedCell) cell).getFamilyByteBuffer(),
          ((ByteBufferExtendedCell) cell).getFamilyPosition(), cell.getFamilyLength(),
          ((ByteBufferExtendedCell) cell).getQualifierByteBuffer(),
          ((ByteBufferExtendedCell) cell).getQualifierPosition(), cell.getQualifierLength());
    }
    return new LastOnRowColCell(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
        cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
        cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
  }

  /**
   * Create a Delete Family Cell for the specified row and family that would be smaller than all
   * other possible Delete Family KeyValues that have the same row and family. Used for seeking.
   * @param row - row key (arbitrary byte array)
   * @param fam - family name
   * @return First Delete Family possible key on passed <code>row</code>.
   */
  public static Cell createFirstDeleteFamilyCellOnRow(final byte[] row, final byte[] fam) {
    return new FirstOnRowDeleteFamilyCell(row, fam);
  }
}
