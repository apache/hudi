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

package org.apache.hudi.hbase.nio;

import static org.apache.hudi.hbase.io.ByteBuffAllocator.NONE;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.InvalidMarkException;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hudi.hbase.io.ByteBuffAllocator.Recycler;
import org.apache.hudi.hbase.util.ByteBufferUtils;
import org.apache.hudi.hbase.util.Bytes;
import org.apache.hudi.hbase.util.ObjectIntPair;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Provides a unified view of all the underlying ByteBuffers and will look as if a bigger
 * sequential buffer. This class provides similar APIs as in {@link ByteBuffer} to put/get int,
 * short, long etc and doing operations like mark, reset, slice etc. This has to be used when
 * data is split across multiple byte buffers and we don't want copy them to single buffer
 * for reading from it.
 */
@InterfaceAudience.Private
public class MultiByteBuff extends ByteBuff {

  private final ByteBuffer[] items;
  // Pointer to the current item in the MBB
  private ByteBuffer curItem = null;
  // Index of the current item in the MBB
  private int curItemIndex = 0;

  private int limit = 0;
  private int limitedItemIndex;
  private int markedItemIndex = -1;
  private final int[] itemBeginPos;

  private Iterator<ByteBuffer> buffsIterator = new Iterator<ByteBuffer>() {
    @Override
    public boolean hasNext() {
      return curItemIndex < limitedItemIndex ||
          (curItemIndex == limitedItemIndex && items[curItemIndex].hasRemaining());
    }

    @Override
    public ByteBuffer next() {
      if (curItemIndex >= items.length) {
        throw new NoSuchElementException("items overflow");
      }
      curItem = items[curItemIndex++];
      return curItem;
    }
  };

  public MultiByteBuff(ByteBuffer... items) {
    this(NONE, items);
  }

  public MultiByteBuff(Recycler recycler, ByteBuffer... items) {
    this(new RefCnt(recycler), items);
  }

  MultiByteBuff(RefCnt refCnt, ByteBuffer... items) {
    this.refCnt = refCnt;
    assert items != null;
    assert items.length > 0;
    this.items = items;
    this.curItem = this.items[this.curItemIndex];
    // See below optimization in getInt(int) where we check whether the given index land in current
    // item. For this we need to check whether the passed index is less than the next item begin
    // offset. To handle this effectively for the last item buffer, we add an extra item into this
    // array.
    itemBeginPos = new int[items.length + 1];
    int offset = 0;
    for (int i = 0; i < items.length; i++) {
      ByteBuffer item = items[i];
      item.rewind();
      itemBeginPos[i] = offset;
      int l = item.limit() - item.position();
      offset += l;
    }
    this.limit = offset;
    this.itemBeginPos[items.length] = offset + 1;
    this.limitedItemIndex = this.items.length - 1;
  }

  private MultiByteBuff(RefCnt refCnt, ByteBuffer[] items, int[] itemBeginPos, int limit,
                        int limitedIndex, int curItemIndex, int markedIndex) {
    this.refCnt = refCnt;
    this.items = items;
    this.curItemIndex = curItemIndex;
    this.curItem = this.items[this.curItemIndex];
    this.itemBeginPos = itemBeginPos;
    this.limit = limit;
    this.limitedItemIndex = limitedIndex;
    this.markedItemIndex = markedIndex;
  }

  /**
   * @throws UnsupportedOperationException MBB does not support
   * array based operations
   */
  @Override
  public byte[] array() {
    throw new UnsupportedOperationException();
  }

  /**
   * @throws UnsupportedOperationException MBB does not
   * support array based operations
   */
  @Override
  public int arrayOffset() {
    throw new UnsupportedOperationException();
  }

  /**
   * @return false. MBB does not support array based operations
   */
  @Override
  public boolean hasArray() {
    return false;
  }

  /**
   * @return the total capacity of this MultiByteBuffer.
   */
  @Override
  public int capacity() {
    checkRefCount();
    int c = 0;
    for (ByteBuffer item : this.items) {
      c += item.capacity();
    }
    return c;
  }

  /**
   * Fetches the byte at the given index. Does not change position of the underlying ByteBuffers
   * @param index
   * @return the byte at the given index
   */
  @Override
  public byte get(int index) {
    checkRefCount();
    int itemIndex = getItemIndex(index);
    return ByteBufferUtils.toByte(this.items[itemIndex], index - this.itemBeginPos[itemIndex]);
  }

  @Override
  public byte getByteAfterPosition(int offset) {
    checkRefCount();
    // Mostly the index specified will land within this current item. Short circuit for that
    int index = offset + this.position();
    int itemIndex = getItemIndexFromCurItemIndex(index);
    return ByteBufferUtils.toByte(this.items[itemIndex], index - this.itemBeginPos[itemIndex]);
  }

  /*
   * Returns in which sub ByteBuffer, the given element index will be available.
   */
  private int getItemIndex(int elemIndex) {
    if (elemIndex < 0) {
      throw new IndexOutOfBoundsException();
    }
    int index = 1;
    while (elemIndex >= this.itemBeginPos[index]) {
      index++;
      if (index == this.itemBeginPos.length) {
        throw new IndexOutOfBoundsException();
      }
    }
    return index - 1;
  }

  /*
   * Returns in which sub ByteBuffer, the given element index will be available. In this case we are
   * sure that the item will be after MBB's current position
   */
  private int getItemIndexFromCurItemIndex(int elemIndex) {
    int index = this.curItemIndex;
    while (elemIndex >= this.itemBeginPos[index]) {
      index++;
      if (index == this.itemBeginPos.length) {
        throw new IndexOutOfBoundsException();
      }
    }
    return index - 1;
  }

  /**
   * Fetches the int at the given index. Does not change position of the underlying ByteBuffers
   * @param index
   * @return the int value at the given index
   */
  @Override
  public int getInt(int index) {
    checkRefCount();
    // Mostly the index specified will land within this current item. Short circuit for that
    int itemIndex;
    if (this.itemBeginPos[this.curItemIndex] <= index
        && this.itemBeginPos[this.curItemIndex + 1] > index) {
      itemIndex = this.curItemIndex;
    } else {
      itemIndex = getItemIndex(index);
    }
    return getInt(index, itemIndex);
  }

  @Override
  public int getIntAfterPosition(int offset) {
    checkRefCount();
    // Mostly the index specified will land within this current item. Short circuit for that
    int index = offset + this.position();
    int itemIndex;
    if (this.itemBeginPos[this.curItemIndex + 1] > index) {
      itemIndex = this.curItemIndex;
    } else {
      itemIndex = getItemIndexFromCurItemIndex(index);
    }
    return getInt(index, itemIndex);
  }

  /**
   * Fetches the short at the given index. Does not change position of the underlying ByteBuffers
   * @param index
   * @return the short value at the given index
   */
  @Override
  public short getShort(int index) {
    checkRefCount();
    // Mostly the index specified will land within this current item. Short circuit for that
    int itemIndex;
    if (this.itemBeginPos[this.curItemIndex] <= index
        && this.itemBeginPos[this.curItemIndex + 1] > index) {
      itemIndex = this.curItemIndex;
    } else {
      itemIndex = getItemIndex(index);
    }
    ByteBuffer item = items[itemIndex];
    int offsetInItem = index - this.itemBeginPos[itemIndex];
    if (item.limit() - offsetInItem >= Bytes.SIZEOF_SHORT) {
      return ByteBufferUtils.toShort(item, offsetInItem);
    }
    if (items.length - 1 == itemIndex) {
      // means cur item is the last one and we wont be able to read a int. Throw exception
      throw new BufferUnderflowException();
    }
    ByteBuffer nextItem = items[itemIndex + 1];
    // Get available one byte from this item and remaining one from next
    short n = 0;
    n = (short) (n ^ (ByteBufferUtils.toByte(item, offsetInItem) & 0xFF));
    n = (short) (n << 8);
    n = (short) (n ^ (ByteBufferUtils.toByte(nextItem, 0) & 0xFF));
    return n;
  }

  @Override
  public short getShortAfterPosition(int offset) {
    checkRefCount();
    // Mostly the index specified will land within this current item. Short circuit for that
    int index = offset + this.position();
    int itemIndex;
    if (this.itemBeginPos[this.curItemIndex + 1] > index) {
      itemIndex = this.curItemIndex;
    } else {
      itemIndex = getItemIndexFromCurItemIndex(index);
    }
    return getShort(index, itemIndex);
  }

  private int getInt(int index, int itemIndex) {
    ByteBuffer item = items[itemIndex];
    int offsetInItem = index - this.itemBeginPos[itemIndex];
    int remainingLen = item.limit() - offsetInItem;
    if (remainingLen >= Bytes.SIZEOF_INT) {
      return ByteBufferUtils.toInt(item, offsetInItem);
    }
    if (items.length - 1 == itemIndex) {
      // means cur item is the last one and we wont be able to read a int. Throw exception
      throw new BufferUnderflowException();
    }
    int l = 0;
    for (int i = 0; i < Bytes.SIZEOF_INT; i++) {
      l <<= 8;
      l ^= get(index + i) & 0xFF;
    }
    return l;
  }

  private short getShort(int index, int itemIndex) {
    ByteBuffer item = items[itemIndex];
    int offsetInItem = index - this.itemBeginPos[itemIndex];
    int remainingLen = item.limit() - offsetInItem;
    if (remainingLen >= Bytes.SIZEOF_SHORT) {
      return ByteBufferUtils.toShort(item, offsetInItem);
    }
    if (items.length - 1 == itemIndex) {
      // means cur item is the last one and we wont be able to read a short. Throw exception
      throw new BufferUnderflowException();
    }
    ByteBuffer nextItem = items[itemIndex + 1];
    // Get available bytes from this item and remaining from next
    short l = 0;
    for (int i = offsetInItem; i < item.capacity(); i++) {
      l = (short) (l << 8);
      l = (short) (l ^ (ByteBufferUtils.toByte(item, i) & 0xFF));
    }
    for (int i = 0; i < Bytes.SIZEOF_SHORT - remainingLen; i++) {
      l = (short) (l << 8);
      l = (short) (l ^ (ByteBufferUtils.toByte(nextItem, i) & 0xFF));
    }
    return l;
  }

  private long getLong(int index, int itemIndex) {
    ByteBuffer item = items[itemIndex];
    int offsetInItem = index - this.itemBeginPos[itemIndex];
    int remainingLen = item.limit() - offsetInItem;
    if (remainingLen >= Bytes.SIZEOF_LONG) {
      return ByteBufferUtils.toLong(item, offsetInItem);
    }
    if (items.length - 1 == itemIndex) {
      // means cur item is the last one and we wont be able to read a long. Throw exception
      throw new BufferUnderflowException();
    }
    long l = 0;
    for (int i = 0; i < Bytes.SIZEOF_LONG; i++) {
      l <<= 8;
      l ^= get(index + i) & 0xFF;
    }
    return l;
  }

  /**
   * Fetches the long at the given index. Does not change position of the underlying ByteBuffers
   * @param index
   * @return the long value at the given index
   */
  @Override
  public long getLong(int index) {
    checkRefCount();
    // Mostly the index specified will land within this current item. Short circuit for that
    int itemIndex;
    if (this.itemBeginPos[this.curItemIndex] <= index
        && this.itemBeginPos[this.curItemIndex + 1] > index) {
      itemIndex = this.curItemIndex;
    } else {
      itemIndex = getItemIndex(index);
    }
    return getLong(index, itemIndex);
  }

  @Override
  public long getLongAfterPosition(int offset) {
    checkRefCount();
    // Mostly the index specified will land within this current item. Short circuit for that
    int index = offset + this.position();
    int itemIndex;
    if (this.itemBeginPos[this.curItemIndex + 1] > index) {
      itemIndex = this.curItemIndex;
    } else {
      itemIndex = getItemIndexFromCurItemIndex(index);
    }
    return getLong(index, itemIndex);
  }

  /**
   * @return this MBB's current position
   */
  @Override
  public int position() {
    checkRefCount();
    return itemBeginPos[this.curItemIndex] + this.curItem.position();
  }

  /**
   * Sets this MBB's position to the given value.
   * @param position
   * @return this object
   */
  @Override
  public MultiByteBuff position(int position) {
    checkRefCount();
    // Short circuit for positioning within the cur item. Mostly that is the case.
    if (this.itemBeginPos[this.curItemIndex] <= position
        && this.itemBeginPos[this.curItemIndex + 1] > position) {
      this.curItem.position(position - this.itemBeginPos[this.curItemIndex]);
      return this;
    }
    int itemIndex = getItemIndex(position);
    // All items from 0 - curItem-1 set position at end.
    for (int i = 0; i < itemIndex; i++) {
      this.items[i].position(this.items[i].limit());
    }
    // All items after curItem set position at begin
    for (int i = itemIndex + 1; i < this.items.length; i++) {
      this.items[i].position(0);
    }
    this.curItem = this.items[itemIndex];
    this.curItem.position(position - this.itemBeginPos[itemIndex]);
    this.curItemIndex = itemIndex;
    return this;
  }

  /**
   * Rewinds this MBB and the position is set to 0
   * @return this object
   */
  @Override
  public MultiByteBuff rewind() {
    checkRefCount();
    for (int i = 0; i < this.items.length; i++) {
      this.items[i].rewind();
    }
    this.curItemIndex = 0;
    this.curItem = this.items[this.curItemIndex];
    this.markedItemIndex = -1;
    return this;
  }

  /**
   * Marks the current position of the MBB
   * @return this object
   */
  @Override
  public MultiByteBuff mark() {
    checkRefCount();
    this.markedItemIndex = this.curItemIndex;
    this.curItem.mark();
    return this;
  }

  /**
   * Similar to {@link ByteBuffer}.reset(), ensures that this MBB
   * is reset back to last marked position.
   * @return This MBB
   */
  @Override
  public MultiByteBuff reset() {
    checkRefCount();
    // when the buffer is moved to the next one.. the reset should happen on the previous marked
    // item and the new one should be taken as the base
    if (this.markedItemIndex < 0) throw new InvalidMarkException();
    ByteBuffer markedItem = this.items[this.markedItemIndex];
    markedItem.reset();
    this.curItem = markedItem;
    // All items after the marked position upto the current item should be reset to 0
    for (int i = this.curItemIndex; i > this.markedItemIndex; i--) {
      this.items[i].position(0);
    }
    this.curItemIndex = this.markedItemIndex;
    return this;
  }

  /**
   * Returns the number of elements between the current position and the
   * limit.
   * @return the remaining elements in this MBB
   */
  @Override
  public int remaining() {
    checkRefCount();
    int remain = 0;
    for (int i = curItemIndex; i < items.length; i++) {
      remain += items[i].remaining();
    }
    return remain;
  }

  /**
   * Returns true if there are elements between the current position and the limt
   * @return true if there are elements, false otherwise
   */
  @Override
  public final boolean hasRemaining() {
    checkRefCount();
    return this.curItem.hasRemaining() || (this.curItemIndex < this.limitedItemIndex
        && this.items[this.curItemIndex + 1].hasRemaining());
  }

  /**
   * A relative method that returns byte at the current position.  Increments the
   * current position by the size of a byte.
   * @return the byte at the current position
   */
  @Override
  public byte get() {
    checkRefCount();
    if (this.curItem.remaining() == 0) {
      if (items.length - 1 == this.curItemIndex) {
        // means cur item is the last one and we wont be able to read a long. Throw exception
        throw new BufferUnderflowException();
      }
      this.curItemIndex++;
      this.curItem = this.items[this.curItemIndex];
    }
    return this.curItem.get();
  }

  /**
   * Returns the short value at the current position. Also advances the position by the size
   * of short
   *
   * @return the short value at the current position
   */
  @Override
  public short getShort() {
    checkRefCount();
    int remaining = this.curItem.remaining();
    if (remaining >= Bytes.SIZEOF_SHORT) {
      return this.curItem.getShort();
    }
    short n = 0;
    n = (short) (n ^ (get() & 0xFF));
    n = (short) (n << 8);
    n = (short) (n ^ (get() & 0xFF));
    return n;
  }

  /**
   * Returns the int value at the current position. Also advances the position by the size of int
   *
   * @return the int value at the current position
   */
  @Override
  public int getInt() {
    checkRefCount();
    int remaining = this.curItem.remaining();
    if (remaining >= Bytes.SIZEOF_INT) {
      return this.curItem.getInt();
    }
    int n = 0;
    for (int i = 0; i < Bytes.SIZEOF_INT; i++) {
      n <<= 8;
      n ^= get() & 0xFF;
    }
    return n;
  }


  /**
   * Returns the long value at the current position. Also advances the position by the size of long
   *
   * @return the long value at the current position
   */
  @Override
  public long getLong() {
    checkRefCount();
    int remaining = this.curItem.remaining();
    if (remaining >= Bytes.SIZEOF_LONG) {
      return this.curItem.getLong();
    }
    long l = 0;
    for (int i = 0; i < Bytes.SIZEOF_LONG; i++) {
      l <<= 8;
      l ^= get() & 0xFF;
    }
    return l;
  }

  /**
   * Copies the content from this MBB's current position to the byte array and fills it. Also
   * advances the position of the MBB by the length of the byte[].
   * @param dst
   */
  @Override
  public void get(byte[] dst) {
    get(dst, 0, dst.length);
  }

  /**
   * Copies the specified number of bytes from this MBB's current position to the byte[]'s offset.
   * Also advances the position of the MBB by the given length.
   * @param dst
   * @param offset within the current array
   * @param length upto which the bytes to be copied
   */
  @Override
  public void get(byte[] dst, int offset, int length) {
    checkRefCount();
    while (length > 0) {
      int toRead = Math.min(length, this.curItem.remaining());
      ByteBufferUtils.copyFromBufferToArray(dst, this.curItem, this.curItem.position(), offset,
          toRead);
      this.curItem.position(this.curItem.position() + toRead);
      length -= toRead;
      if (length == 0) break;
      this.curItemIndex++;
      this.curItem = this.items[this.curItemIndex];
      offset += toRead;
    }
  }

  @Override
  public void get(int sourceOffset, byte[] dst, int offset, int length) {
    checkRefCount();
    int itemIndex = getItemIndex(sourceOffset);
    ByteBuffer item = this.items[itemIndex];
    sourceOffset = sourceOffset - this.itemBeginPos[itemIndex];
    while (length > 0) {
      int toRead = Math.min((item.limit() - sourceOffset), length);
      ByteBufferUtils.copyFromBufferToArray(dst, item, sourceOffset, offset, toRead);
      length -= toRead;
      if (length == 0) break;
      itemIndex++;
      item = this.items[itemIndex];
      offset += toRead;
      sourceOffset = 0;
    }
  }

  /**
   * Marks the limit of this MBB.
   * @param limit
   * @return This MBB
   */
  @Override
  public MultiByteBuff limit(int limit) {
    checkRefCount();
    this.limit = limit;
    // Normally the limit will try to limit within the last BB item
    int limitedIndexBegin = this.itemBeginPos[this.limitedItemIndex];
    if (limit >= limitedIndexBegin && limit < this.itemBeginPos[this.limitedItemIndex + 1]) {
      this.items[this.limitedItemIndex].limit(limit - limitedIndexBegin);
      return this;
    }
    int itemIndex = getItemIndex(limit);
    int beginOffset = this.itemBeginPos[itemIndex];
    int offsetInItem = limit - beginOffset;
    ByteBuffer item = items[itemIndex];
    item.limit(offsetInItem);
    for (int i = this.limitedItemIndex; i < itemIndex; i++) {
      this.items[i].limit(this.items[i].capacity());
    }
    this.limitedItemIndex = itemIndex;
    for (int i = itemIndex + 1; i < this.items.length; i++) {
      this.items[i].limit(this.items[i].position());
    }
    return this;
  }

  /**
   * Returns the limit of this MBB
   * @return limit of the MBB
   */
  @Override
  public int limit() {
    return this.limit;
  }

  /**
   * Returns an MBB which is a sliced version of this MBB. The position, limit and mark
   * of the new MBB will be independent than that of the original MBB.
   * The content of the new MBB will start at this MBB's current position
   * @return a sliced MBB
   */
  @Override
  public MultiByteBuff slice() {
    checkRefCount();
    ByteBuffer[] copy = new ByteBuffer[this.limitedItemIndex - this.curItemIndex + 1];
    for (int i = curItemIndex, j = 0; i <= this.limitedItemIndex; i++, j++) {
      copy[j] = this.items[i].slice();
    }
    return new MultiByteBuff(refCnt, copy);
  }

  /**
   * Returns an MBB which is a duplicate version of this MBB. The position, limit and mark of the
   * new MBB will be independent than that of the original MBB. The content of the new MBB will
   * start at this MBB's current position The position, limit and mark of the new MBB would be
   * identical to this MBB in terms of values.
   * @return a duplicated MBB
   */
  @Override
  public MultiByteBuff duplicate() {
    checkRefCount();
    ByteBuffer[] itemsCopy = new ByteBuffer[this.items.length];
    for (int i = 0; i < this.items.length; i++) {
      itemsCopy[i] = items[i].duplicate();
    }
    return new MultiByteBuff(refCnt, itemsCopy, this.itemBeginPos, this.limit,
        this.limitedItemIndex, this.curItemIndex, this.markedItemIndex);
  }

  /**
   * Writes a byte to this MBB at the current position and increments the position
   * @param b
   * @return this object
   */
  @Override
  public MultiByteBuff put(byte b) {
    checkRefCount();
    if (this.curItem.remaining() == 0) {
      if (this.curItemIndex == this.items.length - 1) {
        throw new BufferOverflowException();
      }
      this.curItemIndex++;
      this.curItem = this.items[this.curItemIndex];
    }
    this.curItem.put(b);
    return this;
  }

  /**
   * Writes a byte to this MBB at the given index and won't affect the position of any of the
   * buffers.
   * @return this object
   * @throws IndexOutOfBoundsException If <tt>index</tt> is negative or not smaller than the
   *           {@link MultiByteBuff#limit}
   */
  @Override
  public MultiByteBuff put(int index, byte b) {
    checkRefCount();
    int itemIndex = getItemIndex(index);
    ByteBuffer item = items[itemIndex];
    item.put(index - itemBeginPos[itemIndex], b);
    return this;
  }

  /**
   * Copies from a src BB to this MBB. This will be absolute positional copying and won't affect the
   * position of any of the buffers.
   * @param destOffset the position in this MBB to which the copy should happen
   * @param src the src MBB
   * @param srcOffset the offset in the src MBB from where the elements should be read
   * @param length the length upto which the copy should happen
   * @throws BufferUnderflowException If there are fewer than length bytes remaining in src
   *           ByteBuff.
   * @throws BufferOverflowException If there is insufficient available space in this MBB for length
   *           bytes.
   */
  @Override
  public MultiByteBuff put(int destOffset, ByteBuff src, int srcOffset, int length) {
    checkRefCount();
    int destItemIndex = getItemIndex(destOffset);
    int srcItemIndex = getItemIndexForByteBuff(src, srcOffset, length);

    ByteBuffer destItem = this.items[destItemIndex];
    destOffset = this.getRelativeOffset(destOffset, destItemIndex);

    ByteBuffer srcItem = getItemByteBuffer(src, srcItemIndex);
    srcOffset = getRelativeOffsetForByteBuff(src, srcOffset, srcItemIndex);

    while (length > 0) {
      int toWrite = destItem.limit() - destOffset;
      if (toWrite <= 0) {
        throw new BufferOverflowException();
      }
      int toRead = srcItem.limit() - srcOffset;
      if (toRead <= 0) {
        throw new BufferUnderflowException();
      }
      int toMove = Math.min(length, Math.min(toRead, toWrite));
      ByteBufferUtils.copyFromBufferToBuffer(srcItem, destItem, srcOffset, destOffset, toMove);
      length -= toMove;
      if (length == 0) {
        break;
      }
      if (toRead < toWrite) {
        if (++srcItemIndex >= getItemByteBufferCount(src)) {
          throw new BufferUnderflowException();
        }
        srcItem = getItemByteBuffer(src, srcItemIndex);
        srcOffset = 0;
        destOffset += toMove;
      } else if (toRead > toWrite) {
        if (++destItemIndex >= this.items.length) {
          throw new BufferOverflowException();
        }
        destItem = this.items[destItemIndex];
        destOffset = 0;
        srcOffset += toMove;
      } else {
        // toRead = toWrite case
        if (++srcItemIndex >= getItemByteBufferCount(src)) {
          throw new BufferUnderflowException();
        }
        srcItem = getItemByteBuffer(src, srcItemIndex);
        srcOffset = 0;
        if (++destItemIndex >= this.items.length) {
          throw new BufferOverflowException();
        }
        destItem = this.items[destItemIndex];
        destOffset = 0;
      }
    }
    return this;
  }

  private static ByteBuffer getItemByteBuffer(ByteBuff buf, int byteBufferIndex) {
    if (buf instanceof SingleByteBuff) {
      if (byteBufferIndex != 0) {
        throw new IndexOutOfBoundsException(
            "index:[" + byteBufferIndex + "],but only index 0 is valid.");
      }
      return buf.nioByteBuffers()[0];
    }
    MultiByteBuff multiByteBuff = (MultiByteBuff) buf;
    if (byteBufferIndex < 0 || byteBufferIndex >= multiByteBuff.items.length) {
      throw new IndexOutOfBoundsException(
          "index:[" + byteBufferIndex + "],but only index [0-" + multiByteBuff.items.length
              + ") is valid.");
    }
    return multiByteBuff.items[byteBufferIndex];
  }

  private static int getItemIndexForByteBuff(ByteBuff byteBuff, int offset, int length) {
    if (byteBuff instanceof SingleByteBuff) {
      ByteBuffer byteBuffer = byteBuff.nioByteBuffers()[0];
      if (offset + length > byteBuffer.limit()) {
        throw new BufferUnderflowException();
      }
      return 0;
    }
    MultiByteBuff multiByteBuff = (MultiByteBuff) byteBuff;
    return multiByteBuff.getItemIndex(offset);
  }

  private static int getRelativeOffsetForByteBuff(ByteBuff byteBuff, int globalOffset,
                                                  int itemIndex) {
    if (byteBuff instanceof SingleByteBuff) {
      if (itemIndex != 0) {
        throw new IndexOutOfBoundsException("index:[" + itemIndex + "],but only index 0 is valid.");
      }
      return globalOffset;
    }
    return ((MultiByteBuff) byteBuff).getRelativeOffset(globalOffset, itemIndex);
  }

  private int getRelativeOffset(int globalOffset, int itemIndex) {
    if (itemIndex < 0 || itemIndex >= this.items.length) {
      throw new IndexOutOfBoundsException(
          "index:[" + itemIndex + "],but only index [0-" + this.items.length + ") is valid.");
    }
    return globalOffset - this.itemBeginPos[itemIndex];
  }

  private static int getItemByteBufferCount(ByteBuff buf) {
    return (buf instanceof SingleByteBuff) ? 1 : ((MultiByteBuff) buf).items.length;
  }

  /**
   * Writes an int to this MBB at its current position. Also advances the position by size of int
   * @param val Int value to write
   * @return this object
   */
  @Override
  public MultiByteBuff putInt(int val) {
    checkRefCount();
    if (this.curItem.remaining() >= Bytes.SIZEOF_INT) {
      this.curItem.putInt(val);
      return this;
    }
    if (this.curItemIndex == this.items.length - 1) {
      throw new BufferOverflowException();
    }
    // During read, we will read as byte by byte for this case. So just write in Big endian
    put(int3(val));
    put(int2(val));
    put(int1(val));
    put(int0(val));
    return this;
  }

  private static byte int3(int x) {
    return (byte) (x >> 24);
  }

  private static byte int2(int x) {
    return (byte) (x >> 16);
  }

  private static byte int1(int x) {
    return (byte) (x >> 8);
  }

  private static byte int0(int x) {
    return (byte) (x);
  }

  /**
   * Copies from the given byte[] to this MBB
   * @param src
   * @return this MBB
   */
  @Override
  public final MultiByteBuff put(byte[] src) {
    return put(src, 0, src.length);
  }

  /**
   * Copies from the given byte[] to this MBB
   * @param src
   * @param offset the position in the byte array from which the copy should be done
   * @param length the length upto which the copy should happen
   * @return this MBB
   */
  @Override
  public MultiByteBuff put(byte[] src, int offset, int length) {
    checkRefCount();
    if (this.curItem.remaining() >= length) {
      ByteBufferUtils.copyFromArrayToBuffer(this.curItem, src, offset, length);
      return this;
    }
    int end = offset + length;
    for (int i = offset; i < end; i++) {
      this.put(src[i]);
    }
    return this;
  }


  /**
   * Writes a long to this MBB at its current position. Also advances the position by size of long
   * @param val Long value to write
   * @return this object
   */
  @Override
  public MultiByteBuff putLong(long val) {
    checkRefCount();
    if (this.curItem.remaining() >= Bytes.SIZEOF_LONG) {
      this.curItem.putLong(val);
      return this;
    }
    if (this.curItemIndex == this.items.length - 1) {
      throw new BufferOverflowException();
    }
    // During read, we will read as byte by byte for this case. So just write in Big endian
    put(long7(val));
    put(long6(val));
    put(long5(val));
    put(long4(val));
    put(long3(val));
    put(long2(val));
    put(long1(val));
    put(long0(val));
    return this;
  }

  private static byte long7(long x) {
    return (byte) (x >> 56);
  }

  private static byte long6(long x) {
    return (byte) (x >> 48);
  }

  private static byte long5(long x) {
    return (byte) (x >> 40);
  }

  private static byte long4(long x) {
    return (byte) (x >> 32);
  }

  private static byte long3(long x) {
    return (byte) (x >> 24);
  }

  private static byte long2(long x) {
    return (byte) (x >> 16);
  }

  private static byte long1(long x) {
    return (byte) (x >> 8);
  }

  private static byte long0(long x) {
    return (byte) (x);
  }

  /**
   * Jumps the current position of this MBB by specified length.
   * @param length
   */
  @Override
  public MultiByteBuff skip(int length) {
    checkRefCount();
    // Get available bytes from this item and remaining from next
    int jump = 0;
    while (true) {
      jump = this.curItem.remaining();
      if (jump >= length) {
        this.curItem.position(this.curItem.position() + length);
        break;
      }
      this.curItem.position(this.curItem.position() + jump);
      length -= jump;
      this.curItemIndex++;
      this.curItem = this.items[this.curItemIndex];
    }
    return this;
  }

  /**
   * Jumps back the current position of this MBB by specified length.
   * @param length
   */
  @Override
  public MultiByteBuff moveBack(int length) {
    checkRefCount();
    while (length != 0) {
      if (length > curItem.position()) {
        length -= curItem.position();
        this.curItem.position(0);
        this.curItemIndex--;
        this.curItem = this.items[curItemIndex];
      } else {
        this.curItem.position(curItem.position() - length);
        break;
      }
    }
    return this;
  }

  /**
   * Returns bytes from current position till length specified, as a single ByteBuffer. When all
   * these bytes happen to be in a single ByteBuffer, which this object wraps, that ByteBuffer item
   * as such will be returned. So users are warned not to change the position or limit of this
   * returned ByteBuffer. The position of the returned byte buffer is at the begin of the required
   * bytes. When the required bytes happen to span across multiple ByteBuffers, this API will copy
   * the bytes to a newly created ByteBuffer of required size and return that.
   *
   * @param length number of bytes required.
   * @return bytes from current position till length specified, as a single ByteButter.
   */
  @Override
  public ByteBuffer asSubByteBuffer(int length) {
    checkRefCount();
    if (this.curItem.remaining() >= length) {
      return this.curItem;
    }
    int offset = 0;
    byte[] dupB = new byte[length];
    int locCurItemIndex = curItemIndex;
    ByteBuffer locCurItem = curItem;
    while (length > 0) {
      int toRead = Math.min(length, locCurItem.remaining());
      ByteBufferUtils.copyFromBufferToArray(dupB, locCurItem, locCurItem.position(), offset,
          toRead);
      length -= toRead;
      if (length == 0) break;
      locCurItemIndex++;
      locCurItem = this.items[locCurItemIndex];
      offset += toRead;
    }
    return ByteBuffer.wrap(dupB);
  }

  /**
   * Returns bytes from given offset till length specified, as a single ByteBuffer. When all these
   * bytes happen to be in a single ByteBuffer, which this object wraps, that ByteBuffer item as
   * such will be returned (with offset in this ByteBuffer where the bytes starts). So users are
   * warned not to change the position or limit of this returned ByteBuffer. When the required bytes
   * happen to span across multiple ByteBuffers, this API will copy the bytes to a newly created
   * ByteBuffer of required size and return that.
   *
   * @param offset the offset in this MBB from where the subBuffer should be created
   * @param length the length of the subBuffer
   * @param pair a pair that will have the bytes from the current position till length specified, as
   *        a single ByteBuffer and offset in that Buffer where the bytes starts. The method would
   *        set the values on the pair that is passed in by the caller
   */
  @Override
  public void asSubByteBuffer(int offset, int length, ObjectIntPair<ByteBuffer> pair) {
    checkRefCount();
    if (this.itemBeginPos[this.curItemIndex] <= offset) {
      int relOffsetInCurItem = offset - this.itemBeginPos[this.curItemIndex];
      if (this.curItem.limit() - relOffsetInCurItem >= length) {
        pair.setFirst(this.curItem);
        pair.setSecond(relOffsetInCurItem);
        return;
      }
    }
    int itemIndex = getItemIndex(offset);
    ByteBuffer item = this.items[itemIndex];
    offset = offset - this.itemBeginPos[itemIndex];
    if (item.limit() - offset >= length) {
      pair.setFirst(item);
      pair.setSecond(offset);
      return;
    }
    byte[] dst = new byte[length];
    int destOffset = 0;
    while (length > 0) {
      int toRead = Math.min(length, item.limit() - offset);
      ByteBufferUtils.copyFromBufferToArray(dst, item, offset, destOffset, toRead);
      length -= toRead;
      if (length == 0) break;
      itemIndex++;
      item = this.items[itemIndex];
      destOffset += toRead;
      offset = 0;
    }
    pair.setFirst(ByteBuffer.wrap(dst));
    pair.setSecond(0);
  }

  /**
   * Copies the content from an this MBB to a ByteBuffer
   * @param out the ByteBuffer to which the copy has to happen, its position will be advanced.
   * @param sourceOffset the offset in the MBB from which the elements has to be copied
   * @param length the length in the MBB upto which the elements has to be copied
   */
  @Override
  public void get(ByteBuffer out, int sourceOffset, int length) {
    checkRefCount();
    int itemIndex = getItemIndex(sourceOffset);
    ByteBuffer in = this.items[itemIndex];
    sourceOffset = sourceOffset - this.itemBeginPos[itemIndex];
    while (length > 0) {
      int toRead = Math.min(in.limit() - sourceOffset, length);
      ByteBufferUtils.copyFromBufferToBuffer(in, out, sourceOffset, toRead);
      length -= toRead;
      if (length == 0) {
        break;
      }
      itemIndex++;
      in = this.items[itemIndex];
      sourceOffset = 0;
    }
  }

  /**
   * Copy the content from this MBB to a byte[] based on the given offset and
   * length
   *
   * @param offset
   *          the position from where the copy should start
   * @param length
   *          the length upto which the copy has to be done
   * @return byte[] with the copied contents from this MBB.
   */
  @Override
  public byte[] toBytes(int offset, int length) {
    checkRefCount();
    byte[] output = new byte[length];
    this.get(offset, output, 0, length);
    return output;
  }

  private int internalRead(ReadableByteChannel channel, long offset,
                           ChannelReader reader) throws IOException {
    checkRefCount();
    int total = 0;
    while (buffsIterator.hasNext()) {
      ByteBuffer buffer = buffsIterator.next();
      int len = read(channel, buffer, offset, reader);
      if (len > 0) {
        total += len;
        offset += len;
      }
      if (buffer.hasRemaining()) {
        break;
      }
    }
    return total;
  }

  @Override
  public int read(ReadableByteChannel channel) throws IOException {
    return internalRead(channel, 0, CHANNEL_READER);
  }

  @Override
  public int read(FileChannel channel, long offset) throws IOException {
    return internalRead(channel, offset, FILE_READER);
  }

  @Override
  public int write(FileChannel channel, long offset) throws IOException {
    checkRefCount();
    int total = 0;
    while (buffsIterator.hasNext()) {
      ByteBuffer buffer = buffsIterator.next();
      while (buffer.hasRemaining()) {
        int len = channel.write(buffer, offset);
        total += len;
        offset += len;
      }
    }
    return total;
  }

  @Override
  public ByteBuffer[] nioByteBuffers() {
    checkRefCount();
    return this.items;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof MultiByteBuff)) return false;
    if (this == obj) return true;
    MultiByteBuff that = (MultiByteBuff) obj;
    if (this.capacity() != that.capacity()) return false;
    if (ByteBuff.compareTo(this, this.position(), this.limit(), that, that.position(),
        that.limit()) == 0) {
      return true;
    }
    return false;
  }

  @Override
  public int hashCode() {
    int hash = 0;
    for (ByteBuffer b : this.items) {
      hash += b.hashCode();
    }
    return hash;
  }

  @Override
  public MultiByteBuff retain() {
    refCnt.retain();
    return this;
  }
}
