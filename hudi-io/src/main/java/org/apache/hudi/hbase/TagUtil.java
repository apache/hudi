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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hudi.hbase.io.util.StreamUtils;
import org.apache.hudi.hbase.util.ByteBufferUtils;
import org.apache.hudi.hbase.util.Bytes;
import org.apache.hudi.hbase.util.Pair;

import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public final class TagUtil {

  private TagUtil(){}

  /**
   * Creates list of tags from given byte array, expected that it is in the expected tag format.
   * @param b The byte array
   * @param offset The offset in array where tag bytes begin
   * @param length Total length of all tags bytes
   * @return List of tags
   */
  public static List<Tag> asList(byte[] b, int offset, int length) {
    List<Tag> tags = new ArrayList<>();
    int pos = offset;
    while (pos < offset + length) {
      int tagLen = Bytes.readAsInt(b, pos, Tag.TAG_LENGTH_SIZE);
      tags.add(new ArrayBackedTag(b, pos, tagLen + Tag.TAG_LENGTH_SIZE));
      pos += Tag.TAG_LENGTH_SIZE + tagLen;
    }
    return tags;
  }

  /**
   * Reads an int value stored as a VInt at tag's given offset.
   * @param tag The Tag
   * @param offset The offset where VInt bytes begin
   * @return A pair of the int value and number of bytes taken to store VInt
   * @throws IOException When varint is malformed and not able to be read correctly
   */
  public static Pair<Integer, Integer> readVIntValuePart(Tag tag, int offset) throws IOException {
    if (tag.hasArray()) {
      return StreamUtils.readRawVarint32(tag.getValueArray(), offset);
    }
    return StreamUtils.readRawVarint32(tag.getValueByteBuffer(), offset);
  }

  /**
   * @return A List&lt;Tag&gt; of any Tags found in <code>cell</code> else null.
   */
  public static List<Tag> carryForwardTags(final Cell cell) {
    return carryForwardTags(null, cell);
  }

  /**
   * Add to <code>tagsOrNull</code> any Tags <code>cell</code> is carrying or null if none.
   */
  public static List<Tag> carryForwardTags(final List<Tag> tagsOrNull, final Cell cell) {
    Iterator<Tag> itr = PrivateCellUtil.tagsIterator(cell);
    if (itr == EMPTY_TAGS_ITR) {
      // If no Tags, return early.
      return tagsOrNull;
    }
    List<Tag> tags = tagsOrNull;
    if (tags == null) {
      tags = new ArrayList<>();
    }
    while (itr.hasNext()) {
      tags.add(itr.next());
    }
    return tags;
  }

  public static byte[] concatTags(byte[] tags, Cell cell) {
    int cellTagsLen = cell.getTagsLength();
    if (cellTagsLen == 0) {
      // If no Tags, return early.
      return tags;
    }
    byte[] b = new byte[tags.length + cellTagsLen];
    int pos = Bytes.putBytes(b, 0, tags, 0, tags.length);
    if (cell instanceof ByteBufferExtendedCell) {
      ByteBufferUtils.copyFromBufferToArray(b, ((ByteBufferExtendedCell) cell).getTagsByteBuffer(),
          ((ByteBufferExtendedCell) cell).getTagsPosition(), pos, cellTagsLen);
    } else {
      Bytes.putBytes(b, pos, cell.getTagsArray(), cell.getTagsOffset(), cellTagsLen);
    }
    return b;
  }

  /**
   * @return Carry forward the TTL tag.
   */
  public static List<Tag> carryForwardTTLTag(final List<Tag> tagsOrNull, final long ttl) {
    if (ttl == Long.MAX_VALUE) {
      return tagsOrNull;
    }
    List<Tag> tags = tagsOrNull;
    // If we are making the array in here, given we are the last thing checked, we'll be only thing
    // in the array so set its size to '1' (I saw this being done in earlier version of
    // tag-handling).
    if (tags == null) {
      tags = new ArrayList<>(1);
    } else {
      // Remove existing TTL tags if any
      Iterator<Tag> tagsItr = tags.iterator();
      while (tagsItr.hasNext()) {
        Tag tag = tagsItr.next();
        if (tag.getType() == TagType.TTL_TAG_TYPE) {
          tagsItr.remove();
          break;
        }
      }
    }
    tags.add(new ArrayBackedTag(TagType.TTL_TAG_TYPE, Bytes.toBytes(ttl)));
    return tags;
  }

  /**
   * Write a list of tags into a byte array
   * Note : these are all purely internal APIs. It helps in
   * cases where we have set of tags and we would want to create a cell out of it. Say in Mobs we
   * create a reference tags to indicate the presence of mob data. Also note that these are not
   * exposed to CPs also
   * @param tags The list of tags
   * @return the serialized tag data as bytes
   */
  public static byte[] fromList(List<Tag> tags) {
    if (tags == null || tags.isEmpty()) {
      return HConstants.EMPTY_BYTE_ARRAY;
    }
    int length = 0;
    for (Tag tag : tags) {
      length += tag.getValueLength() + Tag.INFRASTRUCTURE_SIZE;
    }
    byte[] b = new byte[length];
    int pos = 0;
    int tlen;
    for (Tag tag : tags) {
      tlen = tag.getValueLength();
      pos = Bytes.putAsShort(b, pos, tlen + Tag.TYPE_LENGTH_SIZE);
      pos = Bytes.putByte(b, pos, tag.getType());
      if (tag.hasArray()) {
        pos = Bytes.putBytes(b, pos, tag.getValueArray(), tag.getValueOffset(), tlen);
      } else {
        ByteBufferUtils.copyFromBufferToArray(b, tag.getValueByteBuffer(), tag.getValueOffset(),
            pos, tlen);
        pos += tlen;
      }
    }
    return b;
  }

  /**
   * Iterator returned when no Tags. Used by CellUtil too.
   */
  static final Iterator<Tag> EMPTY_TAGS_ITR = new Iterator<Tag>() {
    @Override
    public boolean hasNext() {
      return false;
    }

    @Override
    // TODO(yihua)
    //@edu.umd.cs.findbugs.annotations.SuppressWarnings(value="IT_NO_SUCH_ELEMENT",
    //    justification="Intentional")
    public Tag next() {
      return null;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  };
}
