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

package org.apache.hudi.hbase.io.hfile;

import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.hudi.hbase.nio.ByteBuff;
import org.apache.hudi.hbase.util.Bytes;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Various types of HFile blocks. Ordinal values of these enum constants must not be relied upon.
 * The values in the enum appear in the order they appear in a version 2 HFile.
 */
@InterfaceAudience.Private
public enum BlockType {

  // Scanned block section

  /** Data block, both versions */
  DATA("DATABLK*", BlockCategory.DATA),

  /** An encoded data block (e.g. with prefix compression), version 2 */
  ENCODED_DATA("DATABLKE", BlockCategory.DATA) {
    @Override
    public int getId() {
      return DATA.ordinal();
    }
  },

  /** Version 2 leaf index block. Appears in the data block section */
  LEAF_INDEX("IDXLEAF2", BlockCategory.INDEX),

  /** Bloom filter block, version 2 */
  BLOOM_CHUNK("BLMFBLK2", BlockCategory.BLOOM),

  // Non-scanned block section

  /** Meta blocks */
  META("METABLKc", BlockCategory.META),

  /** Intermediate-level version 2 index in the non-data block section */
  INTERMEDIATE_INDEX("IDXINTE2", BlockCategory.INDEX),

  // Load-on-open section.

  /** Root index block, also used for the single-level meta index, version 2 */
  ROOT_INDEX("IDXROOT2", BlockCategory.INDEX),

  /** File info, version 2 */
  FILE_INFO("FILEINF2", BlockCategory.META),

  /** General Bloom filter metadata, version 2 */
  GENERAL_BLOOM_META("BLMFMET2", BlockCategory.BLOOM),

  /** Delete Family Bloom filter metadata, version 2 */
  DELETE_FAMILY_BLOOM_META("DFBLMET2", BlockCategory.BLOOM),

  // Trailer

  /** Fixed file trailer, both versions (always just a magic string) */
  TRAILER("TRABLK\"$", BlockCategory.META),

  // Legacy blocks

  /** Block index magic string in version 1 */
  INDEX_V1("IDXBLK)+", BlockCategory.INDEX);

  public enum BlockCategory {
    DATA, META, INDEX, BLOOM, ALL_CATEGORIES, UNKNOWN;

    /**
     * Throws an exception if the block category passed is the special category
     * meaning "all categories".
     */
    public void expectSpecific() {
      if (this == ALL_CATEGORIES) {
        throw new IllegalArgumentException("Expected a specific block " +
            "category but got " + this);
      }
    }
  }

  public static final int MAGIC_LENGTH = 8;

  private final byte[] magic;
  private final BlockCategory metricCat;

  private BlockType(String magicStr, BlockCategory metricCat) {
    magic = Bytes.toBytes(magicStr);
    this.metricCat = metricCat;
    assert magic.length == MAGIC_LENGTH;
  }

  /**
   * Use this instead of {@link #ordinal()}. They work exactly the same, except
   * DATA and ENCODED_DATA get the same id using this method (overridden for
   * {@link #ENCODED_DATA}).
   * @return block type id from 0 to the number of block types - 1
   */
  public int getId() {
    // Default implementation, can be overridden for individual enum members.
    return ordinal();
  }

  public void writeToStream(OutputStream out) throws IOException {
    out.write(magic);
  }

  public void write(DataOutput out) throws IOException {
    out.write(magic);
  }

  public void write(ByteBuffer buf) {
    buf.put(magic);
  }

  public void write(ByteBuff buf) {
    buf.put(magic);
  }

  public BlockCategory getCategory() {
    return metricCat;
  }

  public static BlockType parse(byte[] buf, int offset, int length)
      throws IOException {
    if (length != MAGIC_LENGTH) {
      throw new IOException("Magic record of invalid length: "
          + Bytes.toStringBinary(buf, offset, length));
    }

    for (BlockType blockType : values())
      if (Bytes.compareTo(blockType.magic, 0, MAGIC_LENGTH, buf, offset,
          MAGIC_LENGTH) == 0)
        return blockType;

    throw new IOException("Invalid HFile block magic: "
        + Bytes.toStringBinary(buf, offset, MAGIC_LENGTH));
  }

  public static BlockType read(DataInputStream in) throws IOException {
    byte[] buf = new byte[MAGIC_LENGTH];
    in.readFully(buf);
    return parse(buf, 0, buf.length);
  }

  public static BlockType read(ByteBuff buf) throws IOException {
    byte[] magicBuf = new byte[Math.min(buf.limit() - buf.position(), MAGIC_LENGTH)];
    buf.get(magicBuf);
    BlockType blockType = parse(magicBuf, 0, magicBuf.length);
    // If we got here, we have read exactly MAGIC_LENGTH bytes.
    return blockType;
  }

  /**
   * Put the magic record out to the specified byte array position.
   *
   * @param bytes the byte array
   * @param offset position in the array
   * @return incremented offset
   */
  public int put(byte[] bytes, int offset) {
    System.arraycopy(magic, 0, bytes, offset, MAGIC_LENGTH);
    return offset + MAGIC_LENGTH;
  }

  /**
   * Reads a magic record of the length {@link #MAGIC_LENGTH} from the given
   * stream and expects it to match this block type.
   */
  public void readAndCheck(DataInputStream in) throws IOException {
    byte[] buf = new byte[MAGIC_LENGTH];
    in.readFully(buf);
    if (Bytes.compareTo(buf, magic) != 0) {
      throw new IOException("Invalid magic: expected "
          + Bytes.toStringBinary(magic) + ", got " + Bytes.toStringBinary(buf));
    }
  }

  /**
   * Reads a magic record of the length {@link #MAGIC_LENGTH} from the given
   * byte buffer and expects it to match this block type.
   */
  public void readAndCheck(ByteBuffer in) throws IOException {
    byte[] buf = new byte[MAGIC_LENGTH];
    in.get(buf);
    if (Bytes.compareTo(buf, magic) != 0) {
      throw new IOException("Invalid magic: expected "
          + Bytes.toStringBinary(magic) + ", got " + Bytes.toStringBinary(buf));
    }
  }

  /**
   * @return whether this block type is encoded or unencoded data block
   */
  public final boolean isData() {
    return this == DATA || this == ENCODED_DATA;
  }

}
