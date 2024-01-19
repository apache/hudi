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

package org.apache.hudi.io.hfile;

import org.apache.hudi.io.util.IOUtils;

import java.io.DataInputStream;
import java.io.IOException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hudi.io.hfile.DataSize.MAGIC_LENGTH;

/**
 * Represents the HFile block type.
 * These types are copied from HBase HFile definition to maintain compatibility.
 * Do not delete or reorder the enums as the ordinal is used as the block type ID.
 */
public enum HFileBlockType {
  /**
   * Data block
   */
  DATA("DATABLK*", BlockCategory.DATA),

  /**
   * An encoded data block (e.g. with prefix compression), version 2
   */
  ENCODED_DATA("DATABLKE", BlockCategory.DATA) {
    @Override
    public int getId() {
      return DATA.ordinal();
    }
  },

  /**
   * Version 2 leaf index block. Appears in the data block section
   */
  LEAF_INDEX("IDXLEAF2", BlockCategory.INDEX),

  /**
   * Bloom filter block, version 2
   */
  BLOOM_CHUNK("BLMFBLK2", BlockCategory.BLOOM),

  // Non-scanned block section: these blocks may be skipped for sequential reads.

  /**
   * Meta blocks
   */
  META("METABLKc", BlockCategory.META),

  /**
   * Intermediate-level version 2 index in the non-data block section
   */
  INTERMEDIATE_INDEX("IDXINTE2", BlockCategory.INDEX),

  // Load-on-open section: these blocks must be read upon HFile opening to understand
  // the file structure.

  /**
   * Root index block, also used for the single-level meta index, version 2
   */
  ROOT_INDEX("IDXROOT2", BlockCategory.INDEX),

  /**
   * File info, version 2
   */
  FILE_INFO("FILEINF2", BlockCategory.META),

  /**
   * General Bloom filter metadata, version 2
   */
  GENERAL_BLOOM_META("BLMFMET2", BlockCategory.BLOOM),

  /**
   * Delete Family Bloom filter metadata, version 2
   */
  DELETE_FAMILY_BLOOM_META("DFBLMET2", BlockCategory.BLOOM),

  // Trailer

  /**
   * Fixed file trailer, both versions (always just a magic string)
   */
  TRAILER("TRABLK\"$", BlockCategory.META),

  // Legacy blocks

  /**
   * Block index magic string in version 1
   */
  INDEX_V1("IDXBLK)+", BlockCategory.INDEX);

  public enum BlockCategory {
    DATA, META, INDEX, BLOOM, ALL_CATEGORIES, UNKNOWN;
  }

  private final byte[] magic;
  private final BlockCategory metricCat;

  HFileBlockType(String magicStr, BlockCategory metricCat) {
    magic = magicStr.getBytes(UTF_8);
    this.metricCat = metricCat;
    assert magic.length == MAGIC_LENGTH;
  }

  /**
   * Parses the block type from the block magic.
   *
   * @param buf    input data.
   * @param offset offset to start reading.
   * @return the block type.
   * @throws IOException if the block magic is invalid.
   */
  public static HFileBlockType parse(byte[] buf, int offset)
      throws IOException {
    for (HFileBlockType blockType : values()) {
      if (IOUtils.compareTo(
          blockType.magic, 0, MAGIC_LENGTH, buf, offset, MAGIC_LENGTH) == 0) {
        return blockType;
      }
    }

    throw new IOException("Invalid HFile block magic: "
        + IOUtils.bytesToString(buf, offset, MAGIC_LENGTH));
  }

  /**
   * Uses this instead of {@link #ordinal()}. They work exactly the same, except
   * DATA and ENCODED_DATA get the same id using this method (overridden for
   * {@link #ENCODED_DATA}).
   *
   * @return block type id from 0 to the number of block types - 1.
   */
  public int getId() {
    // Default implementation, can be overridden for individual enum members.
    return ordinal();
  }

  /**
   * Reads a magic record of the length {@link DataSize#MAGIC_LENGTH} from the given
   * stream and expects it to match this block type.
   *
   * @param in input data.
   * @throws IOException when the magic is invalid.
   */
  public void readAndCheckMagic(DataInputStream in) throws IOException {
    byte[] buf = new byte[MAGIC_LENGTH];
    in.readFully(buf);
    if (IOUtils.compareTo(buf, magic) != 0) {
      throw new IOException("Invalid magic: expected "
          + new String(magic) + ", got " + new String(buf));
    }
  }
}
