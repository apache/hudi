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

import org.apache.yetus.audience.InterfaceAudience;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * An extended version of Cell that allows CPs manipulate Tags.
 */
// Added by HBASE-19092 to expose Tags to CPs (history server) w/o exposing ExtendedCell.
// Why is this in hbase-common and not in hbase-server where it is used?
// RawCell is an odd name for a class that is only for CPs that want to manipulate Tags on
// server-side only w/o exposing ExtendedCell -- super rare, super exotic.
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
public interface RawCell extends Cell {
  static final int MAX_TAGS_LENGTH = (2 * Short.MAX_VALUE) + 1;

  /**
   * Allows cloning the tags in the cell to a new byte[]
   * @return the byte[] having the tags
   */
  default byte[] cloneTags() {
    return PrivateCellUtil.cloneTags(this);
  }

  /**
   * Creates a list of tags in the current cell
   * @return a list of tags
   */
  default Iterator<Tag> getTags() {
    return PrivateCellUtil.tagsIterator(this);
  }

  /**
   * Returns the specific tag of the given type
   * @param type the type of the tag
   * @return the specific tag if available or null
   */
  default Optional<Tag> getTag(byte type) {
    return PrivateCellUtil.getTag(this, type);
  }

  /**
   * Check the length of tags. If it is invalid, throw IllegalArgumentException
   * @param tagsLength the given length of tags
   * @throws IllegalArgumentException if tagslength is invalid
   */
  public static void checkForTagsLength(int tagsLength) {
    if (tagsLength > MAX_TAGS_LENGTH) {
      throw new IllegalArgumentException("tagslength " + tagsLength + " > " + MAX_TAGS_LENGTH);
    }
  }

  /**
   * @return A new cell which is having the extra tags also added to it.
   */
  public static Cell createCell(Cell cell, List<Tag> tags) {
    return PrivateCellUtil.createCell(cell, tags);
  }
}
