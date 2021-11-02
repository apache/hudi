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

package org.apache.hudi.common.util.hash;

import org.apache.hudi.exception.HoodieNotSupportedException;

import java.io.Serializable;

/**
 * A serializable ID that can be used to identify any Hoodie table fields and resources.
 */
public abstract class HoodieID implements Serializable {

  private static final long serialVersionUID = 1L;

  /**
   * Supported ID types.
   */
  public enum Type {
    COLUMN("HoodieColumnID"),
    PARTITION("HoodiePartitionID"),
    FILE("HoodieFileID");

    private final String name;

    Type(final String name) {
      this.name = name;
    }

    @Override
    public String toString() {
      return "Type{name='" + name + "'}";
    }
  }

  /**
   * Get the number of bits representing this ID in memory.
   * <p>
   * Note: Will be in multiples of 8 only.
   *
   * @return The number of bits in this ID
   */
  public abstract int bits();

  /**
   * Get this ID as a byte array.
   *
   * @return A byte array representing this ID
   */
  public abstract byte[] asBytes();

  /**
   * Get the String version of this ID.
   *
   * @return String version of this ID.
   */
  public abstract String toString();

  /**
   *
   */
  public String asBase64EncodedString() {
    throw new HoodieNotSupportedException("Unsupported hash for " + getType());
  }

  /**
   * Get the ID type.
   *
   * @return This ID type
   */
  protected abstract Type getType();

  /**
   * Is this ID a ColumnID type ?
   *
   * @return True if this ID of ColumnID type
   */
  public final boolean isColumnID() {
    return (getType() == Type.COLUMN);
  }

  /**
   * Is this ID a Partition type ?
   *
   * @return True if this ID of PartitionID type
   */
  public final boolean isPartition() {
    return (getType() == Type.PARTITION);
  }

  /**
   * Is this ID a FileID type ?
   *
   * @return True if this ID of FileID type
   */
  public final boolean isFileID() {
    return (getType() == Type.FILE);
  }

}
