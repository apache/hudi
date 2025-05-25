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

/**
 * Type of checksum used to validate the integrity of data block.
 * It determines the number of bytes used for checksum.
 */
public enum ChecksumType {

  NULL((byte) 0) {
    @Override
    public String getName() {
      return "NULL";
    }
  },

  CRC32((byte) 1) {
    @Override
    public String getName() {
      return "CRC32";
    }
  },

  CRC32C((byte) 2) {
    @Override
    public String getName() {
      return "CRC32C";
    }
  };

  private final byte code;

  public static ChecksumType getDefaultChecksumType() {
    return ChecksumType.CRC32C;
  }

  /** returns the name of this checksum type */
  public abstract String getName();

  private ChecksumType(final byte c) {
    this.code = c;
  }

  public byte getCode() {
    return this.code;
  }

  /**
   * Use designated byte value to indicate checksum type.
   * @return type associated with passed code
   */
  public static ChecksumType codeToType(final byte b) {
    for (ChecksumType t : ChecksumType.values()) {
      if (t.getCode() == b) {
        return t;
      }
    }
    throw new RuntimeException("Unknown checksum type code " + b);
  }

  /**
   * Map a checksum name to a specific type.
   * @return type associated with the name
   */
  public static ChecksumType nameToType(final String name) {
    for (ChecksumType t : ChecksumType.values()) {
      if (t.getName().equals(name)) {
        return t;
      }
    }
    throw new RuntimeException("Unknown checksum type name " + name);
  }
}
