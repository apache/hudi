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

import org.apache.hudi.common.util.Base64CodecUtil;

/**
 * Hoodie object ID representing any partition.
 */
public class PartitionID extends HoodieID {

  private static final Type TYPE = Type.PARTITION;
  private static final HashID.Size ID_PARTITION_HASH_SIZE = HashID.Size.BITS_64;
  private final byte[] hash;

  public PartitionID(final String message) {
    this.hash = HashID.hash(message, ID_PARTITION_HASH_SIZE);
  }

  @Override
  public int bits() {
    return ID_PARTITION_HASH_SIZE.byteSize();
  }

  @Override
  public byte[] asBytes() {
    return this.hash;
  }

  @Override
  public String asBase64EncodedString() {
    return Base64CodecUtil.encode(this.hash);
  }

  @Override
  public String toString() {
    return new String(this.hash);
  }

  @Override
  protected Type getType() {
    return TYPE;
  }

}
