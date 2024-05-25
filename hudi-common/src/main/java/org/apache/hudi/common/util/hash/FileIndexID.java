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
 * Hoodie object ID representing any file.
 */
public class FileIndexID extends HoodieIndexID {

  private static final Type TYPE = Type.FILE;
  private static final HashID.Size ID_FILE_HASH_SIZE = HashID.Size.BITS_128;
  private final String fileName;
  private final byte[] hash;

  public FileIndexID(final String fileName) {
    this.fileName = fileName;
    this.hash = HashID.hash(fileName, ID_FILE_HASH_SIZE);
  }

  @Override
  public String getName() {
    return fileName;
  }

  @Override
  public int bits() {
    return ID_FILE_HASH_SIZE.byteSize();
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
