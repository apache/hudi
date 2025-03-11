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

import org.apache.hudi.io.hfile.writer.ChecksumType;

public class HFileBlockWriteAttributes {
  public static final int DEFAULT_BYTES_PER_CHECKSUM = 16 * 1024;
  public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
  public static final ChecksumType CHECKSUM_TYPE = ChecksumType.NULL;
  protected long startOffsetInBuff = -1;
  protected int blockSize;

  public HFileBlockWriteAttributes(long startOffsetInBuff, int blockSize) {
    this.startOffsetInBuff = startOffsetInBuff;
    this.blockSize = blockSize;
  }

  public static class Builder {
    private long startOffsetInBuff = -1;
    private int blockSize;

    public Builder() {
      // Optionally, you can initialize default values here.
    }

    public Builder startOffsetInBuff(long startOffsetInBuff) {
      this.startOffsetInBuff = startOffsetInBuff;
      return this;
    }

    public Builder blockSize(int blockSize) {
      this.blockSize = blockSize;
      return this;
    }

    public HFileBlockWriteAttributes build() {
      return new HFileBlockWriteAttributes(startOffsetInBuff, blockSize);
    }
  }
}
