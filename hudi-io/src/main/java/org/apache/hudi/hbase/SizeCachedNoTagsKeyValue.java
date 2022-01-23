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
import java.io.OutputStream;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * This class is an extension to ContentSizeCachedKeyValue where there are no tags in Cell.
 * Note: Please do not use these objects in write path as it will increase the heap space usage.
 * See https://issues.apache.org/jira/browse/HBASE-13448
 */
@InterfaceAudience.Private
public class SizeCachedNoTagsKeyValue extends SizeCachedKeyValue {

  public SizeCachedNoTagsKeyValue(byte[] bytes, int offset, int length, long seqId, int keyLen) {
    super(bytes, offset, length, seqId, keyLen);
  }

  public SizeCachedNoTagsKeyValue(byte[] bytes, int offset, int length, long seqId, int keyLen,
                                  short rowLen) {
    super(bytes, offset, length, seqId, keyLen, rowLen);
  }

  @Override
  public int getTagsLength() {
    return 0;
  }

  @Override
  public int write(OutputStream out, boolean withTags) throws IOException {
    out.write(this.bytes, this.offset, this.length);
    return this.length;
  }

  @Override
  public int getSerializedSize(boolean withTags) {
    return this.length;
  }
}
