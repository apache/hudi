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

package org.apache.hudi.hbase.util;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Utility methods for working with {@link ByteRange}.
 */
@InterfaceAudience.Public
public class ByteRangeUtils {
  public static int numEqualPrefixBytes(ByteRange left, ByteRange right, int rightInnerOffset) {
    int maxCompares = Math.min(left.getLength(), right.getLength() - rightInnerOffset);
    final byte[] lbytes = left.getBytes();
    final byte[] rbytes = right.getBytes();
    final int loffset = left.getOffset();
    final int roffset = right.getOffset();
    for (int i = 0; i < maxCompares; ++i) {
      if (lbytes[loffset + i] != rbytes[roffset + rightInnerOffset + i]) {
        return i;
      }
    }
    return maxCompares;
  }

  public static ArrayList<byte[]> copyToNewArrays(Collection<ByteRange> ranges) {
    if (ranges == null) {
      return new ArrayList<>(0);
    }
    ArrayList<byte[]> arrays = Lists.newArrayListWithCapacity(ranges.size());
    for (ByteRange range : ranges) {
      arrays.add(range.deepCopyToNewArray());
    }
    return arrays;
  }

  public static ArrayList<ByteRange> fromArrays(Collection<byte[]> arrays) {
    if (arrays == null) {
      return new ArrayList<>(0);
    }
    ArrayList<ByteRange> ranges = Lists.newArrayListWithCapacity(arrays.size());
    for (byte[] array : arrays) {
      ranges.add(new SimpleMutableByteRange(array));
    }
    return ranges;
  }

  public static void write(OutputStream os, ByteRange byteRange) throws IOException {
    os.write(byteRange.getBytes(), byteRange.getOffset(), byteRange.getLength());
  }

  public static void write(OutputStream os, ByteRange byteRange, int byteRangeInnerOffset)
      throws IOException {
    os.write(byteRange.getBytes(), byteRange.getOffset() + byteRangeInnerOffset,
        byteRange.getLength() - byteRangeInnerOffset);
  }
}
