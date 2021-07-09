/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.utils;

import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hudi.common.fs.FSUtils;

public class BucketUtils {
  // compatible with the spark bucket name
  private static final Pattern BUCKET_NAME = Pattern.compile(".*_(\\d+)(?:\\..*)?$");

  public static int mod(int x, int y) {
    int r = x % y;
    if (r < 0) {
      return (r + y) % y;
    } else {
      return r;
    }
  }

  public static int bucketId(String key, int numBuckets) {
    return bucketId(Collections.singletonList(key), numBuckets);
  }

  public static int bucketId(List<Object> values, int numBuckets) {
    int hash = 0;
    for (Object value : values) {
      hash = 31 * hash;
      if (value == null) {
        hash += 0;
      } else if (value instanceof Boolean) {
        hash += HiveHasher.hashInt(value.equals(Boolean.TRUE) ? 1 : 0);
      } else if (value instanceof Integer) {
        hash += HiveHasher.hashInt((Integer) value);
      } else if (value instanceof Long) {
        hash += HiveHasher.hashLong((Long) value);
      } else if (value instanceof Float) {
        hash += HiveHasher.hashInt(Float.floatToIntBits((Float) value));
      } else if (value instanceof Double) {
        hash += HiveHasher.hashLong(Double.doubleToLongBits((Double) value));
      } else if (value instanceof String) {
        byte[] a = value.toString().getBytes();
        hash += HiveHasher.hashUnsafeBytes(a, HiveHasher.Platform.BYTE_ARRAY_OFFSET, a.length);
      } else {
        throw new RuntimeException("Unsupported type " + value.getClass().getName());
      }
    }
    return mod(hash & Integer.MAX_VALUE, numBuckets);
  }

  public static int bucketIdFromFileId(String fileId) {
    return Integer.parseInt(fileId.substring(0, 8));
  }

  public static String bucketIdStr(int n) {
    return String.format("%08d", n);
  }

  public static String bucketIdStr(int n, int m) {
    return bucketIdStr(mod(n, m));
  }

  public static String newBucketFileIdPrefix(String bucketId) {
    return FSUtils.createNewFileIdPfx().replaceFirst(".{8}", bucketId);
  }

  public static boolean isBucketFileName(String name) {
    return BUCKET_NAME.matcher(name).matches();
  }
}
