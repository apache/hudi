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

package org.apache.hudi.common.util;

import org.apache.hudi.exception.HoodieException;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;

/**
 * A utility class for numeric.
 */
public class NumericUtils {

  public static String humanReadableByteCount(double bytes) {
    if (bytes < 1024) {
      return String.format("%.1f B", bytes);
    }
    int exp = (int) (Math.log(bytes) / Math.log(1024));
    String pre = "KMGTPE".charAt(exp - 1) + "";
    return String.format("%.1f %sB", bytes / Math.pow(1024, exp), pre);
  }

  public static long getMessageDigestHash(final String algorithmName, final String string) {
    MessageDigest md;
    try {
      md = MessageDigest.getInstance(algorithmName);
    } catch (NoSuchAlgorithmException e) {
      throw new HoodieException(e);
    }
    return asLong(Objects.requireNonNull(md).digest(getUTF8Bytes(string)));
  }

  public static long asLong(byte[] bytes) {
    ValidationUtils.checkState(bytes.length >= 8, "HashCode#asLong() requires >= 8 bytes.");
    return padToLong(bytes);
  }

  public static long padToLong(byte[] bytes) {
    long retVal = (bytes[0] & 0xFF);
    for (int i = 1; i < Math.min(bytes.length, 8); i++) {
      retVal |= (bytes[i] & 0xFFL) << (i * 8);
    }
    return retVal;
  }
}
