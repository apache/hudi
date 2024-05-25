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

import java.nio.ByteBuffer;
import java.util.Base64;

import static org.apache.hudi.common.util.StringUtils.fromUTF8Bytes;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;

/**
 * Utils for Base64 encoding and decoding.
 */
public final class Base64CodecUtil {

  /**
   * Decodes data from the input string into using the encoding scheme.
   *
   * @param encodedString - Base64 encoded string to decode
   * @return A newly-allocated byte array containing the decoded bytes.
   */
  public static byte[] decode(String encodedString) {
    return Base64.getDecoder().decode(getUTF8Bytes(encodedString));
  }

  /**
   * Decodes data from the input {@link ByteBuffer} into using the encoding scheme.
   *
   * @param byteBuffer input data in byte buffer to be decoded.
   * @return A newly-allocated {@link ByteBuffer} containing the decoded bytes.
   */
  public static ByteBuffer decode(ByteBuffer byteBuffer) {
    return Base64.getDecoder().decode(byteBuffer);
  }

  /**
   * Encodes all bytes from the specified byte array into String using StandardCharsets.UTF_8.
   *
   * @param data byte[] source data
   * @return base64 encoded data
   */
  public static String encode(byte[] data) {
    return fromUTF8Bytes(Base64.getEncoder().encode(data));
  }

}
