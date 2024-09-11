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

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.UUID;

import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

/**
 * Tests {@link Base64CodecUtil}.
 */
public class TestBase64CodecUtil {

  @Test
  public void testCodec() {

    int times = 100;
    UUID uuid = UUID.randomUUID();

    for (int i = 0; i < times; i++) {

      byte[] originalData = getUTF8Bytes(uuid.toString());

      String encodeData = Base64CodecUtil.encode(originalData);
      byte[] decodeData = Base64CodecUtil.decode(encodeData);

      ByteBuffer encodedByteBuffer = ByteBuffer.wrap(getUTF8Bytes(encodeData));
      ByteBuffer decodeByteBuffer = Base64CodecUtil.decode(encodedByteBuffer);

      assertArrayEquals(originalData, decodeData);
      assertArrayEquals(originalData, decodeByteBuffer.array());
    }

  }

}
