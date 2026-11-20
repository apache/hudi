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

package org.apache.hudi.utilities.unstructured.embedding.prep;

import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSupplier;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.CRC32;

/**
 * Streams a file once through a fixed buffer and records its size and CRC32 in O(1) memory, so a preprocessor can prove which
 * bytes it was given (aux keys {@code src_bytes}, {@code src_crc32}).
 */
public final class StreamDigest {

  public static final String AUX_SRC_BYTES = "src_bytes";
  public static final String AUX_SRC_CRC32 = "src_crc32";
  private static final int BUFFER_BYTES = 8 * 1024;

  private StreamDigest() {
  }

  public static Map<String, String> digest(BlobBytesSupplier bytes) throws IOException {
    CRC32 crc = new CRC32();
    long total = 0;
    byte[] buffer = new byte[BUFFER_BYTES];
    try (InputStream in = bytes.open()) {
      int n;
      while ((n = in.read(buffer)) > 0) {
        crc.update(buffer, 0, n);
        total += n;
      }
    }
    Map<String, String> aux = new HashMap<>();
    aux.put(AUX_SRC_BYTES, String.valueOf(total));
    aux.put(AUX_SRC_CRC32, Long.toHexString(crc.getValue()));
    return aux;
  }
}
