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

import org.apache.hudi.io.compress.CompressionCodec;

/**
 * Compression codec supported by HFile.
 * <p>
 * The ordinal of these cannot change or else that breaks all existing HFiles out there,
 * even the ones that are not compressed! (They use the NONE algorithm)
 * This is because HFile stores the ordinal to indicate which compression codec is used.
 */
public enum HFileCompressionCodec {
  LZO {
    @Override
    public CompressionCodec toHoodieCompressionCodec() {
      return CompressionCodec.LZO;
    }
  },
  GZIP {
    @Override
    public CompressionCodec toHoodieCompressionCodec() {
      return CompressionCodec.GZIP;
    }
  },
  NONE {
    @Override
    public CompressionCodec toHoodieCompressionCodec() {
      return CompressionCodec.NONE;
    }
  },
  SNAPPY {
    @Override
    public CompressionCodec toHoodieCompressionCodec() {
      return CompressionCodec.SNAPPY;
    }
  },
  LZ4 {
    @Override
    public CompressionCodec toHoodieCompressionCodec() {
      return CompressionCodec.LZ4;
    }
  },
  BZIP2 {
    @Override
    public CompressionCodec toHoodieCompressionCodec() {
      return CompressionCodec.BZIP2;
    }
  },
  ZSTD {
    @Override
    public CompressionCodec toHoodieCompressionCodec() {
      return CompressionCodec.ZSTD;
    }
  };

  /**
   * @return {@link CompressionCodec} enum in Hudi.
   */
  public abstract CompressionCodec toHoodieCompressionCodec();
}
