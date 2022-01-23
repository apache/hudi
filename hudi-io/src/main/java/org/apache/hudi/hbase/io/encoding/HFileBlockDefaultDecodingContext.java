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

package org.apache.hudi.hbase.io.encoding;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.io.IOUtils;
import org.apache.hudi.hbase.io.ByteBuffInputStream;
import org.apache.hudi.hbase.io.TagCompressionContext;
import org.apache.hudi.hbase.io.compress.Compression;
import org.apache.hudi.hbase.io.crypto.Cipher;
import org.apache.hudi.hbase.io.crypto.Decryptor;
import org.apache.hudi.hbase.io.crypto.Encryption;
import org.apache.hudi.hbase.io.hfile.HFileContext;
import org.apache.hudi.hbase.io.util.BlockIOUtils;
import org.apache.hudi.hbase.nio.ByteBuff;
import org.apache.hudi.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A default implementation of {@link HFileBlockDecodingContext}. It assumes the
 * block data section is compressed as a whole.
 *
 * @see HFileBlockDefaultEncodingContext for the default compression context
 *
 */
@InterfaceAudience.Private
public class HFileBlockDefaultDecodingContext implements HFileBlockDecodingContext {
  private final HFileContext fileContext;
  private TagCompressionContext tagCompressionContext;

  public HFileBlockDefaultDecodingContext(HFileContext fileContext) {
    this.fileContext = fileContext;
  }

  @Override
  public void prepareDecoding(int onDiskSizeWithoutHeader, int uncompressedSizeWithoutHeader,
                              ByteBuff blockBufferWithoutHeader, ByteBuff onDiskBlock) throws IOException {
    final ByteBuffInputStream byteBuffInputStream = new ByteBuffInputStream(onDiskBlock);
    InputStream dataInputStream = new DataInputStream(byteBuffInputStream);

    try {
      Encryption.Context cryptoContext = fileContext.getEncryptionContext();
      if (cryptoContext != Encryption.Context.NONE) {

        Cipher cipher = cryptoContext.getCipher();
        Decryptor decryptor = cipher.getDecryptor();
        decryptor.setKey(cryptoContext.getKey());

        // Encrypted block format:
        // +--------------------------+
        // | byte iv length           |
        // +--------------------------+
        // | iv data ...              |
        // +--------------------------+
        // | encrypted block data ... |
        // +--------------------------+

        int ivLength = dataInputStream.read();
        if (ivLength > 0) {
          byte[] iv = new byte[ivLength];
          IOUtils.readFully(dataInputStream, iv);
          decryptor.setIv(iv);
          // All encrypted blocks will have a nonzero IV length. If we see an IV
          // length of zero, this means the encoding context had 0 bytes of
          // plaintext to encode.
          decryptor.reset();
          dataInputStream = decryptor.createDecryptionStream(dataInputStream);
        }
        onDiskSizeWithoutHeader -= Bytes.SIZEOF_BYTE + ivLength;
      }

      Compression.Algorithm compression = fileContext.getCompression();
      if (compression != Compression.Algorithm.NONE) {
        Compression.decompress(blockBufferWithoutHeader, dataInputStream,
            uncompressedSizeWithoutHeader, compression);
      } else {
        BlockIOUtils.readFullyWithHeapBuffer(dataInputStream, blockBufferWithoutHeader,
            onDiskSizeWithoutHeader);
      }
    } finally {
      byteBuffInputStream.close();
      dataInputStream.close();
    }
  }

  @Override
  public HFileContext getHFileContext() {
    return this.fileContext;
  }

  public TagCompressionContext getTagCompressionContext() {
    return tagCompressionContext;
  }

  public void setTagCompressionContext(TagCompressionContext tagCompressionContext) {
    this.tagCompressionContext = tagCompressionContext;
  }
}
