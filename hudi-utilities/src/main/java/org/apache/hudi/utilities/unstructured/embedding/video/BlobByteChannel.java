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

package org.apache.hudi.utilities.unstructured.embedding.video;

import org.apache.hudi.utilities.unstructured.embedding.source.BlobReadException;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobTruncatedException;
import org.apache.hudi.utilities.unstructured.embedding.source.SeekableBlob;
import org.apache.hudi.utilities.unstructured.embedding.source.SeekableBlobInputStream;

import org.jcodec.common.io.SeekableByteChannel;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.NonWritableChannelException;

/**
 * A read-only JCodec {@link SeekableByteChannel} over a {@link SeekableBlob}: JCodec's MP4 demuxer reads the box headers, the
 * {@code moov} box and the samples it is asked for by position, so only those ranges of the blob are read, never the whole file.
 *
 * <p>A storage failure under a read or a seek is thrown as a {@link BlobReadException} (a {@link BlobTruncatedException} is the file's
 * own end and passes as is), so the frame source tells it from corrupt video data, as {@link SeekableBlobInputStream} does for
 * FFmpeg.
 */
final class BlobByteChannel implements SeekableByteChannel {

  /** Bytes copied per read through the transfer buffer: JCodec's own reads are a box header or one sample. */
  private static final int TRANSFER_BYTES = 64 * 1024;

  private final SeekableBlob blob;
  private byte[] transfer;
  private boolean open = true;

  BlobByteChannel(SeekableBlob blob) {
    this.blob = blob;
  }

  @Override
  public long position() throws IOException {
    try {
      return blob.getPos();
    } catch (IOException e) {
      throw classify(e);
    }
  }

  @Override
  public SeekableByteChannel setPosition(long newPosition) throws IOException {
    try {
      // JCodec may position at the end of the file (after the last box); the blob seeks within [0, length]
      blob.seek(Math.min(Math.max(0, newPosition), blob.length()));
    } catch (IOException e) {
      throw classify(e);
    }
    return this;
  }

  @Override
  public long size() {
    return blob.length();
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    int wanted = Math.min(dst.remaining(), TRANSFER_BYTES);
    if (wanted == 0) {
      return 0;
    }
    if (dst.hasArray()) {
      int n = readBlob(dst.array(), dst.arrayOffset() + dst.position(), wanted);
      if (n > 0) {
        dst.position(dst.position() + n);
      }
      return n;
    }
    if (transfer == null) {
      transfer = new byte[TRANSFER_BYTES];
    }
    int n = readBlob(transfer, 0, wanted);
    if (n > 0) {
      dst.put(transfer, 0, n);
    }
    return n;
  }

  private int readBlob(byte[] buffer, int offset, int len) throws IOException {
    try {
      return blob.read(buffer, offset, len);
    } catch (IOException e) {
      throw classify(e);
    }
  }

  @Override
  public SeekableByteChannel truncate(long size) {
    throw new NonWritableChannelException();
  }

  @Override
  public int write(ByteBuffer src) {
    throw new NonWritableChannelException();
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public void close() throws IOException {
    if (open) {
      open = false;
      blob.close();
    }
  }

  private static IOException classify(IOException e) {
    return e instanceof BlobTruncatedException || e instanceof BlobReadException ? e : new BlobReadException(e);
  }
}
