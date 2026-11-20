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
import org.apache.hudi.utilities.unstructured.embedding.source.SeekableBlob;
import org.apache.hudi.utilities.unstructured.embedding.source.SeekableBlobBytesSupplier;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

/**
 * Whether a RIFF file (AVI) holds every byte its top-level chunks declare. An AVI is a run of {@code RIFF} chunks (the first
 * {@code AVI }, then {@code AVIX} ones past 1 GiB), each {@code "RIFF" <little-endian 32-bit size> <form type> <data>}; a file
 * cut short ends inside one, which then declares more bytes than the file holds. FFmpeg re-derives a cut AVI's duration from
 * the packets present, so the duration cannot show the cut. Reads 12 bytes per chunk through seeks; never the data. A size of
 * 0 or 0xFFFFFFFF (the placeholder a writer leaves when it cannot seek back, e.g. FFmpeg writing to a pipe) is not judged, and
 * the pad byte of an odd-sized chunk is not required when the file ends right after that chunk's data.
 */
final class RiffExtent {

  private static final int CHUNK_HEADER_BYTES = 12;
  private static final long UNKNOWN_SIZE = 0xFFFFFFFFL;

  private RiffExtent() {
  }

  /**
   * @param video the video as failure reasons name it
   * @return null when every top-level RIFF chunk fits in the file; otherwise what does not, e.g. "has a RIFF chunk at offset 0
   *     declaring 61472 bytes, but the file ends at 30740"
   * @throws UncheckedIOException over a {@link BlobReadException} when the bytes cannot be read (a storage failure)
   */
  static String truncation(SeekableBlobBytesSupplier bytes, Object video) {
    try (SeekableBlob blob = bytes.openSeekable()) {
      long length = blob.length();
      byte[] header = new byte[CHUNK_HEADER_BYTES];
      long position = 0;
      while (position + CHUNK_HEADER_BYTES <= length) {
        blob.seek(position);
        if (!readHeader(blob, header)) {
          // the blob reads shorter than its own length: the cut is the storage's, the file holds fewer bytes than it says
          return "holds fewer bytes than its length of " + length + " (no RIFF chunk header at offset " + position + ")";
        }
        if (!"RIFF".equals(new String(header, 0, 4, StandardCharsets.US_ASCII))) {
          // data after the last RIFF chunk is not a chunk this check can size
          return null;
        }
        long size = (header[4] & 0xFFL) | (header[5] & 0xFFL) << 8 | (header[6] & 0xFFL) << 16 | (header[7] & 0xFFL) << 24;
        if (size == 0 || size == UNKNOWN_SIZE) {
          return null;
        }
        long dataEnd = position + 8 + size;
        if (dataEnd > length) {
          return "has a RIFF chunk at offset " + position + " declaring " + size + " bytes, but the file ends at " + length;
        }
        position = dataEnd + (size & 1);
      }
      return null;
    } catch (IOException e) {
      throw new UncheckedIOException("video " + video + " RIFF chunk sizes could not be read: storage failure",
          e instanceof BlobReadException ? (BlobReadException) e : new BlobReadException(e));
    }
  }

  /** Reads a whole chunk header into {@code header}; false when the blob ends first. */
  private static boolean readHeader(SeekableBlob blob, byte[] header) throws IOException {
    int read = 0;
    while (read < CHUNK_HEADER_BYTES) {
      int n = blob.read(header, read, CHUNK_HEADER_BYTES - read);
      if (n < 0) {
        return false;
      }
      read += n;
    }
    return true;
  }
}
