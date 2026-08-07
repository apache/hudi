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

package org.apache.hudi.common.blob;

/**
 * Describes a single byte-range read for one out-of-line (OOL) BLOB field.
 *
 * <p>{@code tag} is an opaque correlation key supplied by the caller. {@link BlobRangeReader}
 * groups, sorts, and merges requests before issuing I/O, so result order no longer matches
 * submission order and a caller-side list index cannot be used to match bytes back to rows.
 * The tag travels with each request through merge/read and is returned as the key in the
 * result map from {@link BlobRangeReader#readBatched}.
 *
 * <p>Spark solves the same problem differently: {@code BatchedBlobReader} attaches a
 * {@code RowInfo} (original row + file path + offset + length + index) to every read request,
 * and that object is carried inside {@code MergedRange} through merge/read. Our {@code tag}
 * is the engine-agnostic equivalent for callers that do not use Spark rows (e.g. Flink
 * {@code RowFieldKey = (rowIndex, blobFieldPosition)}).
 *
 * <p>Use {@link #range(String, long, long, Object)} for normal offset+length refs and
 * {@link #wholeFile(String, Object)} when no offset/length is available (the entire file
 * will be read).
 *
 * @param <T> caller-supplied correlation tag type
 */
public final class BlobReadRequest<T> {

  /** Path to the external file. */
  public final String filePath;

  /**
   * Byte offset within the file (0 for whole-file reads).
   */
  public final long offset;

  /**
   * Number of bytes to read. {@code -1} signals a whole-file read
   * (no specific offset/length was stored in the reference).
   */
  public final long length;

  /** Opaque caller tag for correlating this request with its result. */
  public final T tag;

  private BlobReadRequest(String filePath, long offset, long length, T tag) {
    this.filePath = filePath;
    this.offset = offset;
    this.length = length;
    this.tag = tag;
  }

  /**
   * Creates a request to read a specific byte range from {@code filePath}.
   *
   * @param filePath absolute path to the external file
   * @param offset   byte offset of the range start (≥ 0)
   * @param length   number of bytes to read (≥ 0)
   * @param tag      caller correlation key
   */
  public static <T> BlobReadRequest<T> range(String filePath, long offset, long length, T tag) {
    if (offset < 0) {
      throw new IllegalArgumentException("offset must be non-negative, got: " + offset);
    }
    if (length < 0) {
      throw new IllegalArgumentException("length must be non-negative, got: " + length);
    }
    return new BlobReadRequest<>(filePath, offset, length, tag);
  }

  /**
   * Creates a request to read the entire contents of {@code filePath}.
   *
   * @param filePath absolute path to the external file
   * @param tag      caller correlation key
   */
  public static <T> BlobReadRequest<T> wholeFile(String filePath, T tag) {
    return new BlobReadRequest<>(filePath, 0L, -1L, tag);
  }

  /** Returns {@code true} if this request should read the whole file. */
  public boolean isWholeFile() {
    return length < 0;
  }
}
