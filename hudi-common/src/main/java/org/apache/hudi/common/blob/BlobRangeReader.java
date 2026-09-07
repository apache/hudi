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

import org.apache.hudi.io.SeekableDataInputStream;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Engine-agnostic utility for batched byte-range reads of out-of-line (OOL) BLOB references.
 *
 * <h3>Algorithm</h3>
 * <ol>
 *   <li>Separate whole-file requests from range requests.
 *   <li>Group range requests by {@code filePath}, sort each group by {@code offset}, and merge
 *       adjacent or nearby ranges whose gap is ≤ {@code maxGapBytes} into a single
 *       {@link MergedRange}.
 *   <li>For each {@link MergedRange}: seek to {@code startOffset} and {@code readFully} the
 *       entire merged span in one I/O call; slice the buffer to extract each individual request's
 *       bytes.
 *   <li>For whole-file requests: open the file and read all bytes.
 * </ol>
 *
 * <p>The caller provides opaque {@code tag} values on each {@link BlobReadRequest}; those tags are
 * used as keys in the returned map so results can be correlated back to the caller's domain
 * (e.g. row index + field position in Flink). Spark's {@code BatchedBlobReader} solves the same
 * problem by carrying a {@code RowInfo} through {@code MergedRange} instead of a separate tag.
 *
 * <h3>Overlap detection</h3>
 * If two requests for the same file have overlapping byte ranges an
 * {@link IllegalArgumentException} is thrown (same check exists in Spark's
 * {@code BatchedBlobReader.mergeRanges}). Hudi's OOL blob writer assigns disjoint
 * {@code (offset, length)} spans when appending to an external file, so well-formed table data
 * should never produce overlaps; this guard catches corruption, manual edits, or buggy callers.
 * Adjacent or gapped ranges within {@code maxGapBytes} are merged; truly overlapping ranges are not.
 */
public final class BlobRangeReader {

  private static final Logger LOG = LoggerFactory.getLogger(BlobRangeReader.class);

  /**
   * Read chunk size for whole-file streaming. Spark's {@code BatchedBlobReader.readWholeFile}
   * uses {@code InputStream.readAllBytes()} (Java 9+); hudi-common targets Java 8, so we stream
   * into a {@link ByteArrayOutputStream} with a standard 8 KiB buffer instead.
   */
  private static final int WHOLE_FILE_READ_BUFFER_SIZE = 8192;

  private BlobRangeReader() {
  }

  // -------------------------------------------------------------------------
  //  Public API
  // -------------------------------------------------------------------------

  /**
   * Issues batched DFS reads for all {@code requests} and returns a map from each request's
   * {@code tag} to the bytes that were read.
   *
   * @param requests    OOL blob read requests (may contain a mix of range and whole-file refs)
   * @param storage     HoodieStorage instance for file I/O
   * @param maxGapBytes maximum byte gap between two range requests in the same file that are
   *                    still coalesced into a single read (0 = only truly adjacent ranges are
   *                    merged)
   * @param <T>         caller-supplied correlation tag type
   * @return map from {@link BlobReadRequest#tag} to the bytes read for that request
   */
  public static <T> Map<T, byte[]> readBatched(
      List<BlobReadRequest<T>> requests,
      HoodieStorage storage,
      int maxGapBytes) {
    if (requests == null || requests.isEmpty()) {
      return Collections.emptyMap();
    }

    List<BlobReadRequest<T>> wholeFileReqs = new ArrayList<>();
    List<BlobReadRequest<T>> rangeReqs = new ArrayList<>();
    for (BlobReadRequest<T> req : requests) {
      (req.isWholeFile() ? wholeFileReqs : rangeReqs).add(req);
    }

    return Stream.concat(
            readWholeFiles(wholeFileReqs, storage).entrySet().stream(),
            readRanges(rangeReqs, storage, maxGapBytes).entrySet().stream())
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  // -------------------------------------------------------------------------
  //  Package-private: visible for testing
  // -------------------------------------------------------------------------

  /**
   * Groups {@code requests} by file path, sorts each group by offset, and merges nearby ranges.
   *
   * <p>Cross-file processing order does not affect read correctness — each file is opened and
   * read independently. Only within-file offset order matters for range merging. We use a
   * {@link LinkedHashMap} so iteration follows first-seen file path in the request list, giving
   * stable I/O ordering for tests and debug logs. Sorting file paths (e.g. via {@code TreeMap})
   * would also be stable but is unnecessary for correctness.
   */
  static <T> List<MergedRange<T>> groupSortMerge(
      List<BlobReadRequest<T>> requests,
      int maxGapBytes) {
    Map<String, List<BlobReadRequest<T>>> byFile = new LinkedHashMap<>();
    for (BlobReadRequest<T> req : requests) {
      byFile.computeIfAbsent(req.filePath, k -> new ArrayList<>()).add(req);
    }

    List<MergedRange<T>> merged = new ArrayList<>();
    for (Map.Entry<String, List<BlobReadRequest<T>>> entry : byFile.entrySet()) {
      List<BlobReadRequest<T>> fileReqs = entry.getValue();
      fileReqs.sort((a, b) -> Long.compare(a.offset, b.offset));
      merged.addAll(mergeWithinFile(fileReqs, maxGapBytes));
    }
    return merged;
  }

  // -------------------------------------------------------------------------
  //  Private helpers
  // -------------------------------------------------------------------------

  private static <T> Map<T, byte[]> readWholeFiles(
      List<BlobReadRequest<T>> reqs,
      HoodieStorage storage) {
    Map<T, byte[]> result = new HashMap<>();
    for (BlobReadRequest<T> req : reqs) {
      try (InputStream in = storage.open(new StoragePath(req.filePath))) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] buf = new byte[WHOLE_FILE_READ_BUFFER_SIZE];
        int n;
        while ((n = in.read(buf)) != -1) {
          baos.write(buf, 0, n);
        }
        result.put(req.tag, baos.toByteArray());
        LOG.debug("Read whole file {} for tag {}", req.filePath, req.tag);
      } catch (IOException e) {
        throw new RuntimeException("Failed to read whole-file blob: " + req.filePath, e);
      }
    }
    return result;
  }

  private static <T> Map<T, byte[]> readRanges(
      List<BlobReadRequest<T>> rangeReqs,
      HoodieStorage storage,
      int maxGapBytes) {
    if (rangeReqs.isEmpty()) {
      return Collections.emptyMap();
    }
    Map<T, byte[]> result = new HashMap<>();
    for (MergedRange<T> range : groupSortMerge(rangeReqs, maxGapBytes)) {
      try (SeekableDataInputStream in =
          storage.openSeekable(new StoragePath(range.filePath), false)) {
        in.seek(range.startOffset);
        int totalLength = (int) (range.endOffset - range.startOffset);
        byte[] buffer = new byte[totalLength];
        in.readFully(buffer, 0, totalLength);
        LOG.debug("Read {} bytes from {} at offset {} for {} request(s)",
            totalLength, range.filePath, range.startOffset, range.requests.size());
        for (BlobReadRequest<T> req : range.requests) {
          int relOff = (int) (req.offset - range.startOffset);
          result.put(req.tag, Arrays.copyOfRange(buffer, relOff, relOff + (int) req.length));
        }
      } catch (IOException e) {
        throw new RuntimeException(
            "Failed to read batched blob ranges from: " + range.filePath, e);
      }
    }
    return result;
  }

  /**
   * Merges a sorted (by offset) list of range requests from the same file into
   * {@link MergedRange}s. Requests whose gap is ≤ {@code maxGapBytes} are combined into a single
   * range. Truly overlapping ranges (gap &lt; 0) are rejected.
   *
   * @throws IllegalArgumentException if two requests have overlapping byte ranges
   */
  private static <T> List<MergedRange<T>> mergeWithinFile(
      List<BlobReadRequest<T>> sortedReqs,
      int maxGapBytes) {
    List<MergedRange<T>> merged = new ArrayList<>();
    MergedRange<T> current = null;
    for (BlobReadRequest<T> req : sortedReqs) {
      if (current == null) {
        current = new MergedRange<>(req.filePath, req.offset, req.offset + req.length);
        current.requests.add(req);
      } else {
        long gap = req.offset - current.endOffset;
        if (gap < 0) {
          throw new IllegalArgumentException(
              String.format(
                  "Overlapping OOL blob ranges in %s: existing [%d, %d), new [%d, %d)",
                  req.filePath, current.startOffset, current.endOffset,
                  req.offset, req.offset + req.length));
        }
        if (gap <= maxGapBytes) {
          current.endOffset = Math.max(current.endOffset, req.offset + req.length);
          current.requests.add(req);
        } else {
          merged.add(current);
          current = new MergedRange<>(req.filePath, req.offset, req.offset + req.length);
          current.requests.add(req);
        }
      }
    }
    if (current != null) {
      merged.add(current);
    }
    return merged;
  }

  /**
   * A merged byte range combining one or more {@link BlobReadRequest}s from the same file
   * that were close enough to be issued as a single seek+readFully operation.
   *
   * @param <T> caller-supplied correlation tag type
   */
  static final class MergedRange<T> {

    final String filePath;
    final long startOffset;
    long endOffset;
    final List<BlobReadRequest<T>> requests = new ArrayList<>();

    MergedRange(String filePath, long startOffset, long endOffset) {
      this.filePath = filePath;
      this.startOffset = startOffset;
      this.endOffset = endOffset;
    }
  }
}
