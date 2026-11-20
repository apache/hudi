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

package org.apache.hudi.utilities.unstructured.embedding.image;

import org.apache.hudi.utilities.unstructured.SourceCauses;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobReadException;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobReferenceException;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobTruncatedException;
import org.apache.hudi.utilities.unstructured.embedding.source.SeekableBlob;
import org.apache.hudi.utilities.unstructured.embedding.source.SourceFailures;

import javax.imageio.stream.ImageInputStreamImpl;

import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * An {@link javax.imageio.stream.ImageInputStream} over a {@link SeekableBlob}, with no cache: a seek moves the underlying
 * stream, so a reader seeking back in a large OUT_OF_LINE file re-reads storage instead of holding the file on the heap (the
 * failure of {@code MemoryCacheImageInputStream}). A storage failure surfaces as a
 * {@link BlobReadException}, which stays in the cause chain of the reader's own exception; a {@link BlobTruncatedException} of
 * the blob read (the file is shorter than its recorded range, as the blob source's own range checks found) is not one: it passes
 * through as a property of the stored data, in data prep and in compute alike. A bare {@link EOFException} of the storage
 * client is always a storage failure. Closing it closes the blob.
 *
 * <p>A stream with a bound on the bytes it serves (data prep's header read) reads the blob's leading bytes, up to that bound and
 * at most {@link #PREFIX_MAX_BYTES}, in one request at its first read and serves every later read of them from memory: an image
 * reader probing, seeking back and re-reading its header sends one request instead of one per seek. Only bytes past that prefix
 * are read from the blob on demand. The bound counts the distinct bytes the reader is served: a byte served twice (a reader
 * seeking back over it) counts once. Re-reads are bounded too: such a stream serves at most {@link #REREAD_FACTOR} times the
 * bound in all; past {@link #MAX_DISTINCT_RANGES} separate ranges it counts bytes served, re-reads included, instead.
 */
public class SeekableBlobImageInputStream extends ImageInputStreamImpl {

  /** The most leading bytes a bounded stream holds in memory. */
  static final int PREFIX_MAX_BYTES = 1024 * 1024;
  /**
   * The longest read ImageInputStreamImpl's primitive reads make (readLong). Such a read needs all its bytes in one call, so one
   * the bound would cut fails with the bound's own exception; a longer (bulk) read is cut at the bound, as it always was.
   */
  static final int PRIMITIVE_READ_MAX_BYTES = 8;
  /** How many times over the bound a bounded stream serves in all, re-reads included (the JDK JPEG reader reads its header twice). */
  static final long REREAD_FACTOR = 4L;
  /**
   * The most separate ranges a bounded stream tracks to count distinct bytes. Past it the stream stops tracking (bounding the
   * tracking's memory) and counts bytes served instead, re-reads included, as it did before distinct bytes were counted: never a
   * stricter bound than that.
   */
  static final int MAX_DISTINCT_RANGES = 65_536;

  private final SeekableBlob blob;
  /** Reused by {@link #read()}: readers call it per byte while parsing headers. */
  private final byte[] single = new byte[1];
  /**
   * The bound: the most distinct bytes this stream serves while it tracks ranges, the most bytes served (re-reads included) once it
   * no longer does ({@link #MAX_DISTINCT_RANGES}); {@link Long#MAX_VALUE} for no bound.
   */
  private final long maxBytesRead;
  /** The ranges of the blob served so far, when the stream is bounded; null otherwise. */
  private final DistinctRanges served;
  /**
   * The most bytes a bounded stream serves in all, re-reads included ({@link #REREAD_FACTOR} x the bound): a reader that re-reads
   * the same bytes without end (a header whose offsets loop) still ends at the bound's exception.
   */
  private final long maxServedBytes;
  private long servedTotal;
  /** Whether the distinct ranges are still tracked; once too many, the bound counts bytes served ({@link #MAX_DISTINCT_RANGES}). */
  private boolean tracking = true;
  /** How many leading bytes the first read fetches in one request; 0 for none. */
  private final int prefixLimit;
  /** The blob's leading bytes, fetched at the first read; null before it. */
  private byte[] prefix;
  private int prefixLength;
  /** Where the blob's own position is, or -1 when unknown: a read past the prefix seeks the blob only when it is elsewhere. */
  private long blobPos;

  /**
   * A stream with no bound on the bytes it serves, passing a short blob's {@link BlobTruncatedException} through as the data's;
   * compute reads a stored input through {@link #forStoredInput}.
   */
  public SeekableBlobImageInputStream(SeekableBlob blob) {
    this(blob, Long.MAX_VALUE);
  }

  /**
   * The stream compute reads a STORED input through: a storage failure of a read or seek through this stream, a bare
   * {@link EOFException} of the storage client included (a premature EOF, a connection that ended early), surfaces as a
   * {@link BlobReadException}, so it is retried. A stored copy shorter than its recorded range ({@link BlobTruncatedException} of the
   * blob source's own range checks) passes through: it is the row's fault, the same on every read. An EOF the image
   * reader raises itself at the end of intact bytes (a body cut short) stays a property of the data.
   */
  public static SeekableBlobImageInputStream forStoredInput(SeekableBlob blob) {
    return new SeekableBlobImageInputStream(blob, Long.MAX_VALUE);
  }

  /** Opens the seekable blob of a stored input; the storage access {@link #openStoredInput} wraps. */
  @FunctionalInterface
  public interface StoredInputOpener {
    SeekableBlob open() throws IOException;
  }

  /**
   * Opens a STORED input through {@code stored} and reads it through {@link #forStoredInput}. No image byte has been decoded when
   * the open runs, so a failure it raises is the storage's or the stored input's, never the image's: a checked {@link IOException}
   * (an {@link EOFException} of a dropped connection) and a {@link RuntimeException} carrying an {@link IOException} in its cause
   * chain (a storage client reporting unchecked, e.g. HoodieIOException) surface as a {@link BlobReadException}, so the input is
   * retried. Three failures pass through unchanged, because they are not the storage's: a content failure of the stored input
   * ({@link SourceFailures#isContentFailure}: a malformed reference, {@link BlobReferenceException}; a stored file confirmed
   * missing; a stored object shorter than its recorded offset, {@link BlobTruncatedException}), which the caller classifies
   *, and a {@link RuntimeException} with no {@link IOException} under it (a programming error).
   */
  public static SeekableBlobImageInputStream openStoredInput(StoredInputOpener stored) throws IOException {
    SeekableBlob blob;
    try {
      blob = stored.open();
    } catch (IOException e) {
      throw SourceFailures.isContentFailure(e) ? e : new BlobReadException(e);
    } catch (RuntimeException e) {
      if (SourceFailures.isContentFailure(e)) {
        throw e;
      }
      if (carriesIoException(e)) {
        throw new BlobReadException(e);
      }
      throw e;
    }
    return forStoredInput(blob);
  }

  private static boolean carriesIoException(Throwable failure) {
    for (Throwable cause : SourceCauses.getCauses(failure)) {
      if (cause instanceof IOException) {
        return true;
      }
    }
    return false;
  }

  /**
   * A stream that fails with {@link ImageHeaderException} once more than {@code maxBytesRead} distinct bytes have been read (bytes
   * served, re-reads included, past {@link #MAX_DISTINCT_RANGES} separate ranges; at most {@link #REREAD_FACTOR} x the bound in all),
   * wherever they lie in the blob: data prep reads only a header and bounds what a hostile header can make the reader consume. While
   * ranges are tracked, a byte read again (a reader seeking back) is not counted again, so a header the reader parses twice does not fail a file smaller
   * than the bound. Its first read fetches the blob's leading bytes (at most {@code maxBytesRead} and
   * {@link #PREFIX_MAX_BYTES}) in one request.
   */
  public SeekableBlobImageInputStream(SeekableBlob blob, long maxBytesRead) {
    this.blob = blob;
    this.maxBytesRead = maxBytesRead;
    boolean bounded = maxBytesRead != Long.MAX_VALUE;
    this.served = bounded ? new DistinctRanges() : null;
    this.maxServedBytes = bounded && maxBytesRead <= Long.MAX_VALUE / REREAD_FACTOR ? REREAD_FACTOR * maxBytesRead : Long.MAX_VALUE;
    this.prefixLimit = bounded ? (int) Math.max(0L, Math.min(maxBytesRead, PREFIX_MAX_BYTES)) : 0;
    this.blobPos = -1L;
  }

  @Override
  public int read() throws IOException {
    int n = read(single, 0, 1);
    return n < 0 ? -1 : single[0] & 0xff;
  }

  @Override
  public int read(byte[] buffer, int offset, int len) throws IOException {
    checkClosed();
    Objects.checkFromIndexSize(offset, len, buffer.length);
    bitOffset = 0;
    if (len == 0) {
      return 0;
    }
    if (served != null) {
      len = boundedLength(len);
    }
    return fill(buffer, offset, len);
  }

  /**
   * For a bounded stream: fails once the bound is reached, returns {@code len} shortened to what the bound still allows, and
   * fetches the prefix.
   */
  private int boundedLength(int len) throws IOException {
    if (tracking && served.ranges() > MAX_DISTINCT_RANGES) {
      // too scattered to track: from here the bound counts bytes served, re-reads included, never stricter than that
      tracking = false;
      served.clear();
    }
    // the end of the blob, not the bound, ends a read past it: only bytes the blob has count against the bound
    long requestedEnd = Math.max(streamPos + 1, Math.min(streamPos + len, blob.length()));
    long allowedEnd;
    if (tracking) {
      allowedEnd = served.allowedEnd(streamPos, requestedEnd, maxBytesRead - served.total());
    } else {
      long room = Math.max(0L, maxBytesRead - servedTotal);
      allowedEnd = requestedEnd - streamPos <= room ? requestedEnd : streamPos + room;
    }
    // a primitive read (readShort, readInt, readLong) cut short is an EOFException of the reader, which would hide the bound
    if (allowedEnd <= streamPos || (allowedEnd < requestedEnd && len <= PRIMITIVE_READ_MAX_BYTES)
        || servedTotal >= maxServedBytes) {
      throw new ImageHeaderException("image header not found within " + maxBytesRead + " bytes read");
    }
    int bounded = len;
    if (allowedEnd < requestedEnd) {
      bounded = (int) (allowedEnd - streamPos);
    }
    fetchPrefix();
    return bounded;
  }

  /** Fills {@code len} bytes from the prefix and then the blob, counting them against the bound; -1 at once at the end. */
  private int fill(byte[] buffer, int offset, int len) throws IOException {
    // ImageInputStreamImpl's primitive reads expect their bytes in one call. Storage may return
    // a partial read (for example at an S3 response boundary), so fill the existing caller buffer.
    int total = 0;
    while (total < len) {
      long pos = streamPos;
      int n;
      if (pos < prefixLength) {
        n = (int) Math.min((long) len - total, prefixLength - pos);
        System.arraycopy(prefix, (int) pos, buffer, offset + total, n);
      } else {
        n = readBlob(pos, buffer, offset + total, len - total);
        if (n < 0) {
          break;
        }
      }
      if (served != null) {
        if (tracking) {
          served.add(pos, pos + n);
        }
        servedTotal += n;
      }
      streamPos += n;
      total += n;
    }
    return total == 0 ? -1 : total;
  }

  /**
   * Reads the blob at {@code pos}. A bounded stream seeks the blob there first unless it already is (its seeks are lazy); an
   * unbounded one reads where its last seek left the blob, as it always has.
   */
  private int readBlob(long pos, byte[] buffer, int offset, int len) throws IOException {
    int n;
    try {
      if (served != null) {
        if (blobPos != pos) {
          blobPos = -1L;
          blob.seek(Math.min(pos, blob.length()));
          blobPos = Math.min(pos, blob.length());
        }
        if (pos >= blob.length()) {
          return -1;
        }
      }
      n = blob.read(buffer, offset, len);
    } catch (IOException e) {
      blobPos = -1L;
      throw readFailure(e);
    }
    if (n > 0 && blobPos >= 0) {
      blobPos += n;
    }
    return n;
  }

  /**
   * Fetches the blob's leading bytes once, in one request. A failure or a short blob ends the prefix where it stopped, quietly: no
   * reader has asked for those bytes yet, and one that does reads them from the blob as it would have without a prefix, meeting
   * the same failure. An interruption is not swallowed.
   */
  private void fetchPrefix() throws IOException {
    if (prefix != null || prefixLimit == 0) {
      return;
    }
    prefix = new byte[(int) Math.min(prefixLimit, blob.length())];
    int filled = 0;
    try {
      if (blobPos != 0L) {
        blobPos = -1L;
        blob.seek(0L);
        blobPos = 0L;
      }
      while (filled < prefix.length) {
        int n = blob.read(prefix, filled, prefix.length - filled);
        if (n < 0) {
          break;
        }
        filled += n;
        blobPos += n;
      }
    } catch (IOException | RuntimeException e) {
      blobPos = -1L;
      SourceFailures.rethrowJvmFailure(e);
      if (e instanceof InterruptedIOException || Thread.currentThread().isInterrupted()) {
        throw e;
      }
    }
    prefixLength = filled;
  }

  @Override
  public void seek(long pos) throws IOException {
    checkClosed();
    if (pos < flushedPos) {
      throw new IndexOutOfBoundsException("pos < flushedPos");
    }
    bitOffset = 0;
    if (served == null) {
      // unbounded (compute): the blob follows every seek, as a reader of a whole stored image expects
      try {
        blob.seek(Math.min(pos, blob.length()));
      } catch (IOException e) {
        blobPos = -1L;
        throw readFailure(e);
      }
      blobPos = Math.min(pos, blob.length());
    }
    // bounded: the blob is sought at the next read that needs it, so a seek inside the prefix sends nothing
    streamPos = pos;
  }

  /**
   * A failure of the blob read: a {@link BlobReadException}, except a {@link BlobTruncatedException} (the stored object is shorter
   * than its recorded range: a property of the data, the same on every attempt), which passes through.
   */
  private IOException readFailure(IOException e) {
    return e instanceof BlobTruncatedException ? e : new BlobReadException(e);
  }

  @Override
  public long length() {
    return blob.length();
  }

  @Override
  public void close() throws IOException {
    super.close();
    prefix = null;
    blob.close();
  }

  /** Disjoint, non-adjacent byte ranges [start, end) of the blob, and how many bytes they cover. */
  static final class DistinctRanges {
    private final TreeMap<Long, Long> ranges = new TreeMap<>();
    private long total;

    long total() {
      return total;
    }

    /** How many separate ranges are held. */
    int ranges() {
      return ranges.size();
    }

    /** Forgets every range. */
    void clear() {
      ranges.clear();
      total = 0;
    }

    /**
     * The end of the longest leading part of [start, end) that holds at most {@code budget} bytes no range covers yet: a read of
     * it adds at most {@code budget} to {@link #total}.
     */
    long allowedEnd(long start, long end, long budget) {
      long pos = start;
      Map.Entry<Long, Long> covering = ranges.floorEntry(pos);
      if (covering != null && covering.getValue() > pos) {
        pos = Math.min(end, covering.getValue());
      }
      long left = budget;
      while (pos < end) {
        Map.Entry<Long, Long> next = ranges.higherEntry(pos);
        long gapEnd = next == null ? end : Math.min(end, next.getKey());
        long gap = gapEnd - pos;
        if (gap > left) {
          return pos + left;
        }
        left -= gap;
        pos = gapEnd;
        if (next != null && pos < end) {
          pos = Math.min(end, next.getValue());
        }
      }
      return end;
    }

    /** Adds [start, end), merging it with every range it overlaps or touches. */
    void add(long start, long end) {
      if (end <= start) {
        return;
      }
      long mergedStart = start;
      long mergedEnd = end;
      Map.Entry<Long, Long> before = ranges.floorEntry(start);
      if (before != null && before.getValue() >= start) {
        mergedStart = before.getKey();
      }
      long covered = 0;
      for (Map.Entry<Long, Long> range = ranges.ceilingEntry(mergedStart); range != null && range.getKey() <= mergedEnd;
           range = ranges.ceilingEntry(mergedStart)) {
        mergedEnd = Math.max(mergedEnd, range.getValue());
        covered += range.getValue() - range.getKey();
        ranges.remove(range.getKey());
      }
      ranges.put(mergedStart, mergedEnd);
      total += (mergedEnd - mergedStart) - covered;
    }
  }
}
