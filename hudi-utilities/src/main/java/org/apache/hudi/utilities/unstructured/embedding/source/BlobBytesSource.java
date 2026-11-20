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

package org.apache.hudi.utilities.unstructured.embedding.source;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.HoodieStorageUtils;
import org.apache.hudi.exception.ExceptionUtil;
import org.apache.hudi.io.SeekableDataInputStream;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StorageSchemes;
import org.apache.hudi.utilities.unstructured.retry.TransientRetry;
import org.apache.hudi.utilities.unstructured.storage.StorageReferenceRules;

import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hudi.blob.BatchedBlobReader;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URI;
import java.nio.file.AccessDeniedException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import scala.Option;

/**
 * Opens the bytes of one source row's {@code blob} value as a stream, never materializing an
 * out-of-line file in memory.
 * <ul>
 *   <li>{@code INLINE}: a stream over {@code blob.data};</li>
 *   <li>{@code OUT_OF_LINE}: the object at {@code reference.external_path} when it is absolute (has a URI scheme);
 *       restricted to {@code [offset, offset + length)} when those are set. A relative path is resolved only by a source built
 *       with a table base path (compute reading the embedding table's {@code prepared_image}, which the input store writes into
 *       the table's own {@code .hoodie/.blob} folder): every scheme-less reference resolves against the base, regardless of {@code managed}, as {@code read_blob()} does
 *       ({@link BatchedBlobReader#resolveReferencePath}) and must stay inside it;
 *       writers require managed=true for scheme-less paths and managed=false for URIs. A source without a base path (prep
 *       reading source objects where the source announced them) refuses every relative path.</li>
 * </ul>
 * An absolute path is opened on the storage for its own {@code scheme://authority} (one storage per scheme+authority, cached); a
 * relative one on the base-path storage.
 *
 * <p>Opening an OUT_OF_LINE blob (resolving its storage, opening and seeking to its range) is retried in-commit under the
 * source's {@link TransientRetry} when it fails transiently ({@link #isTransient}): any storage failure except the file's own
 * ({@link SourceFailures#isContentFailure}: gone, short, a malformed reference) and a platform failure (an access denial such
 * as a 403 or KMS denial, {@link AccessDeniedException}); {@link #openSeekable} opens the same bytes for random access under
 * the same rules. Reads of an opened stream are not retried here: the caller's decoder holds state.
 */
public class BlobBytesSource implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final int READ_BUFFER_BYTES = 64 * 1024;
  private static final String BLOB_FILE = "Blob file ";
  /**
   * The read-ahead of data prep's source reads ({@link #withReadahead}): a reader that seeks (an image header reader, FFmpeg) gets
   * this many bytes per storage request instead of the file system's default (S3A: 64 KiB), so it sends far fewer requests.
   */
  public static final long PREP_SOURCE_READAHEAD_BYTES = 1024L * 1024;
  // null: no table to resolve a relative path against, so every relative path is refused
  private final String tableBasePath;
  private final StorageConfiguration<?> storageConf;
  private final TransientRetry openRetry;
  // 0: the file system's own read-ahead
  private final long readaheadBytes;
  private transient HoodieStorage baseStorage;
  /** The last whole-object status checked on this source, consumed by its next open. */
  private transient SourceObjectVersion checkedVersion;
  private transient Map<String, HoodieStorage> storageBySchemeAuthority;

  /** A source of absolute paths only, that tries each open once ({@link TransientRetry#NONE}). */
  public BlobBytesSource(StorageConfiguration<?> storageConf) {
    this(storageConf, TransientRetry.NONE);
  }

  /**
   * A source of absolute paths only: a relative {@code external_path} is refused.
   *
   * @param openRetry the in-commit retry of a transient failure to open an OUT_OF_LINE blob
   */
  public BlobBytesSource(StorageConfiguration<?> storageConf, TransientRetry openRetry) {
    this(null, storageConf, openRetry);
  }

  /**
   * A source that resolves a scheme-less path against {@code tableBasePath} and tries each open once
   * ({@link TransientRetry#NONE}).
   */
  public BlobBytesSource(String tableBasePath, StorageConfiguration<?> storageConf) {
    this(tableBasePath, storageConf, TransientRetry.NONE);
  }

  /**
   * @param tableBasePath the base path of the table whose scheme-less references are relative to it, or null for a source of
   *     absolute paths only
   * @param openRetry the in-commit retry of a transient failure to open an OUT_OF_LINE blob
   */
  public BlobBytesSource(String tableBasePath, StorageConfiguration<?> storageConf, TransientRetry openRetry) {
    this(tableBasePath, storageConf, openRetry, 0L);
  }

  private BlobBytesSource(String tableBasePath, StorageConfiguration<?> storageConf, TransientRetry openRetry, long readaheadBytes) {
    this.tableBasePath = tableBasePath;
    this.storageConf = storageConf;
    this.openRetry = openRetry;
    this.readaheadBytes = readaheadBytes;
  }

  /**
   * This source, trying each open once ({@link TransientRetry#NONE}): data prep's read ahead of the next file, whose failure the
   * task's own open, with this source's retry, meets again.
   */
  public BlobBytesSource withoutOpenRetry() {
    return new BlobBytesSource(tableBasePath, storageConf, TransientRetry.NONE, readaheadBytes);
  }

  /**
   * This source, with every OUT_OF_LINE stream it opens set to read {@code bytes} ahead where the stream takes a read-ahead (S3A
   * does; a local file does not). It changes the requests a seeking reader sends, never the bytes it reads.
   *
   * @param bytes the read-ahead in bytes; 0 keeps the file system's own
   */
  public BlobBytesSource withReadahead(long bytes) {
    if (bytes < 0) {
      throw new IllegalArgumentException("Read-ahead must be >= 0 but is " + bytes);
    }
    return new BlobBytesSource(tableBasePath, storageConf, openRetry, bytes);
  }

  /**
   * Whether {@code failure} of an open is transient: a storage failure ({@link IOException} in its cause chain) that is neither the
   * file's own ({@link SourceFailures#isContentFailure}) nor an access denial ({@link SourceFailures#isAccessDenial}:
   * {@link AccessDeniedException}, a 403 or a KMS denial; Hadoop's {@link AccessControlException}, an HDFS permission check; the
   * GCS connector's 403 response, except one that names a rate-limit reason) nor a served scheme's missing file system
   * ({@link UnsupportedFileSystemException}, or a {@link ClassNotFoundException} of its configured implementation: a connector
   * jar absent) — platform failures, which retrying in-commit cannot help.
   */
  public static boolean isTransient(Throwable failure) {
    if (SourceFailures.isContentFailure(failure) || SourceFailures.jvmFailure(failure) != null) {
      // a JVM failure in the graph is the round's failure: a later successful try must not erase it
      return false;
    }
    boolean storageFailure = false;
    for (Throwable cause : ExceptionUtil.getCauses(failure)) {
      if (SourceFailures.isAccessDenial(cause) || cause instanceof UnsupportedFileSystemException
          || cause instanceof ClassNotFoundException || cause instanceof NoClassDefFoundError) {
        // a 403 / KMS denial (S3A or the GCS connector), an HDFS permission check or a missing file system: a platform failure,
        // never retried in the round
        return false;
      }
      storageFailure |= cause instanceof IOException;
    }
    return storageFailure;
  }

  /**
   * Opens the bytes described by the {@code blob} struct of a source row. The caller closes the stream.
   *
   * @param blob the {@code blob} column value (a struct with fields type, data, reference)
   * @throws BlobReferenceException for a malformed row or reference: a null blob, INLINE without data, an unknown type,
   *     OUT_OF_LINE without a reference or with an empty or unparseable path, a relative one this source does not resolve
   *     (no table base path or escaping the base), a range
   *     {@code read_blob()} refuses (offset and length not both set or both null, or either negative; checked before any
   *     storage access), or a reference whose storage cannot be built for its own fault (an unserved scheme, an invalid or
   *     missing required authority, an out-of-range port)
   * @throws IOException also: a {@link BlobTruncatedException} for an offset beyond the object's end, a
   *     {@link java.io.FileNotFoundException} for a file a re-check confirms absent, an {@link AccessDeniedException} for a file
   *     that is present but cannot be opened, and a plain IOException for any other storage failure (after the open retries);
   *     {@link SourceFailures#isContentFailure} tells the file's own failures from the others. A read of the returned stream
   *     fails with a {@link BlobTruncatedException} when the object ends inside the recorded range.
   */
  public InputStream open(Row blob) throws IOException {
    return open(parse(blob), null);
  }

  /**
   * {@link #open(Row)} of a row whose object's length the caller already knows (data prep: the source listing's size). An
   * OUT_OF_LINE reference without a range is the whole object, so the open needs no status request for its length: on S3A
   * the object is opened without the HEAD S3A sends otherwise. A reference with a range, and an INLINE blob, open exactly as
   * {@link #open(Row)} opens them.
   *
   * @param objectLength the length of the object an unranged reference names, as listed
   */
  public InputStream open(Row blob, long objectLength) throws IOException {
    return open(parse(blob), objectLength);
  }

  /**
   * Verifies the listed version before prep consumes a whole out-of-line source object, including read-ahead bytes.
   * Inline bytes and explicit stored ranges already identify their bytes. A zero modification time is unknown; size is always
   * compared. The next open retains the checked status for a pinned read when available and verifies the status after reading.
   * Changed objects fail their file, while a status request's storage failure keeps the source-open retry policy.
   */
  public synchronized void checkVersion(Row blob, long modificationTime, long length) throws IOException {
    checkedVersion = null;
    ParsedBlob parsed = parse(blob);
    if (parsed.data != null || parsed.offset != null) {
      return;
    }
    checkedVersion = openRetry.call("check blob source version " + parsed.path, () -> {
      HoodieStorage storage = storageFor(parsed.path, parsed.absolute);
      try {
        SourceObjectVersion version = SourceObjectVersion.read(storage, parsed.path);
        version.checkListed(modificationTime, length);
        return version;
      } catch (IOException | RuntimeException failure) {
        throw notFoundChecked(storage, parsed.path, failure);
      }
    }, BlobBytesSource::isTransient);
  }

  /** Retains the connector identity observed by checkVersion across a read-ahead cache handoff. */
  @FunctionalInterface
  public interface VersionCheck {
    void check(BlobBytesSource consumer) throws IOException;
  }

  public synchronized VersionCheck checkedVersionCheck(Row blob) throws IOException {
    SourceObjectVersion version = checkedVersion;
    if (version == null) {
      return consumer -> { };
    }
    ParsedBlob parsed = parse(blob);
    return consumer -> consumer.recheckVersion(version, parsed);
  }

  private void recheckVersion(SourceObjectVersion version, ParsedBlob parsed) throws IOException {
    openRetry.call("check cached blob source version " + parsed.path, () -> {
      HoodieStorage storage = storageFor(parsed.path, parsed.absolute);
      try {
        version.recheck(storage);
        return true;
      } catch (IOException | RuntimeException failure) {
        throw notFoundChecked(storage, parsed.path, failure);
      }
    }, BlobBytesSource::isTransient);
  }

  private synchronized SourceObjectVersion takeCheckedVersion(StoragePath path) {
    SourceObjectVersion version = checkedVersion;
    checkedVersion = null;
    return version != null && version.names(path) ? version : null;
  }

  private InputStream open(ParsedBlob parsed, Long objectLength) throws IOException {
    if (parsed.data != null) {
      return new ByteArrayInputStream(parsed.data);
    }
    SourceObjectVersion version = takeCheckedVersion(parsed.path);
    return openRetry.call("open blob " + parsed.path, () -> openRange(parsed, objectLength, version), BlobBytesSource::isTransient);
  }

  private InputStream openRange(ParsedBlob blob, Long objectLength, SourceObjectVersion version) throws IOException {
    HoodieStorage pathStorage = storageFor(blob.path, blob.absolute);
    SeekableDataInputStream in = openChecked(pathStorage, blob.path, knownObjectLength(blob, objectLength), version);
    try {
      if (blob.offset != null && blob.offset > 0) {
        seekChecked(in, pathStorage, blob.path, blob.offset);
      }
      InputStream checked = new NotFoundCheckedInputStream(in, pathStorage, blob.path);
      return blob.length == null ? checked : new RangeInputStream(checked, blob.length, blob.path);
    } catch (IOException | RuntimeException e) {
      // the caller never receives the stream, so it must not leak (e.g. an offset beyond EOF)
      try {
        in.close();
      } catch (IOException closeFailure) {
        e.addSuppressed(closeFailure);
      }
      throw e;
    }
  }

  /**
   * Opens the bytes described by the {@code blob} struct for random access: an INLINE blob over its {@code data} (already in
   * the row), an OUT_OF_LINE one over a seekable storage stream restricted to the referenced range (the whole object when the
   * reference has no range), so a reader that seeks (an image header reader) never needs the file in memory. The row is
   * validated as {@link #open} validates it, and the open is retried as {@link #open} retries it ({@link #isTransient}). A read
   * or seek of the returned blob is not retried; it re-checks a FileNotFoundException as {@link #open}'s stream does, and a
   * seek or read past the object's real end inside the recorded range is a {@link BlobTruncatedException}. The caller closes the
   * result.
   *
   * @param blob the {@code blob} column value
   * @throws BlobReferenceException for a row that names no readable range, exactly as {@link #open} refuses it
   */
  public SeekableBlob openSeekable(Row blob) throws IOException {
    return openSeekable(parse(blob), null);
  }

  /**
   * {@link #openSeekable(Row)} of a row whose object's length the caller already knows (data prep: the source listing's size).
   * An OUT_OF_LINE reference without a range is the whole object, of that length: no status request is sent for it, and on
   * S3A the object is opened without the HEAD S3A sends otherwise. A reference with a range, and an INLINE blob, open exactly
   * as {@link #openSeekable(Row)} opens them.
   *
   * @param objectLength the length of the object an unranged reference names, as listed
   */
  public SeekableBlob openSeekable(Row blob, long objectLength) throws IOException {
    return openSeekable(parse(blob), objectLength);
  }

  /**
   * A random-access view of an object's bytes the caller already read in full (data prep reads the next file ahead), for an
   * OUT_OF_LINE row without a range: it reads and seeks as {@link #openSeekable(Row, long)} of an object of {@code
   * objectBytes.length} bytes does, with no storage access. The row is validated as {@link #openSeekable(Row)} validates it.
   *
   * @throws BlobReferenceException for a row that names no readable range, exactly as {@link #openSeekable(Row)} refuses it
   * @throws IllegalArgumentException for a row that is INLINE or names a range: its bytes are not one whole object
   */
  public SeekableBlob openSeekable(Row blob, byte[] objectBytes) throws IOException {
    ParsedBlob parsed = parse(blob);
    if (parsed.data != null || parsed.offset != null) {
      throw new IllegalArgumentException("Only a whole OUT_OF_LINE object is read ahead, not " + (parsed.data != null ? "an INLINE blob"
          : "the range of " + parsed.path));
    }
    return new ByteArrayBlob(objectBytes, "the " + objectBytes.length + "-byte blob range of " + parsed.path);
  }

  private SeekableBlob openSeekable(ParsedBlob parsed, Long objectLength) throws IOException {
    if (parsed.data != null) {
      return new ByteArrayBlob(parsed.data);
    }
    SourceObjectVersion version = takeCheckedVersion(parsed.path);
    return openRetry.call("open blob " + parsed.path, () -> openSeekableRange(parsed, objectLength, version), BlobBytesSource::isTransient);
  }

  /** The listed length of the object an unranged reference names; null for a ranged one, whose object length is not known. */
  private static Long knownObjectLength(ParsedBlob blob, Long objectLength) {
    return blob.offset == null && blob.length == null ? objectLength : null;
  }

  private SeekableBlob openSeekableRange(ParsedBlob blob, Long objectLength, SourceObjectVersion version) throws IOException {
    HoodieStorage pathStorage = storageFor(blob.path, blob.absolute);
    long base = blob.offset == null ? 0L : blob.offset;
    Long knownLength = knownObjectLength(blob, objectLength);
    long size;
    try {
      if (blob.length != null) {
        size = blob.length;
      } else {
        size = knownLength != null ? knownLength : pathStorage.getPathInfo(blob.path).getLength();
      }
    } catch (IOException | RuntimeException e) {
      throw notFoundChecked(pathStorage, blob.path, e);
    }
    SeekableDataInputStream in = openChecked(pathStorage, blob.path, knownLength, version);
    try {
      if (base > 0) {
        seekChecked(in, pathStorage, blob.path, base);
      }
    } catch (IOException | RuntimeException e) {
      // the caller never receives the blob, so its stream must not leak
      try {
        in.close();
      } catch (IOException closeFailure) {
        e.addSuppressed(closeFailure);
      }
      throw e;
    }
    return new StorageRangeBlob(in, pathStorage, base, size, blob.path);
  }

  /**
   * The one parse of a {@code blob} struct both readers open from: INLINE data, or an OUT_OF_LINE reference whose path
   * resolves and whose range is one {@code read_blob()} reads (the batched blob reader: offset and length both set or both null,
   * neither negative). Anything else is a {@link BlobReferenceException}, before any storage access.
   */
  private ParsedBlob parse(Row blob) throws IOException {
    if (blob == null) {
      throw new BlobReferenceException("blob column is null");
    }
    String type = blob.getAs(HoodieSchema.Blob.TYPE);
    if (HoodieSchema.Blob.INLINE.equals(type)) {
      byte[] data = blob.getAs(HoodieSchema.Blob.INLINE_DATA_FIELD);
      if (data == null) {
        throw new BlobReferenceException("INLINE blob has null data");
      }
      return new ParsedBlob(data, null, false, null, null);
    }
    if (!HoodieSchema.Blob.OUT_OF_LINE.equals(type)) {
      throw new BlobReferenceException("Unsupported blob storage type: " + type);
    }
    Row reference = blob.getAs(HoodieSchema.Blob.EXTERNAL_REFERENCE);
    if (reference == null) {
      throw new BlobReferenceException("OUT_OF_LINE blob has null reference");
    }
    String externalPath = reference.getAs(HoodieSchema.Blob.EXTERNAL_REFERENCE_PATH);
    StoragePath path = resolve(externalPath);
    Long offset = reference.getAs(HoodieSchema.Blob.EXTERNAL_REFERENCE_OFFSET);
    Long length = reference.getAs(HoodieSchema.Blob.EXTERNAL_REFERENCE_LENGTH);
    if ((offset == null) != (length == null)) {
      throw new BlobReferenceException("OUT_OF_LINE blob " + path + " must set both offset and length, or neither: offset "
          + offset + ", length " + length);
    }
    if (offset != null && (offset < 0 || length < 0)) {
      throw new BlobReferenceException("OUT_OF_LINE blob " + path + " has an invalid range: offset " + offset + ", length " + length);
    }
    return new ParsedBlob(null, path, isAbsolute(externalPath), offset, length);
  }

  /**
   * A parsed {@code blob} struct: {@code data} for INLINE, else the resolved OUT_OF_LINE path, whether its {@code external_path}
   * was absolute (a relative one is opened on the base-path storage), and its checked range.
   */
  private static final class ParsedBlob {
    private final byte[] data;
    private final StoragePath path;
    private final boolean absolute;
    private final Long offset;
    private final Long length;

    private ParsedBlob(byte[] data, StoragePath path, boolean absolute, Long offset, Long length) {
      this.data = data;
      this.path = path;
      this.absolute = absolute;
      this.offset = offset;
      this.length = length;
    }
  }

  /**
   * Opens {@code path} ({@link SourceObjectOpener}: with the object's listed length when known, so S3A sends no HEAD, and this
   * source's read-ahead); a FileNotFoundException is the file's absence only when a re-check finds it absent (a local "Permission
   * denied" is a FileNotFoundException of a file that is there: an access denial, a platform failure). An open with a known
   * length is lazy on S3A: a missing object is then reported at the first read or seek, which re-check it the same way.
   */
  private SeekableDataInputStream openChecked(HoodieStorage storage, StoragePath path, Long knownLength,
                                               SourceObjectVersion version) throws IOException {
    try {
      return version == null ? SourceObjectOpener.open(storage, path, READ_BUFFER_BYTES, knownLength, readaheadBytes)
          : version.open(storage, READ_BUFFER_BYTES, knownLength, readaheadBytes);
    } catch (IOException | RuntimeException e) {
      // the storage layer may wrap the FileNotFoundException (HoodieIOException "Exception creating input stream")
      throw notFoundChecked(storage, path, e);
    }
  }

  /**
   * {@code failure} as it is, unless its cause chain holds a FileNotFoundException of a file that a re-check finds present: then an
   * access denial carrying it (the file is there and unreadable, a platform failure); a re-check that cannot be made is no proof of
   * absence either, so that failure becomes a plain storage failure.
   */
  private static Exception notFoundCheckedImpl(HoodieStorage storage, StoragePath path, Exception failure) {
    for (Throwable cause : ExceptionUtil.getCauses(failure)) {
      if ("org.apache.hadoop.fs.s3a.RemoteFileChangedException".equals(cause.getClass().getName())) {
        BlobSourceChangedException changed = new BlobSourceChangedException("Source object " + path + " changed during its pinned read");
        changed.initCause(failure);
        return changed;
      }
    }
    if (!ExceptionUtil.isCausedBy(failure, FileNotFoundException.class)) {
      return failure;
    }
    boolean present;
    try {
      present = storage.exists(path);
    } catch (IOException | RuntimeException recheckFailure) {
      // no proof of absence: a storage failure; the original's report and stack ride as suppressed, never as the cause, so the
      // FileNotFoundException in it cannot read as the file's absence
      IOException unconfirmed = new IOException(BLOB_FILE + path + " could not be opened and its absence could not be confirmed: "
          + failure.getClass().getSimpleName() + ": " + failure.getMessage(), recheckFailure);
      unconfirmed.addSuppressed(failure);
      return unconfirmed;
    }
    if (!present) {
      return failure;
    }
    AccessDeniedException denied = new AccessDeniedException(path.toString(), null,
        "exists but cannot be opened (" + failure.getMessage() + ")");
    denied.initCause(failure);
    return denied;
  }

  private static IOException notFoundChecked(HoodieStorage storage, StoragePath path, Exception failure) {
    Exception checked = notFoundCheckedImpl(storage, path, failure);
    if (checked instanceof RuntimeException) {
      throw (RuntimeException) checked;
    }
    return (IOException) checked;
  }

  /**
   * Seeks {@code in} to {@code offset}. An EOFException is the stored object's shortness ({@link BlobTruncatedException}) only
   * when the object is shorter than the offset; any other EOFException (Hadoop reports a dropped connection as one) is a storage
   * failure.
   */
  private static void seekChecked(SeekableDataInputStream in, HoodieStorage storage, StoragePath path, long offset)
      throws IOException {
    try {
      in.seek(offset);
    } catch (EOFException e) {
      throw shortOrFailed(e, storage, path, offset);
    } catch (IOException | RuntimeException e) {
      // a lazy open (S3A) may report the object missing at its first seek: absent only when a re-check finds it absent
      throw notFoundChecked(storage, path, e);
    }
  }

  /** The failure of a seek that met an EOFException: the object's shortness when it is shorter than {@code offset}, else storage. */
  private static IOException shortOrFailed(EOFException e, HoodieStorage storage, StoragePath path, long offset) {
    long objectLength;
    try {
      objectLength = storage.getPathInfo(path).getLength();
    } catch (IOException | RuntimeException lengthFailure) {
      e.addSuppressed(lengthFailure);
      return new IOException("Seek to " + offset + " of " + path + " failed and the object's length could not be read", e);
    }
    if (objectLength < offset) {
      BlobTruncatedException truncated = new BlobTruncatedException(BLOB_FILE + path + " holds " + objectLength
          + " bytes, before the recorded offset " + offset);
      truncated.initCause(e);
      return truncated;
    }
    return new IOException("Seek to " + offset + " of the " + objectLength + "-byte " + path + " failed", e);
  }

  /**
   * The path of an {@code external_path}: an absolute one as it is; a relative one, as a managed reference, against the table base
   * path exactly as {@code read_blob()} resolves it ({@link BatchedBlobReader#resolveReferencePath}), so compute and
   * {@code read_blob()} open the same file. Without a base path, nothing resolves a relative path: the embedding job then reads an
   * object only where its source row names it.
   *
   * @throws BlobReferenceException for an empty or unparseable path, a relative one without a base path, or one escaping the base
   *     (e.g. {@code ../x})
   */
  public StoragePath resolve(String externalPath) throws IOException {
    if (externalPath == null || externalPath.isEmpty()) {
      throw new BlobReferenceException("OUT_OF_LINE blob has empty external_path");
    }
    if (isAbsolute(externalPath)) {
      return new StoragePath(externalPath);
    }
    if (tableBasePath == null) {
      throw new BlobReferenceException("OUT_OF_LINE blob has a relative external_path '" + externalPath + "'; the embedding job "
          + "reads an object only by its absolute path");
    }
    try {
      return new StoragePath(BatchedBlobReader.resolveReferencePath(externalPath, true, Option.apply(tableBasePath), Option.apply("1")));
    } catch (IllegalArgumentException e) {
      throw new BlobReferenceException(e.getMessage(), e);
    }
  }

  /** A URI scheme makes a path absolute; a scheme-less path belongs to its table. */
  private static boolean isAbsolute(String externalPath) throws BlobReferenceException {
    try {
      URI uri = new StoragePath(externalPath).toUri();
      return uri.getScheme() != null;
    } catch (IllegalArgumentException e) {
      throw new BlobReferenceException("Invalid blob path " + externalPath, e);
    }
  }

  /**
   * The storage that serves {@code path}: the base-path storage for a relative {@code external_path}, else the storage for the
   * path's own {@code scheme://authority}. A scheme outside the served set ({@link StorageReferenceRules#isServedScheme})
   * or an invalid URI/authority rejected during construction is a malformed reference. Other construction failures, a served
   * scheme whose file system is missing included, are storage errors, so prep can fail the round and retry the input.
   */
  private synchronized HoodieStorage storageFor(StoragePath path, boolean absolute) throws IOException {
    try {
      if (!absolute) {
        if (baseStorage == null) {
          baseStorage = HoodieStorageUtils.getStorage(tableBasePath, storageConf);
        }
        return baseStorage;
      }
      if (storageBySchemeAuthority == null) {
        storageBySchemeAuthority = new HashMap<>();
      }
      URI uri = path.toUri();
      String key = (uri.getScheme() == null ? "" : uri.getScheme()) + "://" + (uri.getAuthority() == null ? "" : uri.getAuthority());
      HoodieStorage forPath = storageBySchemeAuthority.get(key);
      if (forPath == null) {
        forPath = HoodieStorageUtils.getStorage(path, storageConf);
        storageBySchemeAuthority.put(key, forPath);
      }
      return forPath;
    } catch (RuntimeException e) {
      String message = "Cannot get storage for blob path " + path + ": " + e.getMessage();
      if (isMalformedStorageReference(path, e)) {
        throw new BlobReferenceException(message, e);
      }
      throw new IOException(message, e);
    }
  }

  /** Construction can reject an authority before any read; I/O failures at open/read never use this rule. */
  private boolean isMalformedStorageReference(StoragePath path, RuntimeException failure) {
    URI uri = path.toUri();
    for (Throwable cause : ExceptionUtil.getCauses(failure)) {
      if (cause instanceof UnsupportedFileSystemException) {
        // a scheme this deployment serves whose file system is missing is a platform fault (a connector jar absent on
        // the executor), never the reference's; a scheme outside that set is the reference's own fault
        String scheme = uri.getScheme();
        return !StorageSchemes.isSchemeSupported(scheme == null ? null : scheme.toLowerCase(Locale.ROOT))
            && !StorageReferenceRules.isServedScheme(scheme, storageConf);
      }
      if (StorageReferenceRules.isInvalidAuthorityFailure(uri, cause)) {
        return true;
      }
    }
    // a host that does not resolve (an UnknownHostException, also wrapped in an argument failure) is NOT the reference's fault: it
    // cannot be told from a DNS outage or a missing HA nameservice configuration, so it is a platform failure and the round fails
    return StorageReferenceRules.isMalformedConstruction(uri, failure, storageConf);
  }

  /**
   * A stream of an opened file whose reads re-check a FileNotFoundException as {@link #openChecked} does at open: a lazy open (S3A)
   * reports the object missing at its first read, and only a re-check that finds it absent makes that the file's absence.
   */
  private static final class NotFoundCheckedInputStream extends FilterInputStream {
    private final HoodieStorage storage;
    private final StoragePath path;

    NotFoundCheckedInputStream(InputStream in, HoodieStorage storage, StoragePath path) {
      super(in);
      this.storage = storage;
      this.path = path;
    }

    @Override
    public int read() throws IOException {
      try {
        return in.read();
      } catch (IOException | RuntimeException e) {
        throw notFoundChecked(storage, path, e);
      }
    }

    @Override
    public int read(byte[] buffer, int off, int len) throws IOException {
      try {
        return in.read(buffer, off, len);
      } catch (IOException | RuntimeException e) {
        throw notFoundChecked(storage, path, e);
      }
    }

    @Override
    public long skip(long n) throws IOException {
      try {
        return in.skip(n);
      } catch (IOException | RuntimeException e) {
        throw notFoundChecked(storage, path, e);
      }
    }

    @Override
    public int available() throws IOException {
      try {
        return in.available();
      } catch (IOException | RuntimeException e) {
        throw notFoundChecked(storage, path, e);
      }
    }
  }

  /**
   * Limits a stream to the next {@code remaining} bytes. The referenced range must exist in full: an underlying
   * stream that ends before {@code remaining} reaches 0 (a truncated file) fails with an {@link EOFException}
   * rather than handing back a short blob.
   */
  private static final class RangeInputStream extends FilterInputStream {
    private final StoragePath path;
    private long remaining;

    RangeInputStream(InputStream in, long length, StoragePath path) {
      super(in);
      this.remaining = length;
      this.path = path;
    }

    private EOFException truncated() {
      return new BlobTruncatedException(BLOB_FILE + path + " ended early: expected " + remaining + " more bytes of the referenced range");
    }

    @Override
    public int read() throws IOException {
      if (remaining <= 0) {
        return -1;
      }
      int b = super.read();
      if (b < 0) {
        throw truncated();
      }
      remaining--;
      return b;
    }

    @Override
    public int read(byte[] buffer, int off, int len) throws IOException {
      if (len == 0) {
        return 0;
      }
      if (remaining <= 0) {
        return -1;
      }
      int n = super.read(buffer, off, (int) Math.min(len, remaining));
      if (n < 0) {
        throw truncated();
      }
      remaining -= n;
      return n;
    }

    @Override
    public long skip(long n) throws IOException {
      long skipped = super.skip(Math.min(n, remaining));
      remaining -= skipped;
      return skipped;
    }

    @Override
    public int available() throws IOException {
      return (int) Math.min(super.available(), remaining);
    }

    @Override
    public boolean markSupported() {
      return false;
    }
  }

  /** A blob's bytes, already in memory: an INLINE blob's, held by the row, or a whole object read ahead. */
  private static final class ByteArrayBlob implements SeekableBlob {
    private final byte[] data;
    /** What a seek outside it names: as a storage range blob names its range, for bytes read ahead. */
    private final String described;
    private int pos;

    ByteArrayBlob(byte[] data) {
      this(data, "an INLINE blob of " + data.length + " bytes");
    }

    ByteArrayBlob(byte[] data, String described) {
      this.data = data;
      this.described = described;
    }

    @Override
    public long length() {
      return data.length;
    }

    @Override
    public long getPos() {
      return pos;
    }

    @Override
    public void seek(long newPos) throws IOException {
      if (newPos < 0 || newPos > data.length) {
        throw new BlobTruncatedException("Seek to " + newPos + " outside " + described);
      }
      pos = (int) newPos;
    }

    @Override
    public int read(byte[] buffer, int offset, int len) {
      if (pos >= data.length) {
        return -1;
      }
      int n = Math.min(len, data.length - pos);
      System.arraycopy(data, pos, buffer, offset, n);
      pos += n;
      return n;
    }

    @Override
    public void close() {
      // nothing to release
    }
  }

  /** An OUT_OF_LINE blob: the range [base, base + length) of a seekable storage stream. */
  private static final class StorageRangeBlob implements SeekableBlob {
    private final SeekableDataInputStream in;
    private final HoodieStorage storage;
    private final long base;
    private final long length;
    private final StoragePath path;

    StorageRangeBlob(SeekableDataInputStream in, HoodieStorage storage, long base, long length, StoragePath path) {
      this.in = in;
      this.storage = storage;
      this.base = base;
      this.length = length;
      this.path = path;
    }

    @Override
    public long length() {
      return length;
    }

    @Override
    public long getPos() throws IOException {
      return in.getPos() - base;
    }

    @Override
    public void seek(long pos) throws IOException {
      if (pos < 0 || pos > length) {
        throw new BlobTruncatedException("Seek to " + pos + " outside the " + length + "-byte blob range of " + path);
      }
      // inside the recorded range, a seek past the object's real end (HDFS and ABFS throw a bare EOFException) is the object's
      // truncation, checked against its length like the first seek
      seekChecked(in, storage, path, base + pos);
    }

    @Override
    public int read(byte[] buffer, int offset, int len) throws IOException {
      long remaining = length - getPos();
      if (remaining <= 0) {
        return -1;
      }
      int n;
      try {
        n = in.read(buffer, offset, (int) Math.min(len, remaining));
      } catch (IOException | RuntimeException e) {
        // a FileNotFoundException of a read is the file's absence only when a re-check finds it absent, as at open
        throw notFoundChecked(storage, path, e);
      }
      if (n < 0) {
        throw new BlobTruncatedException(BLOB_FILE + path + " ended early: expected " + remaining
            + " more bytes of the referenced range");
      }
      return n;
    }

    @Override
    public void close() throws IOException {
      in.close();
    }
  }
}
