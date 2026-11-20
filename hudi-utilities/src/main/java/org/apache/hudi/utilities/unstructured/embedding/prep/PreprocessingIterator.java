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

import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.sources.blob.BlobTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSource;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobColumns;
import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;
import org.apache.hudi.utilities.unstructured.embedding.source.PrepPlatformException;
import org.apache.hudi.utilities.unstructured.embedding.source.PreprocessorConfigurationException;
import org.apache.hudi.utilities.unstructured.embedding.source.Release;
import org.apache.hudi.utilities.unstructured.embedding.source.SeekableBlob;
import org.apache.hudi.utilities.unstructured.embedding.source.SeekableBlobBytesSupplier;
import org.apache.hudi.utilities.unstructured.embedding.source.SourceFailures;
import org.apache.hudi.utilities.unstructured.embedding.text.Tokenizer;

import org.apache.spark.TaskContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.util.TaskCompletionListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Consumer;

import scala.collection.JavaConverters;

/**
 * Lazily turns source rows into embedding-table rows inside one Spark task.
 *
 * <p>Owns everything a {@link FilePreprocessor} must not decide:
 * <ul>
 *   <li>it numbers a file's chunks per MODALITY (the record key includes the source version, modality and chunk):
 *       each modality's chunks are numbered through the default {@link SequentialChunkIdScheme} by their ordinal in that
 *       modality (dense, 0-based, in emit order). It trusts each preprocessor's chunks to keep the {@link Chunk} contract,
 *       which the shipped preprocessors' tests hold them to;</li>
 *   <li>it isolates any other non-fatal failure to its file (WARN {@code EMBED-PREP-FAILED}, keep the chunks already
 *       emitted, hand one {@link PrepSideOutput} of stage {@code prep} to the side-output sink, never a row, continue with the
 *       next file). Such failures include a failure of the preprocessor itself and a failing {@code close()} after the file's
 *       last chunk; the side output names
 *       the chunk_id the file's next chunk of its marker modality would have had, when that modality already has chunks;</li>
 *   <li>a file whose preprocessor finishes without error and without any chunk is WARN-logged ({@code EMBED-PREP-NO-TEXT}) and handed to the sink as stage
 *       {@code empty}, error {@code no_text}; it emits no row. A side output's modality is its preprocessor's
 *       {@link SourceKind#markerModality()};</li>
 *   <li>a file whose extension no preprocessor is mapped to is WARN-logged and handed to the sink as stage {@code empty}, error
 *       {@code no_preprocessor}, modality unknown: not a failure, so it is not counted as failed;</li>
 *   <li>a source row with no blob at all (its blob column is null) whose extension a preprocessor
 *       claims is WARN-logged ({@code EMBED-PREP-GONE}) and handed to the sink as stage {@code prep}, error
 *       {@code gone since announced}, counted as a failed file; with no preprocessor for its extension it is the
 *       {@code no_preprocessor} output above;</li>
 * <li>it keeps no state about a file outside the table: no attempt marker, no tombstone, no quarantine after N
 *       attempts. A file that ends its executor (an OutOfMemoryError) or hangs fails the round every time it is read, and every
 *       run resumes from the same checkpoint until an operator acts;</li>
 *   <li>an interrupted task is never isolated as a file failure: the exception propagates; nor is any failure whose cause or
 *       suppressed chain holds a round failure ({@link PrepFailures#roundFailure}): a {@link SourceReadException} (a storage
 *       failure of a source a later run can still read), a {@link PreprocessorConfigurationException}, a
 *       {@link PrepPlatformException}, a retryable {@link TokenizerException} or a JVM failure a decoder wrapped (an
 *       OutOfMemoryError or a LinkageError, not a StackOverflowError); it propagates and fails the task;</li>
 *   <li>an {@link Error} met while a file is opened or read closes this iterator (the open file and every preprocessor) and
 *       propagates;</li>
 *   <li>it closes the open file iterator on file switch, on failure, on exhaustion and on Spark task completion, and the
 *       preprocessors it resolved ({@link PreprocessorRegistry#close()}) when it closes.</li>
 * </ul>
 * At most one file is open and one output row is buffered at any time.
 */
public class PreprocessingIterator implements Iterator<Row>, AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(PreprocessingIterator.class);

  private final Iterator<Row> blobRows;
  private final EmbeddingTableConfig config;
  private final BlobBytesSource bytesSource;
  private final PreprocessorRegistry registry;
  private final Tokenizer tokenizer;
  private final PrepCounters counters;
  private final int modelColumnCount;
  private final List<String> userPartitionColumns;
  private final ChunkIdScheme chunkIds;
  /** Where failed and empty files go: never into the rows this iterator returns. */
  private final Consumer<PrepSideOutput> sideOutputs;
  /** The side outputs of an iterator built without a sink (tests); null otherwise. */
  private final List<PrepSideOutput> collectedSideOutputs;
  /** Side outputs handed to {@link #sideOutputs} so far: {@link #advanceToRowOrSideOutput()} stops at the next one. */
  private long sideOutputCount;

  /** Protects ownership transfers; storage and preprocessor calls never hold this lock. */
  private final Object lifecycle = new Object();
  private boolean opening;
  private boolean registryClosed;
  private int closingChunks;
  private BlobRowBytes currentBytes;
  private ClosableIterator<Chunk> currentChunks;
  private FilePreprocessor currentPreprocessor;
  private FileMeta currentFile;
  private Row currentSourceRow;
  /** The current source row's file name and source path (its version tag travels in {@link #currentFile}). */
  private String currentFileName;
  private String currentSourcePath;
  /** Chunks the current file emitted, all modalities. */
  private long fileChunks;
  /** Per modality, the chunks the current file emitted: the ordinal of its next chunk of that modality. */
  private final Map<String, Long> nextOrdinals = new HashMap<>();
  private Row pending;
  /** Volatile: close() may run from a task-completion listener on another thread than the consumer's. */
  private volatile boolean closed;
  /** The error that escaped advance(): the file it ended is half read, so the iterator does not go on. */
  private Throwable failedWith;

  /**
   * @param sideOutputs           receives every failed or empty file as it is met; null to keep them in memory
   *                              ({@link #getSideOutputs()})
   */
  public PreprocessingIterator(Iterator<Row> blobRows, EmbeddingTableConfig config, BlobBytesSource bytesSource,
                               PreprocessorRegistry registry, Tokenizer tokenizer, PrepCounters counters,
                               Consumer<PrepSideOutput> sideOutputs) {
    this.collectedSideOutputs = sideOutputs == null ? new ArrayList<>() : null;
    Consumer<PrepSideOutput> sink = sideOutputs == null ? collectedSideOutputs::add : sideOutputs;
    this.sideOutputs = sideOutput -> {
      sideOutputCount++;
      sink.accept(sideOutput);
    };
    this.blobRows = blobRows;
    this.config = config;
    this.bytesSource = bytesSource;
    this.registry = registry;
    this.tokenizer = tokenizer;
    this.counters = counters;
    this.modelColumnCount = config.models().size() * EmbeddingTableSchema.COLUMNS_PER_MODEL;
    this.userPartitionColumns = EmbeddingTableSchema.userPartitionColumns(config.getProps());
    this.chunkIds = new SequentialChunkIdScheme();
    TaskContext taskContext = TaskContext.get();
    if (taskContext != null) {
      taskContext.addTaskCompletionListener((TaskCompletionListener) context -> close());
    }
  }

  /** The side outputs met so far by an iterator built without a sink. */
  public List<PrepSideOutput> getSideOutputs() {
    if (collectedSideOutputs == null) {
      throw new IllegalStateException("This iterator hands its side outputs to a sink");
    }
    return Collections.unmodifiableList(collectedSideOutputs);
  }

  @Override
  public boolean hasNext() {
    checkNotFailed();
    while (pending == null && !closed) {
      advance();
    }
    return pending != null;
  }

  /**
   * Advances until a row is ready, the input ends, or a side output was handed to the sink during this call, whichever comes
   * first; returns whether a row is ready ({@link #next()} then returns it without advancing). A caller that queues side outputs
   * drains its queue between calls, so the queue holds those of one file step at most, however long a run of files that yield
   * no row is. {@link #hasNext()} instead advances past any number of such files.
   */
  public boolean advanceToRowOrSideOutput() {
    checkNotFailed();
    long seen = sideOutputCount;
    while (pending == null && !closed && sideOutputCount == seen) {
      advance();
    }
    return pending != null;
  }

  private void checkNotFailed() {
    if (failedWith != null) {
      throw new IllegalStateException("Prep iterator failed earlier; it does not continue a half-read file", failedWith);
    }
  }

  @Override
  public Row next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    Row row = pending;
    pending = null;
    return row;
  }

  private void advance() {
    try {
      if (currentChunks != null) {
        pullFromCurrentFile();
        return;
      }
      if (!blobRows.hasNext()) {
        close();
        return;
      }
      openFile(blobRows.next());
    } catch (Throwable t) {
      failedWith = t;
      closeSuppressedInto(t);
      throw t;
    }
  }

  private void pullFromCurrentFile() {
    // captured up front: closeCurrentFile() clears currentPreprocessor before close() can throw
    String preprocessorName = currentPreprocessor.name();
    String modality = currentPreprocessor.sourceKind().markerModality();
    Chunk chunk = null;
    long chunkId = 0L;
    try {
      if (currentChunks.hasNext()) {
        chunk = currentChunks.next();
        chunkId = chunkIdOf(chunk);
      } else {
        // a close() failure after the last chunk fails this file like any other per-file failure
        closeCurrentFile();
        counters.getFilesOk().add(1);
        if (fileChunks == 0) {
          // finished without error and without any chunk: no row, a WARN and an empty side output
          LOG.warn("EMBED-PREP-NO-TEXT source_path_sha={} source_path={} preprocessor={}", currentFile.getSourcePathSha(), currentSourcePath,
              preprocessorName);
          counters.getFilesNoText().add(1);
          sideOutputs.accept(sideOutput(modality, PrepSideOutput.STAGE_EMPTY, PrepSideOutput.NO_TEXT));
        }
      }
    } catch (SourceReadException | PreprocessorConfigurationException | PrepPlatformException e) {
      // a storage failure of a source a later run can still read, met while reading it, a configuration or a platform failure: the
      // round fails, never a quarantine entry committed past it
      throw e;
    } catch (Exception e) {
      PrepFailures.rethrowRoundFailure(e);
      rethrowInterruption(e);
      failCurrentFile(preprocessorName, modality, e);
      return;
    } catch (Error e) {
      throw closedOn(e);
    }
    if (chunk != null) {
      pending = chunkRow(chunk, chunkId);
      fileChunks++;
      counters.getChunks().add(1);
    }
  }

  private void openFile(Row blobRow) {
    synchronized (lifecycle) {
      if (closed) {
        return;
      }
      opening = true;
    }
    Throwable failure = null;
    try {
      openFileOwned(blobRow);
    } catch (Error error) {
      failure = error;
      throw closedOn(error);
    } catch (RuntimeException error) {
      failure = error;
      throw error;
    } finally {
      synchronized (lifecycle) {
        opening = false;
      }
      closeRegistryPreserving(failure);
    }
  }

  /**
   * An {@link Error} (a native library that does not link, an exhausted heap) ends the task: this iterator closes the open file and
   * every preprocessor (their native engines) before it propagates, so nothing they
   * hold waits for the task completion listener, which a caller outside Spark does not have.
   */
  private Error closedOn(Error e) {
    try {
      close();
    } catch (RuntimeException | Error closeFailure) {
      e.addSuppressed(closeFailure);
    }
    return e;
  }

  /**
   * The chunk_id of {@code chunk}: numbered per (file version, modality), so the record key of every row the file emits is new.
   * A row sits at the scheme's id of its ordinal in its modality, which the scheme keeps strictly increasing.
   */
  private long chunkIdOf(Chunk chunk) {
    String modality = chunk.getModality();
    long ordinal = nextOrdinals.getOrDefault(modality, 0L);
    long id = chunkIds.chunkId(ordinal);
    nextOrdinals.put(modality, ordinal + 1);
    return id;
  }

  /**
   * An interrupted task is never isolated as a file failure: the exception propagates and the
   * task ends.
   */
  private static void rethrowInterruption(Exception e) {
    TaskContext task = TaskContext.get();
    boolean interrupted = Thread.currentThread().isInterrupted() || (task != null && task.isInterrupted());
    for (Throwable t = e; t != null && !interrupted; t = t.getCause() == t ? null : t.getCause()) {
      interrupted = t instanceof java.io.InterruptedIOException || t instanceof InterruptedException
          || t instanceof java.nio.channels.ClosedByInterruptException; // an NIO read interrupted
    }
    if (interrupted) {
      throw e instanceof RuntimeException ? (RuntimeException) e : new HoodieException("Interrupted while preprocessing", e);
    }
  }

  private void failCurrentFile(String preprocessorName, String modality, Exception cause) {
    try {
      closeCurrentFile();
    } catch (Exception closeFailure) {
      // closing the failed file met a failure that fails the round (a storage failure of a source a later run can still read, a
      // configuration or a platform failure): the round fails, carrying the file's own failure, and nothing is quarantined
      closeFailure.addSuppressed(cause);
      PrepFailures.rethrowRoundFailure(closeFailure);
      LOG.warn("Failed closing preprocessor iterator for source_path_sha={}", currentFile.getSourcePathSha(), closeFailure);
    }
    LOG.warn("EMBED-PREP-FAILED source_path_sha={} source_path={} extension={} preprocessor={} chunksKept={}",
        currentFile.getSourcePathSha(), currentSourcePath, currentFile.getFileExtension(), preprocessorName, fileChunks, cause);
    counters.getFilesFailed().add(1);
    String error = ErrorText.truncate(describe(cause), EmbeddingTableSchema.MAX_ERROR_CHARS);
    Long nextChunkId = nextOrdinals.getOrDefault(modality, 0L) > 0 ? nextOrdinals.get(modality) : null;
    sideOutputs.accept(new PrepSideOutput(currentFile.getSourcePathSha(),
        currentFile.getSourceModificationTime(), currentFile.getFileSize(), currentSourcePath, modality, nextChunkId, PrepSideOutput.STAGE_PREP, error));
  }

  /** A side output of the current file (no chunk: the whole file). */
  private PrepSideOutput sideOutput(String modality, String stage, String error) {
    return new PrepSideOutput(currentFile.getSourcePathSha(),
        currentFile.getSourceModificationTime(), currentFile.getFileSize(), currentSourcePath, modality, null, stage, error);
  }

  /**
   * The quarantined error: a {@link PreprocessingException}'s message as thrown, without a class prefix. For the PDF
   * chunker's failures and the quarantine marker that message is the contract's {@code "CODE: detail"};
   * other preprocessors' messages are kept as they are, uncoded. Any other failure is kept as its class and message plus its root
   * cause, so the quarantine entry alone tells an operator what went wrong.
   */
  private static String describe(Throwable failure) {
    if (failure instanceof PreprocessingException) {
      return failure.getMessage();
    }
    Throwable root = failure;
    while (root.getCause() != null && root.getCause() != root) {
      root = root.getCause();
    }
    String described = failure.getClass().getSimpleName() + ": " + failure.getMessage();
    return root == failure ? described
        : described + " (root cause " + root.getClass().getSimpleName() + ": " + root.getMessage() + ")";
  }

  /** The row of {@code chunk}, in {@link EmbeddingTableSchema#build} column order. */
  private Row chunkRow(Chunk chunk, long chunkId) {
    // every model's VECTOR column stays NULL here
    Object[] values = new Object[EmbeddingTableSchema.BASE_COLUMN_COUNT + modelColumnCount
        + userPartitionColumns.size()];
    values[0] = currentFile.getSourcePathSha();
    values[1] = currentFile.getSourceModificationTime();
    values[2] = currentFile.getFileSize();
    values[3] = chunk.getModality();
    values[4] = chunkId;
    values[5] = currentSourcePath;
    values[6] = currentFileName;
    values[7] = currentFile.getFileExtension();
    values[8] = chunk.getText();
    values[9] = chunk.getTokenCount();
    Map<String, String> aux = chunk.getAux();
    // the Row encoder accepts only a Scala map for MapType
    values[10] = aux == null ? null : JavaConverters.mapAsScalaMap(aux);
    for (int i = 0; i < userPartitionColumns.size(); i++) {
      String column = userPartitionColumns.get(i);
      values[EmbeddingTableSchema.BASE_COLUMN_COUNT + modelColumnCount + i] =
          currentSourceRow.schema().getFieldIndex(column).isDefined() ? currentSourceRow.getAs(column) : null;
    }
    return RowFactory.create(values);
  }

  /** The file identity of a source row, which the unstructured source always fills. */
  private static FileMeta readFileMeta(Row blobRow) {
    return new FileMeta(blobRow.getAs(BlobColumns.SOURCE_PATH_SHA), blobRow.<Long>getAs(BlobColumns.SOURCE_MODIFICATION_TIME),
        blobRow.getAs(BlobColumns.SOURCE_FILE_EXTENSION), blobRow.<Long>getAs(BlobColumns.SOURCE_FILE_SIZE));
  }

  /** Whether the source row carries no blob at all. */
  private static boolean isGone(Row blobRow) {
    return blobRow.isNullAt(blobRow.fieldIndex(BlobColumns.BLOB));
  }

  private void closeCurrentFile() {
    ClosableIterator<Chunk> toClose;
    BlobRowBytes toCheck;
    synchronized (lifecycle) {
      toClose = currentChunks;
      toCheck = currentBytes;
      currentChunks = null;
      currentBytes = null;
      currentPreprocessor = null;
      if (toClose != null) {
        closingChunks++;
      }
    }
    if (toClose == null) {
      return;
    }
    Throwable failure = null;
    try {
      closeChunks(toClose, toCheck);
    } catch (RuntimeException | Error error) {
      failure = error;
      throw error;
    } finally {
      synchronized (lifecycle) {
        closingChunks--;
      }
      closeRegistryPreserving(failure);
    }
  }

  private void closeCurrentFileQuietly() {
    try {
      closeCurrentFile();
    } catch (Exception e) {
      LOG.warn("Failed closing preprocessor iterator for source_path_sha={}", currentFile.getSourcePathSha(), e);
    }
  }

  @Override
  public void close() {
    synchronized (lifecycle) {
      if (closed) {
        return;
      }
      closed = true;
    }
    try {
      closeCurrentFileQuietly();
    } finally {
      closeRegistryIfUnowned();
    }
  }

  /** The bytes of one source row, as a stream. */
  private static final class BlobRowBytes implements SeekableBlobBytesSupplier {
    private final BlobBytesSource source;
    private final Row blob;
    private final FileMeta file;
    private final long objectLength;
    /** The whole object, read ahead; null when it was not. */
    private final byte[] readAhead;
    private Exception readFailure;

    BlobRowBytes(BlobBytesSource source, Row blob, FileMeta file, byte[] readAhead) {
      this.source = source;
      this.blob = blob;
      this.file = file;
      this.objectLength = file.getFileSize();
      this.readAhead = readAhead;
    }

    private IOException recordFailure(Exception failure) {
      try {
        IOException checked = classified(failure);
        remember(checked);
        return checked;
      } catch (RuntimeException roundFailure) {
        remember(roundFailure);
        throw roundFailure;
      }
    }

    private synchronized void remember(Exception failure) {
      if (readFailure == null) {
        readFailure = failure;
      } else if (readFailure != failure) {
        readFailure.addSuppressed(failure);
      }
    }

    private synchronized void checkFailure() {
      if (readFailure != null) {
        // A separate report also avoids self-suppression if decoder close propagated the very failure we retained.
        HoodieException reported = new HoodieException("Source read failed for " + file.getSourcePathSha(), readFailure);
        PrepFailures.rethrowRoundFailure(reported);
        throw reported;
      }
    }

    @Override
    public InputStream open() throws IOException {
      try {
        source.checkVersion(blob, file.getSourceModificationTime(), objectLength);
        if (readAhead != null) {
          return new ByteArrayInputStream(readAhead);
        }
        return new SourceInputStream(source.open(blob, objectLength));
      } catch (IOException | RuntimeException e) {
        throw recordFailure(e);
      }
    }

    @Override
    public SeekableBlob openSeekable() throws IOException {
      try {
        source.checkVersion(blob, file.getSourceModificationTime(), objectLength);
        return new SourceSeekableBlob(readAhead != null ? source.openSeekable(blob, readAhead) : source.openSeekable(blob, objectLength));
      } catch (IOException | RuntimeException e) {
        throw recordFailure(e);
      }
    }

    private final class SourceInputStream extends FilterInputStream {
      private SourceInputStream(InputStream in) {
        super(in);
      }

      @Override
      public int read() throws IOException {
        try {
          return in.read();
        } catch (IOException | RuntimeException e) {
          throw recordFailure(e);
        }
      }

      @Override
      public int read(byte[] buffer, int offset, int length) throws IOException {
        try {
          return in.read(buffer, offset, length);
        } catch (IOException | RuntimeException e) {
          throw recordFailure(e);
        }
      }

      @Override
      public long skip(long length) throws IOException {
        try {
          return in.skip(length);
        } catch (IOException | RuntimeException e) {
          throw recordFailure(e);
        }
      }

      @Override
      public int available() throws IOException {
        try {
          return in.available();
        } catch (IOException | RuntimeException e) {
          throw recordFailure(e);
        }
      }

      @Override
      public void reset() throws IOException {
        try {
          in.reset();
        } catch (IOException | RuntimeException e) {
          throw recordFailure(e);
        }
      }

      @Override
      public void close() throws IOException {
        try {
          in.close();
        } catch (IOException | RuntimeException e) {
          throw recordFailure(e);
        }
      }
    }

    /** The random-access view of the source, its failures classified as the stream's are. */
    private final class SourceSeekableBlob implements SeekableBlob {
      private final SeekableBlob source;

      private SourceSeekableBlob(SeekableBlob source) {
        this.source = source;
      }

      @Override
      public long length() {
        return source.length();
      }

      @Override
      public long getPos() throws IOException {
        try {
          return source.getPos();
        } catch (IOException | RuntimeException e) {
          throw recordFailure(e);
        }
      }

      @Override
      public void seek(long position) throws IOException {
        try {
          source.seek(position);
        } catch (IOException | RuntimeException e) {
          throw recordFailure(e);
        }
      }

      @Override
      public int read(byte[] buffer, int offset, int length) throws IOException {
        try {
          return source.read(buffer, offset, length);
        } catch (IOException | RuntimeException e) {
          throw recordFailure(e);
        }
      }

      @Override
      public void close() throws IOException {
        try {
          source.close();
        } catch (IOException | RuntimeException e) {
          throw recordFailure(e);
        }
      }
    }
  }

  private void openFileOwned(Row blobRow) {
    currentSourceRow = blobRow;
    fileChunks = 0;
    nextOrdinals.clear();
    currentFileName = null;
    currentSourcePath = blobRow.getAs(BlobColumns.SOURCE_FILE_PATH);
    currentFile = readFileMeta(blobRow);
    currentFileName = blobRow.getAs(BlobColumns.SOURCE_FILE_NAME);
    Option<FilePreprocessor> preprocessor = registry.forExtension(currentFile.getFileExtension());
    if (!preprocessor.isPresent()) {
      LOG.warn("EMBED-PREP-NO-PREPROCESSOR source_path_sha={} source_path={} extension={}", currentFile.getSourcePathSha(),
          currentSourcePath, currentFile.getFileExtension());
      counters.getFilesNoPreprocessor().add(1);
      sideOutputs.accept(sideOutput(EmbeddingTableSchema.MODALITY_UNKNOWN, PrepSideOutput.STAGE_EMPTY,
          PrepSideOutput.NO_PREPROCESSOR));
      return;
    }
    FilePreprocessor processor = preprocessor.get();
    if (isGone(blobRow)) {
      // the source confirmed the object absent at its status call or its inline read: the file fails as a failed file does
      // (a side output of stage prep), and only a file a preprocessor would have read
      LOG.warn("EMBED-PREP-GONE source_path_sha={} source_path={}", currentFile.getSourcePathSha(), currentSourcePath);
      counters.getFilesFailed().add(1);
      sideOutputs.accept(sideOutput(processor.sourceKind().markerModality(), PrepSideOutput.STAGE_PREP,
          BlobTableSchema.GONE_SINCE_ANNOUNCED));
      return;
    }
    try {
      Row blob = blobRow.getAs(BlobColumns.BLOB);
      BlobRowBytes openedBytes = new BlobRowBytes(bytesSource, blob, currentFile, null);
      ClosableIterator<Chunk> opened = null;
      try (Release<RuntimeException> sourceCheck = openedBytes::checkFailure) {
        opened = processor.open(openedBytes, currentFile, tokenizer, config.getProps());
      } catch (Exception | Error failure) {
        if (opened != null) {
          try {
            opened.close();
          } catch (RuntimeException | Error closeFailure) {
            if (closeFailure != failure) {
              failure.addSuppressed(closeFailure);
            }
          }
        }
        throw failure;
      }
      synchronized (lifecycle) {
        if (!closed) {
          currentPreprocessor = processor;
          currentChunks = opened;
          currentBytes = openedBytes;
          return;
        }
      }
      closeChunks(opened, openedBytes);
    } catch (SourceReadException | PreprocessorConfigurationException | PrepPlatformException e) {
      // a storage failure of a source a later run can still read, a configuration or a platform failure: the round fails,
      // never a quarantine entry committed past it
      throw e;
    } catch (Exception e) {
      PrepFailures.rethrowRoundFailure(e);
      rethrowInterruption(e);
      if (closed) {
        LOG.warn("A late preprocessor open or close failed for source_path_sha={}", currentFile.getSourcePathSha(), e);
      } else {
        failCurrentFile(processor.name(), processor.sourceKind().markerModality(), e);
      }
    } catch (Error e) {
      throw closedOn(e);
    }
  }

  private void closeRegistryIfUnowned() {
    synchronized (lifecycle) {
      if (!closed || opening || currentChunks != null || closingChunks != 0 || registryClosed) {
        return;
      }
      registryClosed = true;
    }
    registry.close();
  }

  private void closeRegistryPreserving(Throwable failure) {
    try {
      closeRegistryIfUnowned();
    } catch (RuntimeException | Error cleanup) {
      if (failure == null) {
        throw cleanup;
      }
      failure.addSuppressed(cleanup);
    }
  }

  /**
   * A failure of the source is classified before a decoder can wrap it as a content failure: the file's own (its source gone or
   * short, its row malformed) and one that already fails the round pass as they are; any other, checked or unchecked, is a
   * storage failure of a source a later run can still read, because bad input that cannot be told from a platform failure is a
   * platform failure.
   */
  static IOException classified(Exception failure) {
    RuntimeException round = PrepFailures.roundFailure(failure);
    if (round == null && !SourceFailures.isContentFailure(failure)) {
      throw new SourceReadException("Failed to read the blob source", failure);
    }
    if (round != null && round.getCause() == failure) {
      // a JVM failure under the read (roundFailure wraps it around the failure): thrown unchecked, so no decoder can take
      // the read's IOException for an end of data and the file for bad input
      throw round;
    }
    if (failure instanceof RuntimeException) {
      throw (RuntimeException) failure;
    }
    return (IOException) failure;
  }

  /** Source failures remain observable even when decoder cleanup swallowed them. */
  private static void closeChunks(ClosableIterator<Chunk> chunks, BlobRowBytes bytes) {
    try (Release<RuntimeException> sourceCheck = () -> {
      if (bytes != null) {
        bytes.checkFailure();
      }
    }) {
      chunks.close();
    }
  }

  /** Closes this iterator after {@code failure}, which stays the one thrown: a failure of the close is suppressed on it. */
  private void closeSuppressedInto(Throwable failure) {
    try {
      close();
      // by design: the close runs after a failure that is rethrown; whatever the close throws must not replace it
    } catch (Throwable closeFailure) { // NOSONAR (by design: see above)
      if (closeFailure != failure) {
        failure.addSuppressed(closeFailure);
      }
    }
  }
}
