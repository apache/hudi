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

package org.apache.hudi.utilities.unstructured.embedding.compute;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingModelSpec;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.image.ImagePreparer;
import org.apache.hudi.utilities.unstructured.embedding.image.ImagePreprocessingSpec;
import org.apache.hudi.utilities.unstructured.embedding.image.SeekableBlobImageInputStream;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSource;
import org.apache.hudi.utilities.unstructured.retry.TransientRetry;

import org.apache.spark.TaskContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.TaskCompletionListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;

/**
 * Embeds the rows of one Spark task in micro batches of at most {@code batchSize} rows (one {@link EmbeddingClient} call
 * each) and yields EVERY processed row, in {@code schema}, with the model's VECTOR column and the working error
 * column ({@link ComputeRound#ERROR_WORKING_COLUMN}) set: embedded → the
 * vector, error NULL; skipped for good (PERMANENT) → NULL vector, the error. No row is yielded
 * pending.
 *
 * <p>Retries happen here, inside the compute step. An input whose call fails transiently — the
 * call throws, its answer is malformed (wrong size, or an OK result without a vector of the model's dimension), or the client
 * answers RETRYABLE — is tried again alone, after a backoff ({@code compute.retry.backoff.ms}, doubling, capped), until it has
 * been tried {@code compute.max.attempts} times; the first retry of a batch whose call threw calls each input alone, so a
 * healthy batch-mate of a bad input is embedded. An input still failing after its last try fails the task with
 * {@link EmbeddingComputeTransientException}. So do a client
 * that fails to initialize ({@code EMBED-COMPUTE-CLIENT-INIT-FAILED}) and a call throwing
 * {@link EmbeddingProviderUnavailableException}. A call that throws
 * {@link EmbeddingClientConfigException} or {@link EmbeddingComputePlatformException} (a broken prep/compute invariant) is never
 * retried and fails the task. No retry state is stored in a row. At most one
 * batch is buffered.
 *
 * <p>Rows are handed to the client only when the model declares their modality
 * ({@link EmbeddingModelSpec#getModalities()}, {@code hoodie.unstructured.embedding.model.<alias>.modalities}); a row of another
 * modality is never sent and gets a PERMANENT failure {@code EMBED-COMPUTE-<MODALITY>-NOT-SUPPORTED} naming the model and the
 * provider, in its place in the batch. Image rows of a model that declares {@code image} are handed to the client as
 * {@link EmbeddingInput#storedImage} over their {@code prepared_image} and {@link EmbeddingInput#image()}: an
 * {@link ImagePreparer} under the compute image settings ({@link EmbeddingTableConfig#imagePreprocessing()}) over a seekable
 * stream of the stored input, run lazily per input when the client builds its request (never a batch of decoded images), which
 * decodes and resizes the image in memory within the raster budget. A failure of the open, a read or a seek of the stored
 * input, a premature EOF included, reaches the client as a storage failure ({@link SeekableBlobImageInputStream#openStoredInput});
 * the client turns every preparation failure into that input's outcome with {@link ImageFailures#result}.
 *
 * <p>Output rows keep every prepared column as read. A failing {@link EmbeddingClient#close()} is only logged (WARN {@code EMBED-COMPUTE-CLIENT-CLOSE-FAILED}).
 */
public class MicroBatchEmbeddingIterator implements Iterator<Row>, AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(MicroBatchEmbeddingIterator.class);
  /** The task's rows, read one micro batch at a time. */
  private final Iterator<Row> rows;
  private final EmbeddingModelSpec model;
  private final int batchSize;
  /** Tries of one row in this step ({@code compute.max.attempts}). */
  private final int maxAttempts;
  /** The shared backoff schedule, with compute's 60-second cap. */
  private final TransientRetry retry;
  private final ComputeCounters counters;
  private final int outputWidth;
  private final int vectorIndex;
  private final int errorIndex;
  private final int sourcePathShaIndex;
  private final int sourceModificationTimeIndex;
  private final int sourceFileSizeIndex;
  private final int chunkIdIndex;
  private final int modalityIndex;
  private final int textIndex;
  /** The index of {@code prepared_image} in {@code schema}, a column every embedding table has. */
  private final int preparedImageIndex;
  /** Opens image rows' stored inputs; null for an iterator built for text rows only. */
  private final BlobBytesSource storedInputs;
  /** Prepares image inputs of a model that declares image; null otherwise. */
  private final ImagePreparer imagePreparer;
  private final EmbeddingClient client;
  /** A permanent initialization refusal is delivered as a normal task outcome, after the factory closes the client. */
  private final EmbeddingClientConfigException initializationRefusal;
  private Iterator<Row> currentBatch = Collections.emptyIterator();
  private boolean closed;
  /** A permanent provider refusal stops further source reads and model calls. */
  private boolean refused;

  /** Creates the task's embedding iterator for text rows only: an image row fails the task (no stored-input reader). */
  MicroBatchEmbeddingIterator(Iterator<Row> rows, EmbeddingModelSpec model, TypedProperties props,
                              StructType schema, int batchSize, ComputeCounters counters) {
    this(rows, model, props, schema, batchSize, counters, null);
  }

  /**
   * Creates the task's embedding iterator; {@code storedInputs} opens image rows' {@code prepared_image}.
   * The final schema field is the private error slot appended by {@link ComputeRound#embed}.
   */
  MicroBatchEmbeddingIterator(Iterator<Row> rows, EmbeddingModelSpec model, TypedProperties props,
                              StructType schema, int batchSize, ComputeCounters counters, BlobBytesSource storedInputs) {
    this.model = model;
    this.batchSize = batchSize;
    EmbeddingTableConfig config = EmbeddingTableConfig.from(props);
    this.maxAttempts = config.computeMaxAttempts();
    this.retry = new TransientRetry(maxAttempts, config.computeRetryBackoffMs(), EmbeddingTableConfig.COMPUTE_RETRY_MAX_BACKOFF_MS);
    this.counters = counters;
    this.outputWidth = schema.fields().length;
    this.vectorIndex = schema.fieldIndex(model.getColumn());
    // ComputeRound appends the private outcome slot after every table column. Its name may be suffixed to keep
    // customer columns distinct under Spark's case-insensitive resolution.
    this.errorIndex = outputWidth - 1;
    this.sourcePathShaIndex = schema.fieldIndex(EmbeddingTableSchema.SOURCE_PATH_SHA);
    this.sourceModificationTimeIndex = schema.fieldIndex(EmbeddingTableSchema.SOURCE_MODIFICATION_TIME);
    this.sourceFileSizeIndex = schema.fieldIndex(EmbeddingTableSchema.SOURCE_FILE_SIZE);
    this.chunkIdIndex = schema.fieldIndex(EmbeddingTableSchema.CHUNK_ID);
    this.modalityIndex = schema.fieldIndex(EmbeddingTableSchema.MODALITY);
    this.textIndex = schema.fieldIndex(EmbeddingTableSchema.PREPARED_TEXT_CHUNK);
    this.preparedImageIndex = schema.fieldIndex(EmbeddingTableSchema.PREPARED_IMAGE);
    this.storedInputs = storedInputs;
    if (model.getModalities().isEmpty()) {
      throw new EmbeddingClientConfigException("Config " + EmbeddingTableConfig.MODEL_PREFIX + model.getName()
          + EmbeddingTableConfig.MODEL_MODALITIES_SUFFIX + " is not set: declare the modalities model " + model.getName()
          + " embeds (text, image or text,image)");
    }
    // validated before the client starts, so a bad image setting leaves no client to close
    ImagePreprocessingSpec imageSpec = config.imagePreprocessing();
    EmbeddingClient created = null;
    EmbeddingClientConfigException rejected = null;
    try {
      // Empty partitions have no row on which to report a client initialization refusal.
      if (rows.hasNext()) {
        created = EmbeddingClientFactory.create(model, props);
      }
    } catch (EmbeddingClientConfigException e) {
      rejected = e;
    } catch (Exception e) {
      // a client that cannot start fails the task
      throw new EmbeddingComputeTransientException("EMBED-COMPUTE-CLIENT-INIT-FAILED model=" + model.getName()
          + ": the embedding client could not start", e);
    }
    this.client = created;
    this.initializationRefusal = rejected;
    this.imagePreparer = model.declares(EmbeddingTableSchema.MODALITY_IMAGE) ? new ImagePreparer(imageSpec) : null;
    this.rows = rows;
    TaskContext taskContext = TaskContext.get();
    if (taskContext != null) {
      taskContext.addTaskCompletionListener((TaskCompletionListener) context -> close());
    }
  }

  @Override
  public boolean hasNext() {
    while (!refused && !currentBatch.hasNext() && rows.hasNext()) {
      currentBatch = embedNextBatch();
    }
    if (!currentBatch.hasNext()) {
      close();
      return false;
    }
    return true;
  }

  @Override
  public Row next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    return currentBatch.next();
  }

  /**
   * Reads one micro batch and calls the model once for its inputs; an image row the model does not declare is refused
   * without a call, in its place in the batch.
   */
  private Iterator<Row> embedNextBatch() {
    if (initializationRefusal != null) {
      return refusedTask(rows.next(), initializationRefusal);
    }
    List<Row> batch = new ArrayList<>(batchSize);
    List<EmbeddingResult> refusals = new ArrayList<>(batchSize);
    List<EmbeddingInput> inputs = new ArrayList<>(batchSize);
    while (batch.size() < batchSize && rows.hasNext()) {
      Row row = rows.next();
      batch.add(row);
      EmbeddingResult refusal = refusal(row);
      refusals.add(refusal);
      if (refusal == null) {
        inputs.add(toInput(row));
      }
    }
    List<EmbeddingResult> results;
    try {
      results = inputs.isEmpty() ? Collections.emptyList() : embedWithRetries(inputs);
    } catch (EmbeddingClientConfigException e) {
      return refusedTask(batch.get(0), e);
    }
    List<Row> processed = new ArrayList<>(batch.size());
    int called = 0;
    for (int i = 0; i < batch.size(); i++) {
      EmbeddingResult result = refusals.get(i) != null ? refusals.get(i) : results.get(called++);
      processed.add(withOutcome(batch.get(i), result));
    }
    return processed.iterator();
  }

  /**
   * The PERMANENT result of a row whose modality the model does not declare, {@code EMBED-COMPUTE-<MODALITY>-NOT-SUPPORTED};
   * null for a row the client is called for.
   */
  private EmbeddingResult refusal(Row row) {
    String modality = row.getString(modalityIndex);
    if (model.declares(modality)) {
      return null;
    }
    return EmbeddingResult.permanent(notSupported(model, modality));
  }

  /** The error of a row of {@code modality}, which {@code model} does not declare. */
  static String notSupported(EmbeddingModelSpec model, String modality) {
    return "EMBED-COMPUTE-" + String.valueOf(modality).toUpperCase(Locale.ROOT) + "-NOT-SUPPORTED model=" + model.getName()
        + " provider=" + model.getProvider() + ": " + EmbeddingTableConfig.MODEL_PREFIX + model.getName()
        + EmbeddingTableConfig.MODEL_MODALITIES_SUFFIX + "=" + String.join(",", model.getModalities()) + " does not declare "
        + modality + ", so this row is not sent to the model";
  }

  /**
   * One final result per input, embedded or skipped for good: the batch call's, then for every input left RETRYABLE its own
   * single-input calls after a backoff, until the input has been tried {@link #maxAttempts} times.
   *
   * @throws EmbeddingComputeTransientException if an input is still RETRYABLE after its last try, or the provider is
   *     unavailable
   * @throws EmbeddingClientConfigException from any call: the configuration is rejected, retrying cannot help
   * @throws EmbeddingComputePlatformException from any call: a prep/compute invariant is broken, retrying cannot help
   */
  private List<EmbeddingResult> embedWithRetries(List<EmbeddingInput> inputs) {
    List<EmbeddingResult> results = new ArrayList<>(embedBatch(inputs));
    for (int tries = 1; ; tries++) {
      List<Integer> retryable = new ArrayList<>();
      for (int i = 0; i < results.size(); i++) {
        if (results.get(i).getStatus() == EmbeddingResult.Status.RETRYABLE_FAILURE) {
          retryable.add(i);
        }
      }
      if (retryable.isEmpty()) {
        checkNotRejectedAsAWhole(inputs, results);
        return results;
      }
      if (tries >= maxAttempts) {
        EmbeddingInput input = inputs.get(retryable.get(0));
        throw new EmbeddingComputeTransientException("EMBED-COMPUTE-RETRIES-EXHAUSTED model=" + model.getName() + ": "
            + retryable.size() + " input(s) still failing after " + tries + " tries (" + EmbeddingTableConfig.COMPUTE_MAX_ATTEMPTS.key()
            + "), first " + input.describeIdentity() + ": "
            + results.get(retryable.get(0)).getError());
      }
      sleep(retry.waitBefore(tries + 1));
      for (int i : retryable) {
        counters.getRowsRetried().add(1);
        results.set(i, embedAlone(inputs.get(i)));
      }
    }
  }

  /** Alike permanent rejections across files fail the round unless the client promises independent input answers. */
  private void checkNotRejectedAsAWhole(List<EmbeddingInput> inputs, List<EmbeddingResult> results) {
    if (client.answersEachInputAlone() || results.stream().allMatch(EmbeddingResult::isKnownInputFailure)) {
      return;
    }
    String error = results.isEmpty() ? null : results.get(0).getError();
    for (EmbeddingResult result : results) {
      if (result.getStatus() != EmbeddingResult.Status.PERMANENT_FAILURE || error == null || !error.equals(result.getError())) {
        return;
      }
    }
    if (inputs.stream().map(EmbeddingInput::getSourcePathSha).distinct().count() < 2) {
      return;
    }
    throw new EmbeddingClientConfigException("EMBED-COMPUTE-REJECTED-AS-A-WHOLE model=" + model.getName() + ": all "
        + results.size() + " inputs of one call, of more than one file, first " + inputs.get(0).describeIdentity()
        + ", were rejected with the same error and none was embedded, so the provider "
        + "rejects the request, not an input: " + error + "; the compute commit fails and the next sync retries from the same checkpoint");
  }

  private void sleep(long millis) {
    if (millis <= 0) {
      return;
    }
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new EmbeddingComputeTransientException("Embedding compute for model " + model.getName()
          + " was interrupted waiting to retry", e);
    }
  }

  /**
   * One result per input from the batch call; every input RETRYABLE when the call throws or answers malformed.
   *
   * @throws EmbeddingClientConfigException the configuration is rejected
   * @throws EmbeddingComputePlatformException a prep/compute invariant is broken
   * @throws EmbeddingComputeTransientException the provider is unavailable
   */
  private List<EmbeddingResult> embedBatch(List<EmbeddingInput> inputs) {
    try {
      List<EmbeddingResult> results = client.embed(inputs);
      String malformed = malformed(results, inputs.size());
      if (malformed == null) {
        counters.getBatchesOk().add(1);
        return results;
      }
      LOG.warn("EMBED-COMPUTE-BATCH-FAILED model={} rows={} firstInput=[{}]: {}", model.getName(), inputs.size(),
          inputs.get(0).describeIdentity(), malformed);
      counters.getBatchesFailed().add(1);
      return Collections.nCopies(inputs.size(), EmbeddingResult.retryable(malformed));
    } catch (EmbeddingClientConfigException | EmbeddingComputePlatformException e) {
      throw e;
    } catch (EmbeddingProviderUnavailableException e) {
      throw providerUnavailable(e);
    } catch (InterruptedException e) {
      throw interrupted(e);
    } catch (Exception e) {
      LOG.warn("EMBED-COMPUTE-BATCH-FAILED model={} rows={} firstInput=[{}]; retrying each input alone", model.getName(),
          inputs.size(), inputs.get(0).describeIdentity(), e);
      counters.getBatchesFailed().add(1);
      return Collections.nCopies(inputs.size(), EmbeddingResult.retryable(describe(e)));
    } finally {
      addRateLimitWait();
    }
  }

  private EmbeddingResult embedAlone(EmbeddingInput input) {
    try {
      List<EmbeddingResult> results = client.embed(Collections.singletonList(input));
      String malformed = malformed(results, 1);
      return malformed == null ? results.get(0) : EmbeddingResult.retryable(malformed);
    } catch (EmbeddingClientConfigException | EmbeddingComputePlatformException e) {
      throw e;
    } catch (EmbeddingProviderUnavailableException e) {
      throw providerUnavailable(e);
    } catch (InterruptedException e) {
      throw interrupted(e);
    } catch (Exception e) {
      LOG.warn("EMBED-COMPUTE-INPUT-FAILED model={} {}", model.getName(), input.describeIdentity(), e);
      return EmbeddingResult.retryable(describe(e));
    } finally {
      addRateLimitWait();
    }
  }

  /** Includes pacing from successful and throwing calls in the round counters. */
  private void addRateLimitWait() {
    EmbeddingCallStats stats = client.lastCallStats();
    if (stats != null && stats.getRateLimitWaitMillis() > 0) {
      counters.getRateLimitWaitMillis().add(stats.getRateLimitWaitMillis());
    }
  }

  /** A client call interrupted (the task is being killed): the flag is restored and the step ends, never a retry. */
  private EmbeddingComputeTransientException interrupted(InterruptedException e) {
    Thread.currentThread().interrupt();
    return new EmbeddingComputeTransientException("Embedding compute for model " + model.getName() + " was interrupted", e);
  }

  /** The provider is unavailable: the step ends, never a retry. */
  private EmbeddingComputeTransientException providerUnavailable(EmbeddingProviderUnavailableException e) {
    return new EmbeddingComputeTransientException("EMBED-COMPUTE-PROVIDER-UNAVAILABLE model=" + model.getName()
        + ": the provider is unavailable", e);
  }

  /** Why {@code results} breaks the client contract for {@code expected} inputs, or null when it does not. */
  private String malformed(List<EmbeddingResult> results, int expected) {
    if (results == null || results.size() != expected) {
      return "malformed answer: expected " + expected + " results, got " + (results == null ? "null" : results.size());
    }
    for (EmbeddingResult result : results) {
      if (result == null || result.getStatus() == null) {
        return "malformed answer: a null result";
      }
      if (result.getStatus() == EmbeddingResult.Status.OK && result.getVector().length != model.getDimension()) {
        return "malformed answer: expected vectors of dimension " + model.getDimension() + ", got "
            + result.getVector().length;
      }
    }
    return null;
  }

  private EmbeddingInput toInput(Row row) {
    String modality = row.getString(modalityIndex);
    String sourcePathSha = row.getString(sourcePathShaIndex);
    long chunkId = row.getLong(chunkIdIndex);
    if (EmbeddingTableSchema.MODALITY_IMAGE.equals(modality)) {
      return storedImageInput(row, sourcePathSha, chunkId, modality);
    }
    return new EmbeddingInput(sourcePathSha, row.getLong(sourceModificationTimeIndex), row.getLong(sourceFileSizeIndex), modality, chunkId,
        row.isNullAt(textIndex) ? null : row.getString(textIndex));
  }

  /**
   * The input of an image row for a multimodal client: its {@code prepared_image}, opened through {@link #storedInputs}, and the
   * model input prepared from it lazily, through a seekable stream of the stored input, when the client asks for it.
   *
   * @throws EmbeddingComputePlatformException when the row has no {@code prepared_image} or this iterator has no stored-input reader:
   *     prep wrote every image row with one, so a missing one is a broken prep/compute invariant
   */
  private EmbeddingInput storedImageInput(Row row, String sourcePathSha, long chunkId, String modality) {
    if (storedInputs == null || row.isNullAt(preparedImageIndex)) {
      // the five key columns in key order, as EmbeddingInput#describeIdentity names them: two versions of one file stay apart
      throw new EmbeddingComputePlatformException("EMBED-COMPUTE-NO-STORED-IMAGE model=" + model.getName() + " source_path_sha="
          + sourcePathSha + " source_modification_time=" + row.get(sourceModificationTimeIndex) + " source_file_size="
          + row.get(sourceFileSizeIndex) + " modality=" + modality + " chunk_id=" + chunkId + ": an image row reached compute without a "
          + EmbeddingTableSchema.PREPARED_IMAGE + (storedInputs == null ? " reader" : ""));
    }
    Row stored = row.getStruct(preparedImageIndex);
    BlobBytesSource source = storedInputs;
    ImagePreparer preparer = imagePreparer;
    long modificationTime = row.getLong(sourceModificationTimeIndex);
    long fileSize = row.getLong(sourceFileSizeIndex);
    PreparedImageSupplier image = () -> preparer.prepare(
        () -> SeekableBlobImageInputStream.openStoredInput(() -> {
          source.checkVersion(stored, modificationTime, fileSize);
          return source.openSeekable(stored);
        }));
    return EmbeddingInput.storedImage(sourcePathSha, modificationTime, fileSize, modality, chunkId, () -> {
      source.checkVersion(stored, modificationTime, fileSize);
      return source.open(stored);
    }, image);
  }

  /**
   * The processed row: OK → the vector, the working error column NULL; PERMANENT → NULL vector, the error in the working error
   * column.
   * {@link #embedWithRetries} never leaves a row RETRYABLE.
   */
  private Row withOutcome(Row row, EmbeddingResult result) {
    Object[] values = new Object[outputWidth];
    for (int i = 0; i < outputWidth; i++) {
      values[i] = row.get(i);
    }
    // every prepared column is kept as read
    String error;
    switch (result.getStatus()) {
      case OK:
        error = null;
        counters.getRowsEmbedded().add(1);
        break;
      case PERMANENT_FAILURE:
        // a PERMANENT result always carries an error
        error = result.getError() == null || result.getError().isEmpty() ? "PERMANENT result without a message"
            : EmbeddingTableSchema.truncateError(result.getError());
        error = ComputeRound.rowError(error);
        counters.getRowsPermanent().add(1);
        break;
      default:
        throw new UnsupportedOperationException("Unknown embedding result status " + result.getStatus());
    }
    // the Row encoder accepts an array or a Scala Seq for ArrayType, not a java.util.List
    values[vectorIndex] = result.getStatus() == EmbeddingResult.Status.OK ? result.getVector() : null;
    values[errorIndex] = error;
    return RowFactory.create(values);
  }

  private static String describe(Exception failure) {
    return failure.getClass().getSimpleName() + ": " + failure.getMessage();
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      if (client != null) {
        try {
          client.close();
        } catch (Exception e) {
          LOG.warn("EMBED-COMPUTE-CLIENT-CLOSE-FAILED model={}", model.getName(), e);
        }
      }
    }
  }

  /** Returns one refusal marker, then ends this task normally without further requests. */
  private Iterator<Row> refusedTask(Row first, EmbeddingClientConfigException refusal) {
    refused = true;
    counters.getCallsRefused().add(1);
    LOG.warn("EMBED-COMPUTE-PROVIDER-REFUSED model={}: the task stops at the first permanent refusal; no task re-attempt",
        model.getName(), refusal);
    Object[] values = new Object[outputWidth];
    for (int i = 0; i < outputWidth; i++) {
      values[i] = first.get(i);
    }
    values[vectorIndex] = null;
    values[errorIndex] = EmbeddingTableSchema.truncateError(ComputeRound.REFUSED_PREFIX + refusal.getMessage());
    return Collections.singletonList(RowFactory.create(values)).iterator();
  }
}
