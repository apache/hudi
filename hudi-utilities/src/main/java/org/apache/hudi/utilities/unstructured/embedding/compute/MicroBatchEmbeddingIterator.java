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
  /** The wait before a row's second try, in ms; doubles per try up to {@link EmbeddingTableConfig#COMPUTE_RETRY_MAX_BACKOFF_MS}. */
  private final long backoffMs;
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
  private final EmbeddingClient client;
  private Iterator<Row> currentBatch = Collections.emptyIterator();
  private boolean closed;

  /** Creates the task's text embedding iterator. */
  MicroBatchEmbeddingIterator(Iterator<Row> rows, EmbeddingModelSpec model, TypedProperties props,
                              StructType schema, int batchSize, ComputeCounters counters) {
    this.model = model;
    this.batchSize = batchSize;
    EmbeddingTableConfig config = EmbeddingTableConfig.from(props);
    this.maxAttempts = config.computeMaxAttempts();
    this.backoffMs = config.computeRetryBackoffMs();
    this.counters = counters;
    this.outputWidth = schema.fields().length;
    this.vectorIndex = schema.fieldIndex(model.getColumn());
    this.errorIndex = outputWidth - 1;
    this.sourcePathShaIndex = schema.fieldIndex(EmbeddingTableSchema.SOURCE_PATH_SHA);
    this.sourceModificationTimeIndex = schema.fieldIndex(EmbeddingTableSchema.SOURCE_MODIFICATION_TIME);
    this.sourceFileSizeIndex = schema.fieldIndex(EmbeddingTableSchema.SOURCE_FILE_SIZE);
    this.chunkIdIndex = schema.fieldIndex(EmbeddingTableSchema.CHUNK_ID);
    this.modalityIndex = schema.fieldIndex(EmbeddingTableSchema.MODALITY);
    this.textIndex = schema.fieldIndex(EmbeddingTableSchema.PREPARED_TEXT_CHUNK);
    EmbeddingClient created;
    try {
      created = EmbeddingClientFactory.create(model, props);
    } catch (EmbeddingClientConfigException e) {
      throw e;
    } catch (Exception e) {
      // a client that cannot start fails the task
      throw new EmbeddingComputeTransientException("EMBED-COMPUTE-CLIENT-INIT-FAILED model=" + model.getName()
          + ": the embedding client could not start", e);
    }
    this.client = created;
    this.rows = rows;
    TaskContext taskContext = TaskContext.get();
    if (taskContext != null) {
      taskContext.addTaskCompletionListener((TaskCompletionListener) context -> close());
    }
  }

  @Override
  public boolean hasNext() {
    while (!currentBatch.hasNext() && rows.hasNext()) {
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

  /** Reads one micro batch and calls the model once for its inputs. */
  private Iterator<Row> embedNextBatch() {
    List<Row> batch = new ArrayList<>(batchSize);
    List<EmbeddingInput> inputs = new ArrayList<>(batchSize);
    while (batch.size() < batchSize && rows.hasNext()) {
      Row row = rows.next();
      batch.add(row);
      inputs.add(toInput(row));
    }
    List<EmbeddingResult> results = embedWithRetries(inputs);
    List<Row> processed = new ArrayList<>(batch.size());
    for (int i = 0; i < batch.size(); i++) {
      processed.add(withOutcome(batch.get(i), results.get(i)));
    }
    return processed.iterator();
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
        return results;
      }
      if (tries >= maxAttempts) {
        EmbeddingInput input = inputs.get(retryable.get(0));
        throw new EmbeddingComputeTransientException("EMBED-COMPUTE-RETRIES-EXHAUSTED model=" + model.getName() + ": "
            + retryable.size() + " input(s) still failing after " + tries + " tries (" + EmbeddingTableConfig.COMPUTE_MAX_ATTEMPTS.key()
            + "), first " + input.describeIdentity() + ": "
            + results.get(retryable.get(0)).getError());
      }
      sleep(backoff(tries));
      for (int i : retryable) {
        counters.getRowsRetried().add(1);
        results.set(i, embedAlone(inputs.get(i)));
      }
    }
  }

  /** The wait before try {@code tries + 1}: {@link #backoffMs} doubled per earlier retry, capped. */
  private long backoff(int tries) {
    double wait = backoffMs * Math.pow(2, tries - 1);
    return (long) Math.min(EmbeddingTableConfig.COMPUTE_RETRY_MAX_BACKOFF_MS, wait);
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
    return new EmbeddingInput(sourcePathSha, row.getLong(sourceModificationTimeIndex), row.getLong(sourceFileSizeIndex), modality, chunkId,
        row.isNullAt(textIndex) ? null : row.getString(textIndex));
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
}
