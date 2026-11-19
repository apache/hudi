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

import org.apache.hudi.utilities.unstructured.SourceCauses;
import org.apache.hudi.utilities.unstructured.embedding.source.PrepPlatformException;
import org.apache.hudi.utilities.unstructured.embedding.source.PreprocessorConfigurationException;
import org.apache.hudi.utilities.unstructured.embedding.source.SourceFailures;
import org.apache.hudi.utilities.unstructured.embedding.text.TokenizerException;

import org.apache.spark.TaskContext;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.function.Function;

/**
 * Tells a failure that must fail the prep round from a file's own bad input. A preprocessor or decoder may wrap, or add
 * as suppressed, what it met while reading a file; whatever the wrapping, the round fails when the cause or suppressed chain
 * holds:
 * <ul>
 *   <li>a {@link SourceReadException}: a storage failure of a source a later run can still read (transient, after its in-commit
 *       retries, or a 403/KMS denial);</li>
 *   <li>a {@link PreprocessorConfigurationException} or a {@link PrepPlatformException}: the job's configuration or its platform;</li>
 *   <li>a {@link TokenizerException} of kind {@link TokenizerException.Kind#RETRYABLE}: the tokenizer's runtime failed after its
 *       own bounded retries, the text itself was never judged;</li>
 *   <li>a JVM failure a decoder wrapped or suppressed (a {@link VirtualMachineError}, e.g. an OutOfMemoryError, or a
 *       {@link LinkageError}): it says nothing about the file, and fails the round as a {@link PrepPlatformException}. Not a
 *       {@link StackOverflowError}: the file's own nesting depth causes it, the same on every attempt, so it stays the file's (or
 *       a PDF page's) bad input.</li>
 * </ul>
 * Any other failure of a file is its bad input: it is recorded in the job's error table and skipped.
 */
public final class PrepFailures {

  private PrepFailures() {
  }

  /**
   * The first throwable of {@code failure}'s cause and suppressed chain that fails the round (for a JVM failure, a
   * {@link PrepPlatformException} carrying {@code failure}), or null when there is none.
   */
  public static RuntimeException roundFailure(Throwable failure) {
    return findFailure(failure, cause -> {
      if (failsTheRound(cause)) {
        return (RuntimeException) cause;
      }
      if (SourceFailures.isJvmFailure(cause)) {
        return new PrepPlatformException("the JVM failed while the file was read, not the file: " + cause.getClass().getSimpleName()
            + ": " + cause.getMessage() + ", under " + failure.getClass().getSimpleName() + ": " + failure.getMessage(), failure);
      }
      return null;
    });
  }

  /** First matching failure in the cause/suppressed graph, with identity-based cycle protection and no eager list. */
  static <T extends Throwable> T findFailure(Throwable failure, Function<Throwable, T> classify) {
    Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    Deque<Throwable> pending = new ArrayDeque<>();
    pending.add(failure);
    while (!pending.isEmpty()) {
      Throwable cause = pending.removeFirst();
      if (!visited.add(cause)) {
        continue;
      }
      T found = classify.apply(cause);
      if (found != null) {
        return found;
      }
      if (cause.getCause() != null) {
        pending.addLast(cause.getCause());
      }
      Collections.addAll(pending, cause.getSuppressed());
    }
    return null;
  }

  /**
   * Rethrows {@code failure} when its chain holds a failure that fails the round ({@link #roundFailure}): as it is when it is that
   * failure, else as a {@link SourceReadException}, {@link PreprocessorConfigurationException} or {@link PrepPlatformException}
   * (also for a retryable {@link TokenizerException}) carrying {@code failure}, so the message names what wrapped it and the
   * failure found stays in the cause chain. Returns normally for a file's bad input.
   */
  public static void rethrowRoundFailure(Throwable failure) {
    RuntimeException found = roundFailure(failure);
    if (found == null) {
      return;
    }
    if (found == failure) {
      throw found;
    }
    String message = failure.getClass().getSimpleName() + ": " + failure.getMessage();
    if (found instanceof SourceReadException) {
      throw new SourceReadException("Failed to read the blob source: " + message, failure);
    }
    if (found instanceof PreprocessorConfigurationException) {
      throw new PreprocessorConfigurationException(message, failure);
    }
    throw new PrepPlatformException(message, failure);
  }

  /** Whether the thread, Spark task or cause chain reports an interruption, including libraries that clear the flag. */
  public static boolean isInterrupted(Throwable failure) {
    TaskContext task = TaskContext.get();
    if (Thread.currentThread().isInterrupted() || (task != null && task.isInterrupted())) {
      return true;
    }
    for (Throwable cause : SourceCauses.getCauses(failure)) {
      if (cause instanceof java.io.InterruptedIOException || cause instanceof InterruptedException
          || cause instanceof java.nio.channels.ClosedByInterruptException) {
        return true;
      }
    }
    return false;
  }

  private static boolean failsTheRound(Throwable cause) {
    return cause instanceof SourceReadException
        || cause instanceof PreprocessorConfigurationException
        || cause instanceof PrepPlatformException
        || (cause instanceof TokenizerException && ((TokenizerException) cause).kind() == TokenizerException.Kind.RETRYABLE);
  }
}
