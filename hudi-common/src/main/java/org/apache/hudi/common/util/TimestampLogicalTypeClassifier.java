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

package org.apache.hudi.common.util;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

/**
 * Shared classifier for the timestamp logical-type drift introduced by Hudi 0.14.1 / 0.15.0 / 1.0.x.
 * It decides, from three signals about a long-backed column, what the correct target timestamp
 * logical type is, so a caller can suggest a value for
 * {@code hoodie.write.timestamp.logical.type.overrides}.
 *
 * <p>The three signals are: (1) the table schema logical type, (2) the base-file schema logical
 * type, and (3) the shape of the raw stored {@code long} values. Signal (1) must be resolved as of
 * the file's own commit instant, not the latest table schema, so that a file is classified within
 * its own era. The classification here is pure; the I/O that gathers the signals is caller-specific
 * (the OSS scanner reads through the storage abstraction, the data-plane tool reads parquet
 * directly), but both must share this logic so their verdicts cannot drift apart.
 *
 * <p><b>Status:</b> this is the verdict logic only. The inspection scanner that samples base files
 * and emits a ready-to-paste config value is not part of this repo yet, so an in-repo grep finds
 * only tests today. It lives here rather than in the tool so that every consumer shares one
 * definition of the verdict and they cannot drift apart.
 *
 * <p>Do not rely on the enums or method signatures here as stable public API: this is internal to
 * the timestamp-repair workflow.
 */
public class TimestampLogicalTypeClassifier {

  private TimestampLogicalTypeClassifier() {
  }

  // Plausibility windows: an epoch instant in 1990-01-01 .. 2100-01-01, interpreted as millis vs
  // micros. The two windows are ~1000x apart and do not overlap, so a single value fits at most one.
  private static final long PLAUSIBLE_MILLIS_MIN = 631152000000L;     // 1990-01-01
  private static final long PLAUSIBLE_MILLIS_MAX = 4102444800000L;    // 2100-01-01
  private static final long PLAUSIBLE_MICROS_MIN = 631152000000000L;  // 1990-01-01
  private static final long PLAUSIBLE_MICROS_MAX = 4102444800000000L; // 2100-01-01

  /** The timestamp logical type of a long-backed column, or NONE / UNKNOWN. */
  public enum LogicalTimestampType {
    NONE,
    TIMESTAMP_MICROS,
    TIMESTAMP_MILLIS,
    LOCAL_TIMESTAMP_MICROS,
    LOCAL_TIMESTAMP_MILLIS,
    UNKNOWN
  }

  /** The shape of a stored long value, judged against the plausibility windows. */
  public enum DataShape {
    MICROS,
    MILLIS,
    AMBIGUOUS,
    UNKNOWN
  }

  /** The per-column verdict. */
  public enum Bucket {
    /** No timestamp logical type and no timestamp-shaped data. Nothing to do, safe to upgrade. */
    UNAFFECTED,
    /** Table, file, and values agree at the same precision. Correct, though it may still need a
     * defensive pin if the ingestion source declares a different precision. */
    CORRECT,
    /**
     * Label says micros but the values are millis, or the symmetric inverse (label says millis but
     * values are micros): the 0.14.1 drift. Both directions map to the same repair action — pin the
     * field to whatever the values actually are — so they share this bucket. The observed
     * production case is label_micros/values_millis; the symmetric case is included to be safe.
     */
    LEGACY_0X_BUG,
    /** Bare long, but the values are timestamp-shaped: the 0.x local-timestamp logical-type loss. */
    DROPPED_LOGICAL_TYPE,
    /** The three signals disagree in some other way. */
    DIVERGENT,
    /** The value shape cannot be judged confidently (near-epoch, sentinels, zeros, negatives). */
    AMBIGUOUS
  }

  /** Classifies the logical type of a long-backed Avro field (the union is expected to be unwrapped). */
  public static LogicalTimestampType classifyAvroLogicalType(Schema longSchema) {
    LogicalType lt = longSchema.getLogicalType();
    if (lt == null) {
      return LogicalTimestampType.NONE;
    }
    if (lt instanceof LogicalTypes.TimestampMillis) {
      return LogicalTimestampType.TIMESTAMP_MILLIS;
    }
    if (lt instanceof LogicalTypes.TimestampMicros) {
      return LogicalTimestampType.TIMESTAMP_MICROS;
    }
    if (lt instanceof LogicalTypes.LocalTimestampMillis) {
      return LogicalTimestampType.LOCAL_TIMESTAMP_MILLIS;
    }
    if (lt instanceof LogicalTypes.LocalTimestampMicros) {
      return LogicalTimestampType.LOCAL_TIMESTAMP_MICROS;
    }
    return LogicalTimestampType.UNKNOWN;
  }

  /**
   * Judges a single raw long. Zeros, negatives, and sentinels (for example the year-9999 markers)
   * fall outside both plausibility windows and are reported UNKNOWN so they never drive a verdict.
   */
  public static DataShape classifyValueShape(long value) {
    if (value <= 0) {
      return DataShape.UNKNOWN;
    }
    boolean millisPlausible = value >= PLAUSIBLE_MILLIS_MIN && value < PLAUSIBLE_MILLIS_MAX;
    boolean microsPlausible = value >= PLAUSIBLE_MICROS_MIN && value < PLAUSIBLE_MICROS_MAX;
    if (millisPlausible && !microsPlausible) {
      return DataShape.MILLIS;
    }
    if (microsPlausible && !millisPlausible) {
      return DataShape.MICROS;
    }
    if (millisPlausible) {
      // The windows do not overlap, so this is unreachable for a single value; kept for safety.
      return DataShape.AMBIGUOUS;
    }
    return DataShape.UNKNOWN;
  }

  /** Folds one sampled value's shape into the running per-column shape across many samples/files. */
  public static DataShape reduceShape(DataShape acc, DataShape sample) {
    if (acc == null || acc == DataShape.UNKNOWN) {
      return sample;
    }
    if (sample == DataShape.UNKNOWN || acc == sample) {
      return acc;
    }
    return DataShape.AMBIGUOUS;
  }

  /**
   * Reconciles the three signals into a verdict. {@code tableType} must be the table logical type as
   * of the inspected file's commit instant.
   */
  public static Bucket classifyBucket(LogicalTimestampType tableType, LogicalTimestampType fileType, DataShape shape) {
    boolean noLogicalType = tableType == LogicalTimestampType.NONE && fileType == LogicalTimestampType.NONE;
    if (shape == DataShape.UNKNOWN) {
      // Nothing timestamp-shaped was seen. Bare-long-everywhere is a plain non-timestamp column;
      // anything else cannot be judged without a clearer value signal.
      return noLogicalType ? Bucket.UNAFFECTED : Bucket.AMBIGUOUS;
    }
    if (shape == DataShape.AMBIGUOUS) {
      return Bucket.AMBIGUOUS;
    }
    if (noLogicalType) {
      // Bare long with timestamp-shaped data: 0.x dropped the local-timestamp logical type.
      return Bucket.DROPPED_LOGICAL_TYPE;
    }
    boolean tableMicros = tableType == LogicalTimestampType.TIMESTAMP_MICROS || tableType == LogicalTimestampType.LOCAL_TIMESTAMP_MICROS;
    boolean tableMillis = tableType == LogicalTimestampType.TIMESTAMP_MILLIS || tableType == LogicalTimestampType.LOCAL_TIMESTAMP_MILLIS;
    boolean fileMicros = fileType == LogicalTimestampType.TIMESTAMP_MICROS || fileType == LogicalTimestampType.LOCAL_TIMESTAMP_MICROS;
    boolean fileMillis = fileType == LogicalTimestampType.TIMESTAMP_MILLIS || fileType == LogicalTimestampType.LOCAL_TIMESTAMP_MILLIS;
    if (tableMicros && fileMicros && shape == DataShape.MILLIS) {
      return Bucket.LEGACY_0X_BUG;
    }
    if (tableMillis && fileMillis && shape == DataShape.MICROS) {
      return Bucket.LEGACY_0X_BUG;
    }
    if (tableMicros && fileMicros && shape == DataShape.MICROS) {
      // All three agree on micros. Genuinely correct; a source-declared millis is indistinguishable
      // here and maps to the same action (keep micros), so it is not a separate bucket.
      return Bucket.CORRECT;
    }
    if (tableMillis && fileMillis && shape == DataShape.MILLIS) {
      return Bucket.CORRECT;
    }
    // Reached when the table and file logical types disagree (for example table_micros +
    // file_millis) or the surviving cases where the three signals do not line up to CORRECT
    // or LEGACY_0X_BUG. The operator must decide the correct override; no auto-suggestion.
    return Bucket.DIVERGENT;
  }

  /**
   * The suggested {@code hoodie.write.timestamp.logical.type.overrides} token for a column, or empty
   * when the operator must decide (ambiguous / divergent) or nothing is needed (unaffected).
   * {@code local} selects the local-timestamp variant, carried from the table/file logical type.
   */
  public static Option<String> suggestedOverrideToken(Bucket bucket, DataShape shape, boolean local) {
    switch (bucket) {
      case CORRECT:
        // Pin to the current precision so a differently-declared source cannot flip it.
        return Option.of(token(shape, local));
      case LEGACY_0X_BUG:
      case DROPPED_LOGICAL_TYPE:
        // Repair to what the values actually are.
        return Option.of(token(shape, local));
      default:
        return Option.empty();
    }
  }

  private static String token(DataShape shape, boolean local) {
    String precision = shape == DataShape.MILLIS ? "millis" : "micros";
    return (local ? "local-timestamp-" : "timestamp-") + precision;
  }
}
