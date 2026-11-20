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

package org.apache.hudi.utilities.unstructured.embedding.input;

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.io.hadoop.HoodieBaseParquetWriter;
import org.apache.hudi.utilities.sources.blob.BlobInlineCeiling;
import org.apache.hudi.utilities.sources.blob.BlobPhysicalPins;
import org.apache.hudi.utilities.unstructured.storage.InlineCeiling;
import org.apache.hudi.utilities.unstructured.storage.ParquetInlineCap;

import org.apache.spark.SparkConf;

import java.util.Properties;

/**
 * Keys of the embedding table's input store (its inline threshold). They live under
 * {@code hoodie.unstructured.embedding.input.} and are read from the job's {@link TypedProperties}.
 *
 * <p>The inline threshold is bounded by the blob table's per-task ceiling rule ({@link BlobInlineCeiling}) with this store's own
 * fraction: a threshold above {@code fraction x per-task heap share} is refused, never clamped, so inline versus out of line never
 * depends on the heap of the attempt that stores an input.
 */
@ConfigClassProperty(name = "Embedding Input Store Configs",
    groupName = ConfigGroups.Names.HUDI_STREAMER,
    subGroupName = ConfigGroups.SubGroupNames.DELTA_STREAMER_SOURCE,
    description = "Configurations controlling inline storage and the per-task heap ceiling for prepared embedding inputs.")
public final class EmbeddingInputConfig {

  /** Prefix of every input-store key. */
  public static final String PREFIX = "hoodie.unstructured.embedding.input.";
  /**
   * A stored input of at most this many bytes is INLINE in the row; a larger one is a managed copy. The declared default is
   * 1 MiB, effective on Parquet 1.14+ (the Spark 4 builds); Spark 3.5's older Parquet uses 80 KiB by default and as its cap.
   * The effective value is pinned in the embedding table's properties when it is created ({@link #TABLE_INLINE_MAX_BYTES}): a
   * job that stores inputs (images, video frames) and whose effective value differs (for example a Spark 3.5-created table,
   * 80 KiB, written by a Spark 4 job that leaves this key unset, 1 MiB) is refused before anything is written.
   */
  public static final ConfigProperty<Long> INLINE_MAX_BYTES = ConfigProperty
      .key(PREFIX + "inline.max.bytes")
      .defaultValue(1048576L)
      .markAdvanced()
      .sinceVersion("1.3.0")
      .withDocumentation("A stored input of at most this many bytes is INLINE in the row; a larger one is a managed copy. The "
          + "declared default is 1 MiB (1048576 bytes), effective on Parquet 1.14+ (the Spark 4 builds). On Spark 3.5's Parquet older "
          + "than 1.14, which cannot turn off min/max statistics for one column, the effective default is 80 KiB (81920 bytes), also "
          + "the cap: a configured value above it is logged at WARN and capped at 81920. The effective value is pinned in the "
          + "table's properties when the table is created, and a job that stores inputs (images, video frames) and whose effective "
          + "value differs is refused before anything is written (for example a table created on Spark 3.5 and written by a "
          + "Spark 4 job that leaves this key unset).");
  /**
   * The input-store inline threshold in bytes the embedding table was created with ({@link #inlineMaxBytes(TypedProperties)}),
   * written by every job that creates the table and checked ({@link #checkTable}) for a job whose preprocessors store inputs.
   */
  public static final ConfigProperty<String> TABLE_INLINE_MAX_BYTES = ConfigProperty
      .key("hoodie.table.embedding.input.inline.max.bytes").noDefaultValue();
  /**
   * Declared default of 1 MiB, used with Parquet's per-column statistics switch. Without that switch the effective default
   * and cap are 80 KiB ({@link #inlineMaxBytes(TypedProperties)}); the table pins the value it was created with.
   */
  public static final long DEFAULT_INLINE_MAX_BYTES = 1024L * 1024L;
  /** Fraction in (0, 1] of the per-task heap share up to which {@link #INLINE_MAX_BYTES} may be set. */
  public static final ConfigProperty<Double> INLINE_MAX_HEAP_FRACTION = ConfigProperty
      .key(PREFIX + "inline.max.heap.fraction")
      .defaultValue(0.015625)
      .markAdvanced()
      .sinceVersion("1.3.0")
      .withDocumentation("Fraction in (0, 1] of the per-task heap share up to which "
          + "hoodie.unstructured.embedding.input.inline.max.bytes may be set.");
  /** 1/64, the blob table's fraction: the 1 MiB default then needs a per-task heap share of at least 64 MiB. */
  public static final double DEFAULT_INLINE_MAX_HEAP_FRACTION = 1.0 / 64;

  /** Largest threshold a byte array (and so an inline value) can hold. */
  private static final long MAX_INLINE_MAX_BYTES = Integer.MAX_VALUE - 8L;

  private EmbeddingInputConfig() {
  }

  /**
   * The inline threshold a writer uses: {@link #INLINE_MAX_BYTES} as the job sets it, or {@link #DEFAULT_INLINE_MAX_BYTES}, on a
   * Parquet with the per-column statistics switch; without it at most 80 KiB, and 80 KiB by default ({@link ParquetInlineCap}).
   * The table pins the value it was created with ({@link #TABLE_INLINE_MAX_BYTES}), so a writer whose value differs is refused.
   *
   * @throws IllegalArgumentException naming the key when it is not a number in [0, 2^31-9]
   */
  public static long inlineMaxBytes(TypedProperties props) {
    return inlineMaxBytes(props, HoodieBaseParquetWriter.supportsColumnStatisticsSwitch());
  }

  /**
   * As {@link #inlineMaxBytes(TypedProperties)}, for a Parquet with or without the per-column statistics switch.
   *
   * @throws IllegalArgumentException naming the key when it is not a number in [0, 2^31-9]
   */
  public static long inlineMaxBytes(TypedProperties props, boolean statisticsSwitch) {
    String raw = ConfigUtils.getStringWithAltKeys(props, INLINE_MAX_BYTES, null);
    if (raw == null) {
      return ParquetInlineCap.effective(INLINE_MAX_BYTES.key(), null, DEFAULT_INLINE_MAX_BYTES, ParquetInlineCap.MAX_BYTES,
          statisticsSwitch);
    }
    long value;
    try {
      value = Long.parseLong(raw.trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(INLINE_MAX_BYTES.key() + " must be a number of bytes, got '" + raw + "'", e);
    }
    if (value < 0 || value > MAX_INLINE_MAX_BYTES) {
      throw new IllegalArgumentException(INLINE_MAX_BYTES.key() + " must be in [0, " + MAX_INLINE_MAX_BYTES + "]: " + value);
    }
    return ParquetInlineCap.effective(INLINE_MAX_BYTES.key(), value, DEFAULT_INLINE_MAX_BYTES, ParquetInlineCap.MAX_BYTES,
        statisticsSwitch);
  }

  /**
   * Refuses a table at {@code basePath} that does not store {@code inlineMaxBytes}, this job's effective threshold, as its
   * {@link #TABLE_INLINE_MAX_BYTES}, naming the setting of {@link #INLINE_MAX_BYTES} that matches the table, as the blob table's
   * refusal does ({@link BlobPhysicalPins#thresholdHint}); a table that stores none was created by an earlier build.
   *
   * @throws IllegalArgumentException naming {@code sourceName}, the table and both values
   */
  public static void checkTable(String sourceName, String basePath, Properties table, String inlineMaxBytes) {
    String stored = table.getProperty(TABLE_INLINE_MAX_BYTES.key());
    if (!inlineMaxBytes.equals(stored)) {
      throw new IllegalArgumentException(sourceName + " requires the table's " + TABLE_INLINE_MAX_BYTES.key() + "=" + inlineMaxBytes
          + " (the table stores " + (stored == null ? "none" : stored)
          + BlobPhysicalPins.thresholdHint(stored, INLINE_MAX_BYTES.key(), EmbeddingInputConfig::inlineMaxBytes)
          + "): the table at " + basePath + " was written with another input-store threshold; nothing was written");
    }
  }

  /**
   * The configured ceiling fraction.
   *
   * @throws IllegalArgumentException naming the key when it is not a number in (0, 1]
   */
  public static double inlineMaxHeapFraction(TypedProperties props) {
    String raw = ConfigUtils.getStringWithAltKeys(props, INLINE_MAX_HEAP_FRACTION, String.valueOf(DEFAULT_INLINE_MAX_HEAP_FRACTION));
    double fraction;
    try {
      fraction = Double.parseDouble(raw.trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(INLINE_MAX_HEAP_FRACTION.key() + " must be a fraction in (0, 1], got '" + raw + "'", e);
    }
    if (!(fraction > 0 && fraction <= 1)) {
      throw new IllegalArgumentException(INLINE_MAX_HEAP_FRACTION.key() + " must be in (0, 1]: " + fraction);
    }
    return fraction;
  }

  /**
   * Refuses the job's inline threshold when it is above the ceiling the driver computes from {@code conf} at job start
   * ({@link BlobInlineCeiling#onDriver}).
   *
   * @throws HoodieValidationException when the threshold is above the ceiling
   */
  public static void checkOnDriver(TypedProperties props, SparkConf conf) {
    checkCeiling(inlineMaxBytes(props), BlobInlineCeiling.onDriver(conf, inlineMaxHeapFraction(props)), "on the driver at job start");
  }

  /**
   * Refuses {@code inlineMaxBytes} above {@code ceiling} with the shared refusal ({@link InlineCeiling#check}) under this
   * store's keys; there is no switch that turns the refusal into a clamp or a warning.
   *
   * @param where where the check runs, for the message
   * @throws HoodieValidationException when the threshold is above the ceiling
   */
  public static void checkCeiling(long inlineMaxBytes, BlobInlineCeiling ceiling, String where) {
    new InlineCeiling(ceiling.getFraction(), ceiling.getExecutorHeapBytes(), ceiling.getExecutorHeapSource(),
        ceiling.getReservedBytes(), ceiling.getExecutorCores(), ceiling.getTaskCpus())
        .check(inlineMaxBytes, where, INLINE_MAX_BYTES.key(), INLINE_MAX_HEAP_FRACTION.key());
  }
}
