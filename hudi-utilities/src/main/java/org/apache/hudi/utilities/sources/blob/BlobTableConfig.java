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

package org.apache.hudi.utilities.sources.blob;

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.io.hadoop.HoodieBaseParquetWriter;
import org.apache.hudi.utilities.unstructured.storage.ParquetInlineCap;

import javax.annotation.concurrent.Immutable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Configuration for selecting object announcements and storing their bytes in a blob table. The unstructured data source owns
 * the upstream checkpoint, instant and byte budgets.
 * Inline values use the configured threshold or its default. The blob job's Parquet codec and bloom filter are set by its
 * source class when the job leaves them unset (uncompressed, no bloom filter); any value the job sets is kept. The effective
 * threshold, codec and bloom switch are pinned in the table's properties when it is created ({@link BlobPhysicalPins}), and a
 * job whose effective values differ is refused.
 */
@Immutable
@ConfigClassProperty(name = "Blob Table Source Configs",
    groupName = ConfigGroups.Names.HUDI_STREAMER,
    subGroupName = ConfigGroups.SubGroupNames.DELTA_STREAMER_SOURCE,
    description = "Configurations controlling how unstructured files are ingested into a Hudi blob table.")
public class BlobTableConfig extends HoodieConfig {

  private static final String SINCE_VERSION = "1.3.0";

  public static final String PREFIX = "hoodie.blob.table.";

  /**
   * Small blob threshold: a file of at most this many bytes is stored INLINE in the data file. A larger one is not copied: it
   * stays where it is, and its row is an OUT_OF_LINE blob with an unmanaged reference (managed=false) to the object's own URI, as
   * listed, without offset or length, so readers read the whole current file. The costs of referencing in place are listed once, in this
   * key's documentation. The declared 1 MiB default is effective on Parquet 1.14+ (the Spark 4 builds). On Spark 3.5's older
   * Parquet, which cannot disable statistics per column, the effective default is 10 KiB and configured values are capped at
   * 80 KiB ({@link ParquetInlineCap}). The effective value is pinned in the table's properties when it is created
   * ({@link BlobPhysicalPins#INLINE_MAX_BYTES}): a job whose effective value differs (for example a Spark 3.5-created table,
   * 10 KiB, written by a Spark 4 job that leaves this key unset, 1 MiB) is refused before anything is written.
   * Bounded above by {@link BlobInlineCeiling}. Read it with {@link #inlineMaxBytes(TypedProperties)}.
   */
  public static final ConfigProperty<Long> INLINE_MAX_BYTES = ConfigProperty
      .key(PREFIX + "inline.max.bytes")
      .defaultValue(1024L * 1024L)
      .markAdvanced()
      .sinceVersion(SINCE_VERSION)
      .withDocumentation("Files with size <= this many bytes are stored inline in the blob column. Larger files are not copied: "
          + "they stay where they are and are referenced out of line (managed=false, external_path = the object's own URI as "
          + "listed, offset and length null: the whole current file). A later overwrite or delete of a referenced source "
          + "object is not frozen (the row keeps the ingested size and modification time). Once a large object is overwritten, "
          + "its version rows reference the same file: every version row reads its full current content. Readers of the table need read access to the "
          + "source bucket, and table cleaning never touches source objects. The declared default is 1 MiB (1048576 bytes), "
          + "effective on Parquet 1.14+ (the Spark 4 builds). On Spark 3.5's Parquet older than 1.14, which cannot turn off min/max "
          + "statistics for one column, the effective default is 10 KiB (10240 bytes); a configured value above 80 KiB (81920 bytes) "
          + "is logged at WARN and capped at 81920. The effective threshold is pinned when the table is created; a job whose "
          + "effective threshold differs is refused before anything is written. A Spark 4 job writing a table created with "
          + "Spark 3.5's default must set this key to the table's stored threshold. "
          + "On Parquet without per-column statistics, blob-table writes require 128 times this effective threshold <= "
          + "hoodie.parquet.block.size, which must be positive. Each completed row group's retained blob min/max payload "
          + "(2 times the effective threshold) has a budget of 1/64 of the effective row-group target. The job checks its "
          + "settings before source processing; the writer checks the final caller/delegate target without letting a delegate "
          + "lower the prepared rows' threshold. This per-group configuration budget does not bound total heap across "
          + "accumulated groups or historical inline values. The legacy defaults (10 KiB inline, 120 MiB row groups) pass. "
          + "A value above the per-task inline ceiling ("
          + PREFIX + "inline.max.heap.fraction x the per-task heap share) is refused at the job's first round, before the round "
          + "reads the events table's rows or starts its instant, unless "
          + PREFIX + "inline.ceiling.enforce is false.");

  /**
   * The inline threshold a blob round uses: {@link #INLINE_MAX_BYTES} as the job sets it, or its default, on a Parquet with the
   * per-column statistics switch; without it 10 KiB by default and at most 80 KiB ({@link ParquetInlineCap}). A job writing an
   * existing table must match its pinned effective threshold ({@link BlobPhysicalPins#INLINE_MAX_BYTES}).
   */
  public static long inlineMaxBytes(TypedProperties props) {
    return inlineMaxBytes(props, HoodieBaseParquetWriter.supportsColumnStatisticsSwitch());
  }

  /**
   * As {@link #inlineMaxBytes(TypedProperties)}, for a Parquet with or without the per-column statistics switch.
   *
   * @throws IllegalArgumentException naming the key when the configured value is not in [0, 2^31-9]
   */
  public static long inlineMaxBytes(TypedProperties props, boolean statisticsSwitch) {
    Long configured = ConfigUtils.containsConfigProperty(props, INLINE_MAX_BYTES)
        ? ConfigUtils.getLongWithAltKeys(props, INLINE_MAX_BYTES) : null;
    if (configured != null && (configured < 0 || configured > Integer.MAX_VALUE - 8)) {
      throw new IllegalArgumentException(INLINE_MAX_BYTES.key() + " must be in [0, 2^31-9]: " + configured);
    }
    return ParquetInlineCap.effective(INLINE_MAX_BYTES.key(), configured, INLINE_MAX_BYTES.defaultValue(), statisticsSwitch);
  }

  /**
   * Fraction of the per-task heap share that the inline threshold may reach (see {@link BlobInlineCeiling}). The default 1/64 keeps
   * the modeled per-task memory of a blob round (about 112 MiB that does not depend on the threshold plus 50 times the threshold for
   * two open file writers) under the per-task share once that share is at least 511 MiB.
   */
  public static final ConfigProperty<Double> INLINE_MAX_HEAP_FRACTION = ConfigProperty
      .key(PREFIX + "inline.max.heap.fraction")
      .defaultValue(1.0 / 64)
      .markAdvanced()
      .sinceVersion(SINCE_VERSION)
      .withDocumentation("Fraction in (0, 1] of the per-task heap share up to which " + PREFIX + "inline.max.bytes may be set. "
          + "Per-task share = (executor heap - Spark's reserved system memory, 300 MiB outside tests) / task slots, task slots = "
          + "executor cores / spark.task.cpus; in local mode the executor heap is the driver JVM's maximum heap and the cores are "
          + "the master's thread count, otherwise spark.executor.memory and spark.executor.cores (default 1: set it when executors "
          + "run more than one task). A threshold above fraction x share is refused at the job's first round and again in a task "
          + "before its first inline read, naming both numbers. The default 1/64 keeps the modeled per-task memory of a round "
          + "(about 112 MiB independent of the threshold plus 50 x the threshold) under the share for shares of 511 MiB or more. "
          + "The default 1 MiB threshold needs a share of at least 64 MiB (heap >= 300 MiB + task slots x 64 MiB): below that the "
          + "defaults refuse each other and " + PREFIX + "inline.max.bytes must be lowered. local[*] counts every core of the "
          + "machine, so a small heap on a many-core host is such a case; use local[N].");

  /**
   * The only way past the inline ceiling: with it off a threshold above the ceiling is logged at WARN and used as configured.
   */
  public static final ConfigProperty<Boolean> INLINE_CEILING_ENFORCE = ConfigProperty
      .key(PREFIX + "inline.ceiling.enforce")
      .defaultValue(true)
      .markAdvanced()
      .sinceVersion(SINCE_VERSION)
      .withDocumentation("When true (default), an " + PREFIX + "inline.max.bytes above the per-task inline ceiling ("
          + PREFIX + "inline.max.heap.fraction x the per-task heap share) fails the job before any file is read. When false, "
          + "it is only logged at WARN and the threshold is used as configured: the blob job's memory is then no longer "
          + "hard capped, and a file up to the threshold can exhaust a task's heap.");

  /**
   * Extensions (without the dot) of the object keys that are ingested, matched case-sensitively; every other announced object
   * is not ingested (it gets no row at all).
   */
  public static final ConfigProperty<String> FILE_EXTENSIONS = ConfigProperty
      .key(PREFIX + "file.extensions")
      .defaultValue("pdf,png,jpg,jpeg,gif,bmp,tif,tiff,mp4,mov,avi,mkv,webm")
      .sinceVersion(SINCE_VERSION)
      .withDocumentation("Comma-separated file extensions (without the dot) of the object keys to ingest: pdf, image and "
          + "video files by default. Matched case-sensitively against the end of the key, dot included: a key ending in "
          + ".JPG is not ingested. The list may only narrow the default: an empty list, or one naming any other extension, is "
          + "refused when the source starts.");

  /** The configured extensions, trimmed, with blanks dropped and case kept. */
  public static List<String> fileExtensions(TypedProperties props) {
    return splitList(ConfigUtils.getStringWithAltKeys(props, FILE_EXTENSIONS, true));
  }

  private static List<String> splitList(String value) {
    return value == null ? Collections.emptyList() : Arrays.stream(value.split(","))
        .map(String::trim).filter(entry -> !entry.isEmpty()).collect(Collectors.toList());
  }

}
