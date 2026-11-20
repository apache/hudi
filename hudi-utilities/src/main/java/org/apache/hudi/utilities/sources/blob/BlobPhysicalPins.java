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

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.ConfigUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.function.ToLongFunction;

/**
 * The blob table's persisted-layout settings that are job configuration, pinned in the table's properties when it is created:
 * the effective inline threshold (which differs by Parquet version when the job leaves it unset, see
 * {@link BlobTableConfig#INLINE_MAX_BYTES}), the Parquet compression codec and the bloom-filter switch. A blob job whose
 * effective values differ from the table's, or a table that does not store them, is refused before anything is written. The
 * per-column statistics and dictionary switches follow the Parquet version, not the job, and are not pinned.
 *
 * <p>The pins are checked by the blob ingestion job's contract only. A table service run as a separate job (compaction,
 * clustering) is not checked against the codec and bloom pins, so it must carry the ingestion job's settings itself
 * ({@link org.apache.hudi.utilities.sources.unstructured.UnstructuredMode}); one that does not rewrites the table's files with
 * its own codec and bloom filter.
 */
public final class BlobPhysicalPins {

  /** The inline threshold in bytes the table was created with ({@link BlobTableConfig#inlineMaxBytes(TypedProperties)}). */
  public static final ConfigProperty<String> INLINE_MAX_BYTES = ConfigProperty
      .key("hoodie.table.blob.inline.max.bytes").noDefaultValue();
  /** The Parquet compression codec the table's files are written with, lower case. */
  public static final ConfigProperty<String> PARQUET_COMPRESSION_CODEC = ConfigProperty
      .key("hoodie.table.blob.parquet.compression.codec").noDefaultValue();
  /** Whether the table's files are written with a bloom filter, {@code true} or {@code false}. */
  public static final ConfigProperty<String> PARQUET_BLOOM_FILTER_ENABLED = ConfigProperty
      .key("hoodie.table.blob.parquet.bloom.filter.enabled").noDefaultValue();
  /** The blob job's codec when it sets none (or a blank value). */
  public static final String DEFAULT_CODEC = "uncompressed";
  /** The blob job's bloom-filter switch when it sets none (or a blank value). */
  public static final String DEFAULT_BLOOM_FILTER_ENABLED = "false";
  /** The hint for a pin the table does not store: the table predates the pin, and no setting matches it. */
  public static final String CREATED_BY_AN_EARLIER_BUILD = "; the table was created by an earlier build that does not store it: "
      + "write this job to a new table";

  private BlobPhysicalPins() {
  }

  /** The pins of the job of {@code props}, by table property, as the job resolves them. */
  public static Map<String, String> of(TypedProperties props) {
    Map<String, String> pins = new LinkedHashMap<>();
    pins.put(INLINE_MAX_BYTES.key(), Long.toString(BlobTableConfig.inlineMaxBytes(props)));
    pins.put(PARQUET_COMPRESSION_CODEC.key(),
        configured(props, HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME, DEFAULT_CODEC).toLowerCase(Locale.ROOT));
    pins.put(PARQUET_BLOOM_FILTER_ENABLED.key(), Boolean.toString(Boolean.parseBoolean(
        configured(props, HoodieStorageConfig.PARQUET_WITH_BLOOM_FILTER_ENABLED, DEFAULT_BLOOM_FILTER_ENABLED))));
    return Collections.unmodifiableMap(pins);
  }

  /**
   * Refuses a table at {@code basePath} that does not store {@code pins} exactly, naming for each differing pin the job setting
   * that would match the table, or, for a pin the table does not store, that an earlier build created it and a new table is
   * needed.
   *
   * @throws IllegalArgumentException naming {@code sourceName}, the table and every differing pin
   */
  public static void check(String sourceName, String basePath, Properties table, Map<String, String> pins) {
    List<String> mismatches = new ArrayList<>();
    pins.forEach((key, value) -> {
      String stored = table.getProperty(key);
      if (!value.equals(stored)) {
        mismatches.add(key + "=" + value + " (the table stores " + (stored == null ? "none" : stored) + hint(key, stored) + ")");
      }
    });
    if (!mismatches.isEmpty()) {
      throw new IllegalArgumentException(sourceName + " requires the table's " + String.join(", ", mismatches)
          + ": the table at " + basePath + " was written with other physical settings; nothing was written");
    }
  }

  /**
   * How a job can match the table's {@code stored} value of {@code key}: the setting to set ({@link #thresholdHint} for the inline
   * threshold), or, when the table stores no value, that an earlier build created it ({@link #CREATED_BY_AN_EARLIER_BUILD}).
   */
  private static String hint(String key, String stored) {
    if (INLINE_MAX_BYTES.key().equals(key)) {
      return thresholdHint(stored, BlobTableConfig.INLINE_MAX_BYTES.key(), BlobTableConfig::inlineMaxBytes);
    }
    return stored == null ? CREATED_BY_AN_EARLIER_BUILD : "; set " + jobSetting(key) + "=" + stored + " to match it";
  }

  /**
   * How a job can match a table that stores {@code stored} as an inline threshold the job resolves from {@code jobSetting} with
   * {@code threshold}: the setting to set; for a threshold this Parquet cannot write (above its cap, see
   * {@link BlobTableConfig#INLINE_MAX_BYTES}), that a build with Parquet 1.14 or later is needed; for a table that stores none,
   * that an earlier build created it ({@link #CREATED_BY_AN_EARLIER_BUILD}). Text to append after the stored value.
   */
  public static String thresholdHint(String stored, String jobSetting, ToLongFunction<TypedProperties> threshold) {
    if (stored == null) {
      return CREATED_BY_AN_EARLIER_BUILD;
    }
    TypedProperties matching = new TypedProperties();
    matching.setProperty(jobSetting, stored);
    boolean reachable;
    try {
      reachable = Long.toString(threshold.applyAsLong(matching)).equals(stored);
    } catch (IllegalArgumentException invalid) {
      reachable = false;
    }
    if (!reachable) {
      return "; this build's Parquet cannot write that threshold: use a build with Parquet 1.14 or later";
    }
    return "; set " + jobSetting + "=" + stored + " to match it";
  }

  /** The job setting that decides the codec or bloom pin {@code key}. */
  private static String jobSetting(String key) {
    if (PARQUET_COMPRESSION_CODEC.key().equals(key)) {
      return HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME.key();
    }
    return HoodieStorageConfig.PARQUET_WITH_BLOOM_FILTER_ENABLED.key();
  }

  private static String configured(TypedProperties props, ConfigProperty<?> key, String defaultValue) {
    String value = ConfigUtils.getRawValueWithAltKeys(props, key).map(Object::toString).map(String::trim).orElse("");
    return value.isEmpty() ? defaultValue : value;
  }
}
