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

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.core.io.HoodieParquetConfigInjector;
import org.apache.hudi.io.hadoop.HoodieBaseParquetWriter;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;

import java.util.Collections;

/**
 * The blob table's Parquet setup ({@link BlobColumnParquetSettings}: the {@code blob} column's inline bytes PLAIN, without
 * dictionary or min/max statistics), plus bounds on the writer's buffers, retaining the caller's injector and smaller size
 * settings. Page and row-group checks run after every whole row (Parquet otherwise checks only after 100 records, by which
 * point 1 MiB inline values have made a ~100 MB page); an inline row may exceed the configured page target. The page size is
 * the job's (Hudi's default 1 MiB), and so is the row-group size (Hudi's default 120 MiB): once the per-record check bounds the
 * overshoot, a larger page or an open or read row group of that size costs a table without blob columns the same, so
 * it is an executor sizing term (about one row group per concurrent writer or reader task), not a writer cap.
 * Files grow to the caller's size target; on a Parquet with per-column statistics (1.14 or later) it is checked on every record.
 * An older Parquet keeps a full copy of the min and max blob value of every finished row group until the file
 * closes; there the inline threshold is capped instead ({@link BlobTableConfig#inlineMaxBytes}).
 * Legacy blob writes also require twice that effective threshold to fit within 1/64 of the effective row-group target:
 * the retained min/max payload budget per completed group. This is a per-group configuration budget for newly prepared
 * rows, not a total heap limit for accumulated groups or historical values with a different inline threshold.
 */
public class BlobParquetConfigInjector implements HoodieParquetConfigInjector {
  private static final int LEGACY_STATISTICS_BUDGET_DIVISOR = 64;
  /** The job's own Parquet config injector, when it configures one: this injector runs it first. */
  public static final String DELEGATE_CLASS = BlobTableConfig.PREFIX + "parquet.config.injector.delegate.class";

  @Override
  public Pair<StorageConfiguration, HoodieConfig> injectConfig(StoragePath path, StorageConfiguration storageConf, HoodieConfig hoodieConfig) {
    return injectConfig(path, storageConf, hoodieConfig, HoodieBaseParquetWriter.supportsColumnStatisticsSwitch());
  }

  /** As {@link #injectConfig(StoragePath, StorageConfiguration, HoodieConfig)}, for a Parquet with or without the switch. */
  Pair<StorageConfiguration, HoodieConfig> injectConfig(StoragePath path, StorageConfiguration storageConf, HoodieConfig hoodieConfig,
                                                        boolean statisticsSwitch) {
    Pair<StorageConfiguration, HoodieConfig> copies =
        BlobColumnParquetSettings.runDelegate(BlobParquetConfigInjector.class, DELEGATE_CLASS, path, storageConf, hoodieConfig);
    StorageConfiguration storageCopy = copies.getLeft();
    HoodieConfig configCopy = copies.getRight();
    // Keep both caller and delegate caps: a custom injector must not enlarge a smaller caller budget.
    configCopy.setValue(HoodieStorageConfig.PARQUET_PAGE_SIZE, Integer.toString(
        Math.min(hoodieConfig.getIntOrDefault(HoodieStorageConfig.PARQUET_PAGE_SIZE), configCopy.getIntOrDefault(HoodieStorageConfig.PARQUET_PAGE_SIZE))));
    configCopy.setValue(HoodieStorageConfig.PARQUET_BLOCK_SIZE, Integer.toString(
        Math.min(hoodieConfig.getIntOrDefault(HoodieStorageConfig.PARQUET_BLOCK_SIZE), configCopy.getIntOrDefault(HoodieStorageConfig.PARQUET_BLOCK_SIZE))));
    configCopy.setValue(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE, Long.toString(
        Math.min(hoodieConfig.getLongOrDefault(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE), configCopy.getLongOrDefault(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE))));
    checkLegacyStatisticsBudget(TypedProperties.copy(hoodieConfig.getProps()),
        configCopy.getIntOrDefault(HoodieStorageConfig.PARQUET_BLOCK_SIZE), statisticsSwitch);
    storageCopy.set(HoodieBaseParquetWriter.INTERNAL_MIN_ROW_COUNT_FOR_PAGE_SIZE_CHECK, "1");
    storageCopy.set(HoodieBaseParquetWriter.INTERNAL_MAX_ROW_COUNT_FOR_PAGE_SIZE_CHECK, "1");
    BlobColumnParquetSettings.applyColumnDefaults(storageCopy, Collections.singletonList(BlobTableSchema.BLOB), statisticsSwitch);
    if (statisticsSwitch) {
      storageCopy.set(HoodieBaseParquetWriter.INTERNAL_SIZE_CHECK_EVERY_RECORD, "true");
    }
    return Pair.of(storageCopy, configCopy);
  }

  /** Checks the job's prepared inline threshold against the final caller/delegate row-group target. */
  static void checkLegacyStatisticsBudget(TypedProperties props, int rowGroupTarget, boolean statisticsSwitch) {
    if (!statisticsSwitch) {
      long inline = BlobTableConfig.inlineMaxBytes(props, false);
      long budget = rowGroupTarget / (long) LEGACY_STATISTICS_BUDGET_DIVISOR;
      if (rowGroupTarget <= 0 || 2L * inline > budget) {
        throw new IllegalArgumentException("Legacy Parquet requires 2 x " + BlobTableConfig.INLINE_MAX_BYTES.key()
            + " (effective " + inline + " bytes) <= " + HoodieStorageConfig.PARQUET_BLOCK_SIZE.key()
            + " (effective " + rowGroupTarget + " bytes) / " + LEGACY_STATISTICS_BUDGET_DIVISOR
            + ": retained blob min/max payload per group must fit its " + budget + " byte budget; lower the inline threshold"
            + " or increase the positive row-group target");
      }
    }
  }
}
