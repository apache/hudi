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

package org.apache.hudi.utilities.unstructured.embedding;

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.core.io.HoodieParquetConfigInjector;
import org.apache.hudi.io.hadoop.HoodieBaseParquetWriter;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.utilities.sources.blob.BlobColumnParquetSettings;

import java.util.Collections;

/**
 * The embedding table's per-column Parquet setup: the inline bytes of its {@link EmbeddingTableSchema#PREPARED_IMAGE} blob column
 * ({@code prepared_image.data}) are written without dictionary encoding and, where Parquet has the per-column switch, without
 * min/max statistics ({@link BlobColumnParquetSettings#applyColumnDefaults}), as the blob table's {@code blob.data} is. An inline
 * image is opaque bytes: a dictionary over them never pays off, and min/max statistics would keep a copy of two whole images per
 * row group in the footer. Page-size checks run after every row, so near-threshold inline images cannot accumulate for a
 * 100-row check interval. A whole image can exceed the page target. The table-wide settings (codec, record-key bloom filter,
 * page, row-group and file sizes) stay the job's.
 * An injector the job configured runs first; these settings are applied after it.
 */
public class EmbeddingParquetConfigInjector implements HoodieParquetConfigInjector {

  /** The job's own Parquet config injector, when it configures one: this injector runs it first. */
  public static final String DELEGATE_CLASS = EmbeddingTableConfig.PREFIX + "parquet.config.injector.delegate.class";

  @Override
  public Pair<StorageConfiguration, HoodieConfig> injectConfig(StoragePath path, StorageConfiguration storageConf, HoodieConfig hoodieConfig) {
    return injectConfig(path, storageConf, hoodieConfig, HoodieBaseParquetWriter.supportsColumnStatisticsSwitch());
  }

  /** As {@link #injectConfig(StoragePath, StorageConfiguration, HoodieConfig)}, for a Parquet with or without the switch. */
  Pair<StorageConfiguration, HoodieConfig> injectConfig(StoragePath path, StorageConfiguration storageConf, HoodieConfig hoodieConfig,
                                                        boolean statisticsSwitch) {
    Pair<StorageConfiguration, HoodieConfig> copies =
        BlobColumnParquetSettings.runDelegate(EmbeddingParquetConfigInjector.class, DELEGATE_CLASS, path, storageConf, hoodieConfig);
    copies.getLeft().set(HoodieBaseParquetWriter.INTERNAL_MIN_ROW_COUNT_FOR_PAGE_SIZE_CHECK, "1");
    copies.getLeft().set(HoodieBaseParquetWriter.INTERNAL_MAX_ROW_COUNT_FOR_PAGE_SIZE_CHECK, "1");
    BlobColumnParquetSettings.applyColumnDefaults(copies.getLeft(), Collections.singletonList(EmbeddingTableSchema.PREPARED_IMAGE),
        statisticsSwitch);
    return copies;
  }
}
