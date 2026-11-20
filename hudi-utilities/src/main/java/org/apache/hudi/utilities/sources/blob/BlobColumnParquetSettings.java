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
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.core.io.HoodieParquetConfigInjector;
import org.apache.hudi.io.hadoop.HoodieBaseParquetWriter;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;

/**
 * The default Parquet write setup of the blob table (a table with {@code HoodieSchema.Blob} columns),
 * and the composition of a table's own Parquet config injector with one the job configured.
 *
 * <p>Per blob column, on its inline bytes leaf ({@code <column>.data}), because Parquet sets these per column: dictionary
 * encoding off, so the values are written PLAIN (Hudi writes Parquet writer version 1), and min/max statistics off where
 * Parquet has the per-column switch (1.14 or later). Per table, because Parquet has no per-column setting for them: compression
 * {@code UNCOMPRESSED} ({@code hoodie.parquet.compression.codec}, one codec per file) and no record-key bloom filter
 * ({@code hoodie.parquet.bloom.filter.enabled}, a filter over the record key, not a column). The table-wide settings are
 * the blob job's to set in its properties. The per-column settings are applied after the job's own injector and
 * override it for the blob columns' inline bytes.
 */
public final class BlobColumnParquetSettings {

  private BlobColumnParquetSettings() {
  }

  /** The dotted Parquet path of the inline bytes of blob column {@code column}. */
  public static String inlineDataPath(String column) {
    return column + "." + HoodieSchema.Blob.INLINE_DATA_FIELD;
  }

  /**
   * The per-column defaults on {@code storage}: dictionary off and, when {@code statisticsSwitch}, statistics off for each
   * blob column's inline bytes.
   */
  public static void applyColumnDefaults(StorageConfiguration<?> storage, Iterable<String> blobColumns, boolean statisticsSwitch) {
    for (String column : blobColumns) {
      String data = inlineDataPath(column);
      storage.set(HoodieBaseParquetWriter.COLUMN_DICTIONARY_ENABLED + "#" + data, "false");
      if (statisticsSwitch) {
        storage.set(HoodieBaseParquetWriter.COLUMN_STATISTICS_ENABLED + "#" + data, "false");
      }
    }
  }

  /**
   * Copies of {@code storageConf} and {@code hoodieConfig}, after the job's own injector (named under {@code delegateKey}) has
   * run on them; the caller's objects are never changed.
   *
   * @throws IllegalArgumentException when the delegate is {@code self}
   */
  public static Pair<StorageConfiguration, HoodieConfig> runDelegate(Class<?> self, String delegateKey, StoragePath path,
                                                                     StorageConfiguration storageConf, HoodieConfig hoodieConfig) {
    StorageConfiguration storageCopy = storageConf.newInstance();
    HoodieConfig configCopy = new HoodieConfig(TypedProperties.copy(hoodieConfig.getProps()));
    String delegateClass = configCopy.getStringOrDefault(delegateKey, "");
    if (StringUtils.isNullOrEmpty(delegateClass)) {
      return Pair.of(storageCopy, configCopy);
    }
    if (self.getName().equals(delegateClass)) {
      throw new IllegalArgumentException(self.getSimpleName() + " cannot delegate to itself");
    }
    HoodieParquetConfigInjector delegate = (HoodieParquetConfigInjector) ReflectionUtils.loadClass(delegateClass);
    Pair<StorageConfiguration, HoodieConfig> delegated = delegate.injectConfig(path, storageCopy, configCopy);
    return Pair.of(delegated.getLeft().newInstance(), new HoodieConfig(TypedProperties.copy(delegated.getRight().getProps())));
  }
}
