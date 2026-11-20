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
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.utilities.sources.unstructured.UnstructuredMode;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Legacy blob min/max payload stays within one sixty-fourth of the configured row-group target. */
class TestBlobLegacyStatisticsBudget {
  private static TypedProperties props(long inline, int group) {
    TypedProperties props = new TypedProperties();
    props.setProperty(BlobTableConfig.INLINE_MAX_BYTES.key(), Long.toString(inline));
    props.setProperty(HoodieStorageConfig.PARQUET_BLOCK_SIZE.key(), Integer.toString(group));
    props.setProperty(HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME.key(), "gzip");
    return props;
  }

  private static Pair<StorageConfiguration, HoodieConfig> inject(TypedProperties props, boolean modern) {
    return new BlobParquetConfigInjector().injectConfig(new StoragePath("file:/unused-budget.parquet"),
        new HadoopStorageConfiguration(new Configuration(false)), new HoodieConfig(props), modern);
  }

  private static void assertKeys(IllegalArgumentException failure) {
    assertTrue(failure.getMessage().contains(BlobTableConfig.INLINE_MAX_BYTES.key()), failure.getMessage());
    assertTrue(failure.getMessage().contains(HoodieStorageConfig.PARQUET_BLOCK_SIZE.key()), failure.getMessage());
  }

  @Test
  void legacySmallGroupsAreRefusedBeforeTheSourceStarts() {
    TypedProperties props = props(81920, 65536);
    if (HoodieBaseParquetWriter.supportsColumnStatisticsSwitch()) {
      BlobSourceSetup.validate(props, UnstructuredMode.BLOB, "budget test");
    } else {
      assertKeys(assertThrows(IllegalArgumentException.class,
          () -> BlobSourceSetup.validate(props, UnstructuredMode.BLOB, "budget test")));
    }
  }

  @Test
  void legacyUsesTheEffectiveThresholdAtTheExactBudgetBoundary() {
    // 100000 is capped to 81920 on legacy Parquet; 2*81920 == 10485760/64.
    inject(props(100000, 10485760), false);
    assertKeys(assertThrows(IllegalArgumentException.class, () -> inject(props(81920, 10485759), false)));
    inject(new TypedProperties(), false);
    inject(props(0, 1), false);
    assertKeys(assertThrows(IllegalArgumentException.class, () -> inject(props(0, 0), false)));
    inject(props(81920, 65536), true);
  }

  @Test
  void delegateCannotHidePreparedInlineRowsByLoweringTheirThreshold() {
    TypedProperties props = props(81920, 125829120);
    props.setProperty(BlobParquetConfigInjector.DELEGATE_CLASS, LowerThresholdInjector.class.getName());
    assertKeys(assertThrows(IllegalArgumentException.class, () -> inject(props, false)));
    assertEquals("81920", props.getProperty(BlobTableConfig.INLINE_MAX_BYTES.key()));
    assertEquals("125829120", props.getProperty(HoodieStorageConfig.PARQUET_BLOCK_SIZE.key()));
    inject(props, true);
  }

  public static class LowerThresholdInjector implements HoodieParquetConfigInjector {
    @Override
    public Pair<StorageConfiguration, HoodieConfig> injectConfig(StoragePath path, StorageConfiguration conf, HoodieConfig config) {
      config.setValue(BlobTableConfig.INLINE_MAX_BYTES, "0");
      config.setValue(HoodieStorageConfig.PARQUET_BLOCK_SIZE, "65536");
      return Pair.of(conf, config);
    }
  }
}
