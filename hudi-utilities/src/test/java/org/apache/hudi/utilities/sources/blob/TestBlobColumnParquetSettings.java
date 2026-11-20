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
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TestBlobColumnParquetSettings {
  private static final String DELEGATE = "test.parquet.delegate";

  @Test
  void selfDelegateIsRefusedBeforeInstantiation() {
    StorageConfiguration<?> storage = HadoopFSUtils.getStorageConf(new Configuration(false));
    HoodieConfig config = new HoodieConfig();
    config.setValue(DELEGATE, BlobParquetConfigInjector.class.getName());
    IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
        () -> BlobColumnParquetSettings.runDelegate(BlobParquetConfigInjector.class, DELEGATE,
            new StoragePath("file:/table/file.parquet"), storage, config));
    assertEquals("BlobParquetConfigInjector cannot delegate to itself", failure.getMessage());
    assertEquals(BlobParquetConfigInjector.class.getName(), config.getString(DELEGATE));
  }

  @Test
  void absentDelegateReturnsIndependentCopies() {
    StorageConfiguration<?> storage = HadoopFSUtils.getStorageConf(new Configuration(false));
    storage.set("test.setting", "original");
    HoodieConfig config = new HoodieConfig();
    config.setValue("test.setting", "original");
    Pair<StorageConfiguration, HoodieConfig> copies = BlobColumnParquetSettings.runDelegate(
        BlobParquetConfigInjector.class, DELEGATE, new StoragePath("file:/table/file.parquet"), storage, config);
    assertNotSame(storage, copies.getLeft());
    assertNotSame(config, copies.getRight());
    copies.getLeft().set("test.setting", "changed");
    copies.getRight().setValue("test.setting", "changed");
    assertEquals("original", storage.getString("test.setting").get());
    assertEquals("original", config.getString("test.setting"));
  }
}
