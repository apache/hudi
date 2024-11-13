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

package org.apache.hudi.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;

public class TestBaseHoodieWriteClient {
  private HoodieStorage storage;
  private StoragePath metaPath;

  @TempDir
  private Path tempDir;

  @BeforeEach
  public void setUp() throws IOException {
    StorageConfiguration<Configuration> storageConf = HoodieTestUtils.getDefaultStorageConf();
    storageConf.set("fs.defaultFS", "file:///");
    storageConf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    this.storage = HoodieStorageUtils.getStorage(tempDir.toString(), storageConf);
    this.metaPath = new StoragePath(tempDir + "/.hoodie");
  }

  @Test
  public void testValidateAgainstTableProperties() throws IOException {
    // Init table config
    Properties properties = new Properties();
    properties.put(HoodieTableConfig.TYPE.key(), HoodieTableType.COPY_ON_WRITE.toString());
    properties.put(HoodieTableConfig.VERSION.key(), 6);
    properties.put(HoodieTableConfig.POPULATE_META_FIELDS.key(), false);
    HoodieTestUtils.init(this.storage.getConf().newInstance(), this.tempDir.toString(), HoodieTableType.MERGE_ON_READ, properties);

    HoodieTableConfig tableConfig = new HoodieTableConfig(this.storage, this.metaPath, RecordMergeMode.OVERWRITE_WITH_LATEST,
        HoodieTableConfig.PAYLOAD_CLASS_NAME.defaultValue(), HoodieRecordMerger.DEFAULT_MERGE_STRATEGY_UUID);

    // Test version conflicts
    HoodieWriteConfig versionConflictConfig = HoodieWriteConfig.newBuilder()
        .withPath(tempDir.toString())
        .withWriteTableVersion(8)
        .build();
    Assertions.assertThrowsExactly(HoodieNotSupportedException.class,
        () -> BaseHoodieWriteClient.validateAgainstTableProperties(tableConfig, versionConflictConfig),
        "Table version (6) and Writer version (8) do not match.");

    // Test hoodie.populate.meta.fields conflicts
    HoodieWriteConfig metaFieldsConflictConfig = HoodieWriteConfig.newBuilder()
        .withPath(tempDir.toString())
        .withWriteTableVersion(6)
        .withPopulateMetaFields(true)
        .build();
    Assertions.assertThrowsExactly(HoodieException.class,
        () -> BaseHoodieWriteClient.validateAgainstTableProperties(tableConfig, metaFieldsConflictConfig),
        "hoodie.populate.meta.fields already disabled for the table. Can't be re-enabled back");

    // Test record key fields conflicts
    HoodieWriteConfig recordKeyConflictConfig = HoodieWriteConfig.newBuilder()
        .withPath(tempDir.toString())
        .withWriteTableVersion(6)
        .withPopulateMetaFields(false)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withRecordKeyField("a,b").build())
        .build();
    Assertions.assertThrowsExactly(HoodieException.class,
        () -> BaseHoodieWriteClient.validateAgainstTableProperties(tableConfig, recordKeyConflictConfig),
        "When meta fields are not populated, the number of record key fields must be exactly one");

    // Test hoodie.allow.operation.metadata.field conflicts
    HoodieWriteConfig operationMetaFieldConflictConfig = HoodieWriteConfig.newBuilder()
        .withPath(tempDir.toString())
        .withWriteTableVersion(6)
        .withPopulateMetaFields(false)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withRecordKeyField("a").build())
        .withAllowOperationMetadataField(true)
        .build();
    Assertions.assertThrowsExactly(HoodieException.class,
        () -> BaseHoodieWriteClient.validateAgainstTableProperties(tableConfig, operationMetaFieldConflictConfig),
        "Operation metadata fields are allowed, but populateMetaFields is not enabled. "
        + "Please ensure that populateMetaFields is set to true in the configuration.");

    // Test hoodie.index.bucket.engine conflicts
    HoodieWriteConfig bucketIndexEngineTypeConflictConfig = HoodieWriteConfig.newBuilder()
        .withPath(tempDir.toString())
        .withWriteTableVersion(6)
        .withPopulateMetaFields(false)
        .withIndexConfig(HoodieIndexConfig.newBuilder()
            .withRecordKeyField("a")
            .withIndexType(HoodieIndex.IndexType.BUCKET)
            .withBucketIndexEngineType(HoodieIndex.BucketIndexEngineType.CONSISTENT_HASHING)
            .build())
        .withAllowOperationMetadataField(true)
        .build();
    Assertions.assertThrowsExactly(HoodieException.class,
        () -> BaseHoodieWriteClient.validateAgainstTableProperties(tableConfig, bucketIndexEngineTypeConflictConfig),
        "Consistent hashing bucket index does not work with COW table. Use simple bucket index or an MOR table.");
  }
}
