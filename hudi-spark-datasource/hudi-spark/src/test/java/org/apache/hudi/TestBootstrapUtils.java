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

package org.apache.hudi;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hudi.cli.BootstrapExecutorUtils;

import org.apache.hudi.cli.HDFSParquetImporterUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.config.HoodieBootstrapConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.multisync.MockSyncTool1;
import org.apache.hudi.multisync.MockSyncTool2;
import org.apache.hudi.multisync.MockSyncToolException1;
import org.apache.hudi.multisync.MockSyncToolException2;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;

import static org.apache.hudi.keygen.constant.KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME;
import static org.apache.hudi.keygen.constant.KeyGeneratorOptions.RECORDKEY_FIELD_NAME;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_ENABLED;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
public class TestBootstrapUtils {
  SparkSession spark = SparkSession
          .builder()
          .master("local[*]")
        .appName(TestBootstrapUtils.class.getName())
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate();

  @Test
  public void testMultiSync() throws Exception {

    String basePath = Files.createTempDirectory(System.currentTimeMillis() + "-").toFile().getAbsolutePath();
    BootstrapExecutorUtils.Config cfg = new BootstrapExecutorUtils.Config();
    cfg.setTableName("test_table");
    cfg.setTableType("COPY_ON_WRITE");
    cfg.setBasePath(basePath);
    cfg.setBaseFileFormat("PARQUET");
    cfg.setEnableHiveSync(false);

    JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
    FileSystem fs = FSUtils.getFs(basePath, jsc.hadoopConfiguration());
    TypedProperties properties = HDFSParquetImporterUtils.buildProperties(new ArrayList<String>());
    properties.setProperty(HoodieBootstrapConfig.BASE_PATH.key(), basePath);
    properties.setProperty(RECORDKEY_FIELD_NAME.key(), "rowKeyField");
    properties.setProperty(PARTITIONPATH_FIELD_NAME.key(), "partitionPathField");
    properties.setProperty(META_SYNC_ENABLED.key(), "true");

    String syncClientToolClasses = Arrays.asList(
        MockSyncTool1.class.getName(), MockSyncTool2.class.getName(),
        MockSyncToolException1.class.getName(), MockSyncToolException2.class.getName())
          .stream().collect(Collectors.joining(","));
    properties.setProperty("hoodie.meta.sync.client.tool.class", syncClientToolClasses);

    Exception e = assertThrows(
        HoodieException.class,
        () -> new BootstrapExecutorUtils(cfg, jsc, fs, jsc.hadoopConfiguration(), properties).metaSync());
    assertTrue(MockSyncTool1.syncSuccess);
    assertTrue(MockSyncTool2.syncSuccess);
    assertTrue(e.getMessage().contains(MockSyncToolException1.class.getName()));
    assertTrue(e.getMessage().contains(MockSyncToolException2.class.getName()));

    File file = new File(basePath);
    FileIOUtils.deleteDirectory(file);
  }
}
