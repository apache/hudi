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

package org.apache.hudi.aws.testutils;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.versioning.DefaultInstantFileNameGenerator;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import software.amazon.awssdk.services.glue.model.Column;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Instant;
import java.util.Arrays;

import static org.apache.hudi.common.table.HoodieTableMetaClient.METAFOLDER_NAME;
import static org.apache.hudi.config.GlueCatalogSyncClientConfig.GLUE_SYNC_DATABASE_NAME;
import static org.apache.hudi.config.GlueCatalogSyncClientConfig.GLUE_SYNC_TABLE_NAME;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_BATCH_SYNC_PARTITION_NUM;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_PASS;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_USER;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_USE_PRE_APACHE_INPUT_FORMAT;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_PATH;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_PARTITION_FIELDS;

public class GlueTestUtil {

  public static TypedProperties glueSyncProps;
  public static final String DB_NAME = "testdb";
  public static final String TABLE_NAME = "test1";
  private static String basePath;
  public static FileSystem fileSystem;
  private static HiveSyncConfig hiveSyncConfig;
  private static Configuration hadoopConf;
  private static HoodieTableMetaClient metaClient;

  public static void setUp() throws IOException {
    basePath = Files.createTempDirectory("glueClientTest" + Instant.now().toEpochMilli()).toUri().toString();

    glueSyncProps = new TypedProperties();
    glueSyncProps.setProperty(HIVE_USER.key(), "");
    glueSyncProps.setProperty(HIVE_PASS.key(), "");
    glueSyncProps.setProperty(GLUE_SYNC_DATABASE_NAME.key(), DB_NAME);
    glueSyncProps.setProperty(GLUE_SYNC_TABLE_NAME.key(), TABLE_NAME);
    glueSyncProps.setProperty(META_SYNC_BASE_PATH.key(), basePath);
    glueSyncProps.setProperty(HIVE_USE_PRE_APACHE_INPUT_FORMAT.key(), "false");
    glueSyncProps.setProperty(META_SYNC_PARTITION_EXTRACTOR_CLASS.key(), SlashEncodedDayPartitionValueExtractor.class.getName());
    glueSyncProps.setProperty(META_SYNC_PARTITION_FIELDS.key(), "datestr");
    glueSyncProps.setProperty(HIVE_BATCH_SYNC_PARTITION_NUM.key(), "3");
    hiveSyncConfig = new HiveSyncConfig(glueSyncProps);
    fileSystem = hiveSyncConfig.getHadoopFileSystem();
    hadoopConf = HadoopFSUtils.getFs(glueSyncProps.getString(META_SYNC_BASE_PATH.key()), new Configuration()).getConf();
    // creates a hoodie table in the base path for testing
    createHoodieTable();
  }

  public static void clear() throws IOException {
    fileSystem.delete(new Path(basePath), true);
  }

  public static void teardown() throws IOException {
    fileSystem.close();
  }

  public static HiveSyncConfig getHiveSyncConfig() {
    return hiveSyncConfig;
  }

  public static Configuration getHadoopConf() {
    return hadoopConf;
  }

  public static void createHoodieTable() throws IOException {
    Path path = new Path(basePath);
    if (fileSystem.exists(path)) {
      fileSystem.delete(path, true);
    }
    metaClient = HoodieTableMetaClient.newTableBuilder()
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setTableName(TABLE_NAME)
        .setPayloadClass(HoodieAvroPayload.class)
        .initTable(HadoopFSUtils.getStorageConf(new Configuration()), basePath);

    String instantTime = "101";
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata(false);
    createMetaFile(basePath, new DefaultInstantFileNameGenerator().makeCommitFileName(instantTime), commitMetadata);
  }

  public static HoodieSchema getSimpleSchema() {
    return HoodieSchema.createRecord("example_schema", null, null,
        Arrays.asList(
            HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT)),
            HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING))
        ));
  }

  private static void createMetaFile(String basePath, String fileName, HoodieCommitMetadata metadata)
      throws IOException {
    byte[] bytes = metadata.toJsonString().getBytes(StandardCharsets.UTF_8);
    Path fullPath = new Path(basePath + "/" + METAFOLDER_NAME + "/" + fileName);
    FSDataOutputStream fsout = fileSystem.create(fullPath, true);
    fsout.write(bytes);
    fsout.close();
  }

  public static Column getColumn(String name, String type, String comment) {
    return Column.builder().name(name).type(type).comment(comment).build();
  }

  public static HoodieTableMetaClient getMetaClient() {
    return metaClient;
  }
}
