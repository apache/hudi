/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.utilities.streamer;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieTimelineTimeZone;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HiveSyncTool;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.NonpartitionedKeyGenerator;
import org.apache.hudi.keygen.SimpleKeyGenerator;
import org.apache.hudi.util.SparkKeyGenUtils;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.schema.SchemaProvider;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;

import static org.apache.hudi.common.table.HoodieTableConfig.PARTITION_METAFILE_USE_BASE_FORMAT;
import static org.apache.hudi.common.table.HoodieTableConfig.POPULATE_META_FIELDS;
import static org.apache.hudi.common.table.HoodieTableConfig.TIMELINE_HISTORY_PATH;
import static org.apache.hudi.common.table.HoodieTableConfig.TIMELINE_TIMEZONE;
import static org.apache.hudi.config.HoodieWriteConfig.WRITE_TABLE_VERSION;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SYNC_BUCKET_SYNC;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SYNC_BUCKET_SYNC_SPEC;
import static org.apache.hudi.keygen.constant.KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_ENABLE;
import static org.apache.hudi.keygen.constant.KeyGeneratorOptions.RECORDKEY_FIELD_NAME;
import static org.apache.hudi.keygen.constant.KeyGeneratorOptions.URL_ENCODE_PARTITIONING;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_FILE_FORMAT;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_PATH;

/**
 * Performs bootstrap from a non-hudi source.
 */
public class BootstrapExecutor implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(BootstrapExecutor.class);

  /**
   * Config.
   */
  private final HoodieStreamer.Config cfg;

  /**
   * Schema provider that supplies the command for reading the input and writing out the target table.
   */
  private transient SchemaProvider schemaProvider;

  /**
   * Spark context.
   */
  private transient JavaSparkContext jssc;

  /**
   * Bag of properties with source, hoodie client, key generator etc.
   */
  private final TypedProperties props;

  /**
   * Hadoop Configuration.
   */
  private final Configuration configuration;

  /**
   * Bootstrap Configuration.
   */
  private final HoodieWriteConfig bootstrapConfig;

  /**
   * FileSystem instance.
   */
  private transient FileSystem fs;

  private final String bootstrapBasePath;

  /**
   * Bootstrap Executor.
   *
   * @param cfg        DeltaStreamer Config
   * @param jssc       Java Spark Context
   * @param fs         File System
   * @param properties Bootstrap Writer Properties
   * @throws IOException
   */
  public BootstrapExecutor(HoodieStreamer.Config cfg, JavaSparkContext jssc, FileSystem fs, Configuration conf,
                           TypedProperties properties) throws IOException {
    this.cfg = cfg;
    this.jssc = jssc;
    this.fs = fs;
    this.configuration = conf;
    this.props = properties;

    ValidationUtils.checkArgument(properties.containsKey(HoodieTableConfig.BOOTSTRAP_BASE_PATH
            .key()),
        HoodieTableConfig.BOOTSTRAP_BASE_PATH.key() + " must be specified.");
    this.bootstrapBasePath = properties.getString(HoodieTableConfig.BOOTSTRAP_BASE_PATH.key());
    this.schemaProvider = UtilHelpers.createSchemaProvider(cfg.schemaProviderClassName, props, jssc);
    HoodieWriteConfig.Builder builder =
        HoodieWriteConfig.newBuilder().withPath(cfg.targetBasePath)
            .withCompactionConfig(HoodieCompactionConfig.newBuilder().withInlineCompaction(false).build())
            .forTable(cfg.targetTableName)
            .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
            .withRecordMergeImplClasses(cfg.recordMergeImplClasses)
            .withProps(props);

    if (null != schemaProvider && null != schemaProvider.getTargetSchema()) {
      builder = builder.withSchema(schemaProvider.getTargetSchema().toString());
    }
    this.bootstrapConfig = builder.build();
    LOG.info("Created bootstrap executor with configs : " + bootstrapConfig.getProps());
  }

  /**
   * Executes Bootstrap.
   */
  public void execute() throws IOException {
    initializeTable();
    try (SparkRDDWriteClient bootstrapClient = new SparkRDDWriteClient(new HoodieSparkEngineContext(jssc), bootstrapConfig)) {
      HashMap<String, String> checkpointCommitMetadata = new HashMap<>();
      checkpointCommitMetadata.put(HoodieStreamer.CHECKPOINT_KEY, cfg.checkpoint);
      if (cfg.checkpoint != null) {
        checkpointCommitMetadata.put(HoodieStreamer.CHECKPOINT_RESET_KEY, cfg.checkpoint);
      }
      bootstrapClient.bootstrap(Option.of(checkpointCommitMetadata));
      syncHive();
    }
  }

  /**
   * Sync to Hive.
   */
  private void syncHive() {
    if (cfg.enableHiveSync || cfg.enableMetaSync) {
      TypedProperties metaProps = new TypedProperties();
      metaProps.putAll(props);
      metaProps.put(META_SYNC_BASE_PATH.key(), cfg.targetBasePath);
      metaProps.put(META_SYNC_BASE_FILE_FORMAT.key(), cfg.baseFileFormat);
      if (props.getBoolean(HIVE_SYNC_BUCKET_SYNC.key(), HIVE_SYNC_BUCKET_SYNC.defaultValue())) {
        metaProps.put(HIVE_SYNC_BUCKET_SYNC_SPEC.key(), HiveSyncConfig.getBucketSpec(props.getString(HoodieIndexConfig.BUCKET_INDEX_HASH_FIELD.key()),
            props.getInteger(HoodieIndexConfig.BUCKET_INDEX_NUM_BUCKETS.key())));
      }

      try (HiveSyncTool hiveSyncTool = new HiveSyncTool(metaProps, configuration)) {
        hiveSyncTool.syncHoodieTable();
      }
    }
  }

  private void initializeTable() throws IOException {
    Path basePath = new Path(cfg.targetBasePath);
    if (fs.exists(basePath)) {
      if (cfg.bootstrapOverwrite) {
        LOG.info("Target base path already exists, overwrite it");
        fs.delete(basePath, true);
      } else {
        throw new HoodieException("target base path already exists at " + cfg.targetBasePath
            + ". Cannot bootstrap data on top of an existing table");
      }
    }

    if (cfg.targetBasePath.equals(bootstrapBasePath)) {
      throw new IllegalArgumentException("Bootstrap source base path and Hudi table base path must be different");
    }

    HoodieTableMetaClient.TableBuilder builder = HoodieTableMetaClient.newTableBuilder()
        .fromProperties(props)
        .setTableType(cfg.tableType)
        .setTableName(cfg.targetTableName)
        .setRecordKeyFields(props.getString(RECORDKEY_FIELD_NAME.key()))
        .setOrderingFields(ConfigUtils.getOrderingFieldsStrDuringWrite(props))
        .setTableVersion(ConfigUtils.getIntWithAltKeys(props, WRITE_TABLE_VERSION))
        .setTableFormat(props.getString(HoodieTableConfig.TABLE_FORMAT.key(), HoodieTableConfig.TABLE_FORMAT.defaultValue()))
        .setPopulateMetaFields(props.getBoolean(
            POPULATE_META_FIELDS.key(), POPULATE_META_FIELDS.defaultValue()))
        .setArchiveLogFolder(props.getString(
            TIMELINE_HISTORY_PATH.key(), TIMELINE_HISTORY_PATH.defaultValue()))
        .setPayloadClassName(cfg.payloadClassName)
        .setRecordMergeMode(cfg.recordMergeMode)
        .setRecordMergeStrategyId(cfg.recordMergeStrategyId)
        .setBaseFileFormat(cfg.baseFileFormat)
        .setBootstrapIndexClass(cfg.bootstrapIndexClass)
        .setBootstrapBasePath(bootstrapBasePath)
        .setHiveStylePartitioningEnable(props.getBoolean(
            HIVE_STYLE_PARTITIONING_ENABLE.key(),
            Boolean.parseBoolean(HIVE_STYLE_PARTITIONING_ENABLE.defaultValue())
        ))
        .setUrlEncodePartitioning(props.getBoolean(
            URL_ENCODE_PARTITIONING.key(),
            Boolean.parseBoolean(URL_ENCODE_PARTITIONING.defaultValue())))
        .setCommitTimezone(HoodieTimelineTimeZone.valueOf(
            props.getString(
                TIMELINE_TIMEZONE.key(),
                String.valueOf(TIMELINE_TIMEZONE.defaultValue()))))
        .setPartitionMetafileUseBaseFormat(props.getBoolean(
            PARTITION_METAFILE_USE_BASE_FORMAT.key(),
            PARTITION_METAFILE_USE_BASE_FORMAT.defaultValue()));
    String partitionColumnsForKeyGenerator = SparkKeyGenUtils.getPartitionColumnsForKeyGenerator(props, HoodieTableVersion.fromVersionCode(ConfigUtils.getIntWithAltKeys(props, WRITE_TABLE_VERSION)));
    if (!StringUtils.isNullOrEmpty(partitionColumnsForKeyGenerator)) {
      builder.setPartitionFields(partitionColumnsForKeyGenerator).setKeyGeneratorClassProp(
          props.getString(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key(), SimpleKeyGenerator.class.getName()));
    } else {
      builder.setKeyGeneratorClassProp(props.getString(
          HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key(), NonpartitionedKeyGenerator.class.getName()));
    }

    builder.initTable(HadoopFSUtils.getStorageConfWithCopy(jssc.hadoopConfiguration()), cfg.targetBasePath);
  }

  public HoodieWriteConfig getBootstrapConfig() {
    return bootstrapConfig;
  }
}
