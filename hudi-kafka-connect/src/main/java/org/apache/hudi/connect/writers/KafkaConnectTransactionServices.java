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

package org.apache.hudi.connect.writers;

import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.connect.transaction.TransactionCoordinator;
import org.apache.hudi.connect.utils.KafkaConnectUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.keygen.factory.HoodieAvroKeyGeneratorFactory;
import org.apache.hudi.sync.common.HoodieSyncConfig;
import org.apache.hudi.sync.common.util.SyncUtilHelpers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Implementation of Transaction service APIs used by
 * {@link TransactionCoordinator}
 * using {@link HoodieJavaWriteClient}.
 */
public class KafkaConnectTransactionServices implements ConnectTransactionServices {

  private static final Logger LOG = LogManager.getLogger(KafkaConnectTransactionServices.class);

  private final KafkaConnectConfigs connectConfigs;
  private final Option<HoodieTableMetaClient> tableMetaClient;
  private final Configuration hadoopConf;
  private final HoodieWriteConfig writeConfig;
  private final String tableBasePath;
  private final String tableName;
  private final HoodieEngineContext context;

  private final HoodieJavaWriteClient<HoodieAvroPayload> javaClient;

  public KafkaConnectTransactionServices(KafkaConnectConfigs connectConfigs) throws HoodieException {
    this.connectConfigs = connectConfigs;
    // This is the writeConfig for the Transaction Coordinator
    this.writeConfig = HoodieWriteConfig.newBuilder()
        .withEngineType(EngineType.JAVA)
        .withProperties(connectConfigs.getProps())
        .build();

    tableBasePath = writeConfig.getBasePath();
    tableName = writeConfig.getTableName();
    hadoopConf = KafkaConnectUtils.getDefaultHadoopConf(connectConfigs);
    context = new HoodieJavaEngineContext(hadoopConf);

    try {
      KeyGenerator keyGenerator = HoodieAvroKeyGeneratorFactory.createKeyGenerator(
          new TypedProperties(connectConfigs.getProps()));
      String recordKeyFields = KafkaConnectUtils.getRecordKeyColumns(keyGenerator);
      String partitionColumns = KafkaConnectUtils.getPartitionColumns(keyGenerator,
          new TypedProperties(connectConfigs.getProps()));

      LOG.info(String.format("Setting record key %s and partition fields %s for table %s",
          recordKeyFields, partitionColumns, tableBasePath + tableName));

      tableMetaClient = Option.of(HoodieTableMetaClient.withPropertyBuilder()
          .setTableType(HoodieTableType.COPY_ON_WRITE.name())
          .setTableName(tableName)
          .setPayloadClassName(HoodieAvroPayload.class.getName())
          .setRecordKeyFields(recordKeyFields)
          .setPartitionFields(partitionColumns)
          .setKeyGeneratorClassProp(writeConfig.getKeyGeneratorClass())
          .fromProperties(connectConfigs.getProps())
          .initTable(hadoopConf, tableBasePath));

      javaClient = new HoodieJavaWriteClient<>(context, writeConfig);
    } catch (Exception exception) {
      throw new HoodieException("Fatal error instantiating Hudi Transaction Services ", exception);
    }
  }

  @Override
  public String startCommit() {
    String newCommitTime = javaClient.startCommit();
    javaClient.transitionInflight(newCommitTime);
    LOG.info("Starting Hudi commit " + newCommitTime);
    return newCommitTime;
  }

  @Override
  public boolean endCommit(String commitTime, List<WriteStatus> writeStatuses, Map<String, String> extraMetadata) {
    boolean success = javaClient.commit(commitTime, writeStatuses, Option.of(extraMetadata));
    if (success) {
      LOG.info("Ending Hudi commit " + commitTime);

      // Schedule clustering and compaction as needed.
      if (writeConfig.isAsyncClusteringEnabled()) {
        javaClient.scheduleClustering(Option.empty()).ifPresent(
            instantTs -> LOG.info("Scheduled clustering at instant time:" + instantTs));
      }
      if (isAsyncCompactionEnabled()) {
        javaClient.scheduleCompaction(Option.empty()).ifPresent(
            instantTs -> LOG.info("Scheduled compaction at instant time:" + instantTs));
      }
      syncMeta();
    }
    return success;
  }

  @Override
  public Map<String, String> fetchLatestExtraCommitMetadata() {
    if (tableMetaClient.isPresent()) {
      Option<HoodieCommitMetadata> metadata = KafkaConnectUtils.getCommitMetadataForLatestInstant(tableMetaClient.get());
      if (metadata.isPresent()) {
        return metadata.get().getExtraMetadata();
      } else {
        LOG.info("Hoodie Extra Metadata from latest commit is absent");
        return Collections.emptyMap();
      }
    }
    throw new HoodieException("Fatal error retrieving Hoodie Extra Metadata since Table Meta Client is absent");
  }

  private boolean isAsyncCompactionEnabled() {
    return tableMetaClient.isPresent()
        && HoodieTableType.MERGE_ON_READ.equals(tableMetaClient.get().getTableType())
        && connectConfigs.isAsyncCompactEnabled();
  }

  private void syncMeta() {
    if (connectConfigs.isMetaSyncEnabled()) {
      Set<String> syncClientToolClasses = new HashSet<>(
          Arrays.asList(connectConfigs.getMetaSyncClasses().split(",")));
      FileSystem fs = FSUtils.getFs(tableBasePath, new Configuration());
      for (String impl : syncClientToolClasses) {
        // TODO kafka connect config needs to support setting base file format
        String baseFileFormat = connectConfigs.getStringOrDefault(HoodieSyncConfig.META_SYNC_BASE_FILE_FORMAT);
        SyncUtilHelpers.runHoodieMetaSync(impl.trim(), connectConfigs.getProps(), hadoopConf, fs, tableBasePath, baseFileFormat);
      }
    }
  }
}
