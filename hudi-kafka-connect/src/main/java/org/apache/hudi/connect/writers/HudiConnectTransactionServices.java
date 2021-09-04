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
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.connect.core.TransactionCoordinator;
import org.apache.hudi.connect.utils.KafkaConnectUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.keygen.factory.HoodieAvroKeyGeneratorFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Implementation of Transaction service APIs used by
 * {@link TransactionCoordinator}
 * using {@link HoodieJavaWriteClient}.
 */
public class HudiConnectTransactionServices implements ConnectTransactionServices {

  private static final Logger LOG = LoggerFactory.getLogger(HudiConnectTransactionServices.class);
  private static final String TABLE_FORMAT = "PARQUET";

  private final HudiConnectConfigs connectConfigs;
  private final Option<HoodieTableMetaClient> tableMetaClient;
  private final Configuration hadoopConf;
  private final FileSystem fs;
  private final String tableBasePath;
  private final String tableName;
  private final HoodieEngineContext context;

  private final HoodieJavaWriteClient<HoodieAvroPayload> hudiJavaClient;

  public HudiConnectTransactionServices(
      HudiConnectConfigs connectConfigs) throws HoodieException {
    this.connectConfigs = connectConfigs;
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withProperties(connectConfigs.getProps()).build();

    tableBasePath = writeConfig.getBasePath();
    tableName = writeConfig.getTableName();
    hadoopConf = KafkaConnectUtils.getDefaultHadoopConf();
    context = new HoodieJavaEngineContext(hadoopConf);
    fs = FSUtils.getFs(tableBasePath, hadoopConf);

    try {
      KeyGenerator keyGenerator = HoodieAvroKeyGeneratorFactory.createKeyGenerator(
          new TypedProperties(connectConfigs.getProps()));

      String recordKeyFields = KafkaConnectUtils.getRecordKeyColumns(keyGenerator);
      String partitionColumns = KafkaConnectUtils.getPartitionColumns(keyGenerator,
          new TypedProperties(connectConfigs.getProps()));

      LOG.info("Setting record key {} and partitionfields {} for table {}",
          recordKeyFields,
          partitionColumns,
          tableBasePath + tableName);

      tableMetaClient = Option.of(HoodieTableMetaClient.withPropertyBuilder()
          .setTableType(HoodieTableType.COPY_ON_WRITE.name())
          .setTableName(tableName)
          .setPayloadClassName(HoodieAvroPayload.class.getName())
          .setBaseFileFormat(TABLE_FORMAT)
          .setRecordKeyFields(recordKeyFields)
          .setPartitionFields(partitionColumns)
          .setKeyGeneratorClassProp(writeConfig.getKeyGeneratorClass())
          .initTable(hadoopConf, tableBasePath));

      hudiJavaClient = new HoodieJavaWriteClient<>(context, writeConfig);
    } catch (Exception exception) {
      throw new HoodieException("Fatal error instantiating Hudi Transaction Services ", exception);
    }
  }

  public String startCommit() {
    String newCommitTime = hudiJavaClient.startCommit();
    hudiJavaClient.preBulkWrite(newCommitTime);
    LOG.info("Starting Hudi commit " + newCommitTime);
    return newCommitTime;
  }

  public void endCommit(String commitTime, List<WriteStatus> writeStatuses, Map<String, String> extraMetadata) {
    hudiJavaClient.commit(commitTime, writeStatuses, Option.of(extraMetadata),
        HoodieActiveTimeline.COMMIT_ACTION, Collections.emptyMap());
    LOG.info("Ending Hudi commit " + commitTime);
  }

  public Map<String, String> loadLatestCommitMetadata() {
    if (tableMetaClient.isPresent()) {
      Option<HoodieCommitMetadata> metadata = CommitUtils.getCommitMetadataForLatestInstant(tableMetaClient.get());
      if (metadata.isPresent()) {
        return metadata.get().getExtraMetadata();
      } else {
        LOG.info("Hoodie Extra Metadata from latest commit is absent");
        return Collections.emptyMap();
      }
    }
    throw new HoodieException("Fatal error retrieving Hoodie Extra Metadata since Table Meta Client is absent");
  }
}
