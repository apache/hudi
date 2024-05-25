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
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.connect.KafkaConnectFileIdPrefixProvider;
import org.apache.hudi.connect.utils.KafkaConnectUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.keygen.factory.HoodieAvroKeyGeneratorFactory;
import org.apache.hudi.schema.SchemaProvider;
import org.apache.hudi.storage.StorageConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

/**
 * Provides the Hudi Writer for the {@link org.apache.hudi.connect.transaction.TransactionParticipant}
 * to write the incoming records to Hudi.
 */
public class KafkaConnectWriterProvider implements ConnectWriterProvider<WriteStatus> {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaConnectWriterProvider.class);

  private final KafkaConnectConfigs connectConfigs;
  private final HoodieEngineContext context;
  private final HoodieWriteConfig writeConfig;
  private final HoodieJavaWriteClient<HoodieAvroPayload> hudiJavaClient;
  private final KeyGenerator keyGenerator;
  private final SchemaProvider schemaProvider;

  public KafkaConnectWriterProvider(
      KafkaConnectConfigs connectConfigs,
      TopicPartition partition) throws HoodieException {
    this.connectConfigs = connectConfigs;
    StorageConfiguration<Configuration> storageConf =
        KafkaConnectUtils.getDefaultStorageConf(connectConfigs);

    try {
      this.schemaProvider = StringUtils.isNullOrEmpty(connectConfigs.getSchemaProviderClass()) ? null
          : (SchemaProvider) ReflectionUtils.loadClass(connectConfigs.getSchemaProviderClass(),
          new TypedProperties(connectConfigs.getProps()));

      this.keyGenerator = HoodieAvroKeyGeneratorFactory.createKeyGenerator(
          new TypedProperties(connectConfigs.getProps()));

      // This is the writeConfig for the writers for the individual Transaction Coordinators
      writeConfig = HoodieWriteConfig.newBuilder()
          .withEngineType(EngineType.JAVA)
          .withProperties(connectConfigs.getProps())
          .withFileIdPrefixProviderClassName(KafkaConnectFileIdPrefixProvider.class.getName())
          .withProps(Collections.singletonMap(
              KafkaConnectFileIdPrefixProvider.KAFKA_CONNECT_PARTITION_ID,
              String.valueOf(partition)))
          .withSchema(schemaProvider.getSourceSchema().toString())
          .withAutoCommit(false)
          .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build())
          // participants should not trigger table services, and leave it to the coordinator
          .withArchivalConfig(HoodieArchivalConfig.newBuilder().withAutoArchive(false).build())
          .withCleanConfig(HoodieCleanConfig.newBuilder().withAutoClean(false).build())
          .withCompactionConfig(HoodieCompactionConfig.newBuilder().withInlineCompaction(false).build())
          .withClusteringConfig(HoodieClusteringConfig.newBuilder().withInlineClustering(false).build())
          .withWritesFileIdEncoding(1)
          .build();

      context = new HoodieJavaEngineContext(storageConf);

      hudiJavaClient = new HoodieJavaWriteClient<>(context, writeConfig);
    } catch (Throwable e) {
      throw new HoodieException("Fatal error instantiating Hudi Write Provider ", e);
    }
  }

  public AbstractConnectWriter getWriter(String commitTime) {
    return new BufferedConnectWriter(
        context,
        hudiJavaClient,
        commitTime,
        connectConfigs,
        writeConfig,
        keyGenerator,
        schemaProvider);
  }
}
