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
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.fileid.KafkaConnectFileIdPrefixProvider;
import org.apache.hudi.fileid.RandomFileIdPrefixProvider;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.keygen.factory.HoodieAvroKeyGeneratorFactory;
import org.apache.hudi.schema.SchemaProvider;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

public class HudiConnectWriterProvider {

  private static final Logger LOG = LoggerFactory.getLogger(HudiConnectWriterProvider.class);

  protected final HudiConnectConfigs connectConfigs;
  /**
   * Extract the key for the target table.
   */
  protected final Configuration hadoopConf;
  protected final String tableBasePath;
  protected final String tableName;
  protected final FileSystem fs;
  protected final HoodieEngineContext context;
  protected final HoodieWriteConfig writeConfig;
  protected final HoodieJavaWriteClient<HoodieAvroPayload> hoodieJavaWriteClient;
  protected final KeyGenerator keyGenerator;
  protected final SchemaProvider schemaProvider;
  private final TopicPartition partition;

  public HudiConnectWriterProvider(
      HudiConnectConfigs connectConfigs,
      TopicPartition partition) throws HoodieException {
    this.connectConfigs = connectConfigs;
    this.partition = partition;
    hadoopConf = new Configuration();
    hadoopConf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

    try {
      this.schemaProvider = StringUtils.isNullOrEmpty(connectConfigs.getSchemaProviderClass()) ? null
          : (SchemaProvider) ReflectionUtils.loadClass(connectConfigs.getSchemaProviderClass(),
          new TypedProperties(connectConfigs.getProps()));

      this.keyGenerator = HoodieAvroKeyGeneratorFactory.createKeyGenerator(
          new TypedProperties(connectConfigs.getProps()));

      // Create the write client to write some records in
      writeConfig = HoodieWriteConfig.newBuilder()
          .withProperties(connectConfigs.getProps())
          .withFileIdPrefixProviderClassName(KafkaConnectFileIdPrefixProvider.class.getName())
          .withProps(Collections.singletonMap(
              KafkaConnectFileIdPrefixProvider.KAFKA_CONNECT_PARTITION_ID,
              String.valueOf(partition)))
          .withSchema(schemaProvider.getSourceSchema().toString())
          .withAutoCommit(false)
          .withEnableInsertAvoidTransitionInflight()
          .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build())
          .build();

      tableBasePath = writeConfig.getBasePath();
      tableName = writeConfig.getTableName();
      fs = FSUtils.getFs(tableBasePath, hadoopConf);
      context = new HoodieJavaEngineContext(hadoopConf);
      hoodieJavaWriteClient = new HoodieJavaWriteClient<>(context, writeConfig);
    } catch (Throwable e) {
      throw new HoodieException("Fatal error instantiating Hudi Write Provider ", e);
    }
  }

  public AbstractHudiConnectWriter getWriter(String commitTime) {
    return new HudiConnectBufferedWriter(
        context,
        hoodieJavaWriteClient,
        commitTime,
        connectConfigs,
        writeConfig,
        keyGenerator,
        schemaProvider);
  }
}
