/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.hudi.writers;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.connect.writers.KafkaConnectConfigs;
import org.apache.hudi.connect.writers.KafkaConnectTransactionServices;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestKafkaConnectTransactionServices {

  @TempDir
  Path path;

  @Test
  public void testTableCreation() {
    Properties props = new Properties();
    props.put("hoodie.table.name", "test");
    props.put("hoodie.base.path", path);
    props.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "partition_path");
    props.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "key");
    props.put("hoodie.table.version", "6");
    KafkaConnectConfigs configs = KafkaConnectConfigs.newBuilder()
        .withProperties(props)
        .build();
    new KafkaConnectTransactionServices(configs);

    HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(path.toFile().getPath());
    assertEquals(HoodieTableVersion.SIX, metaClient.getTableConfig().getTableVersion());
    assertEquals(HoodieTableType.COPY_ON_WRITE, metaClient.getTableConfig().getTableType());
  }
}
