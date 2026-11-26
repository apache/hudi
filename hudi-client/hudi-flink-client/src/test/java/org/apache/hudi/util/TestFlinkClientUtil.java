/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.util;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.apache.hudi.util.FlinkClientUtil.FLINK_HADOOP_CONFIG_PREFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;

/**
 * Test cases for {@link FlinkClientUtil}
 */
public class TestFlinkClientUtil {
  private static final String FLINK_CRYPTO_RETRIEVER_KEY = FLINK_HADOOP_CONFIG_PREFIX + "parquet.crypto.encryptor.decryptor.retriever.class";
  private static final String CRYPTO_RETRIEVER_KEY = "parquet.crypto.encryptor.decryptor.retriever.class";
  private static final String CRYPTO_RETRIEVER_CLASS = "org.apache.parquet.crypto.CryptoMetadataRetriever";

  @Test
  public void testGetHadoopConf() throws Exception {
    try (MockedStatic<GlobalConfiguration> mockedStatic = Mockito.mockStatic(GlobalConfiguration.class)) {
      Configuration flinkConf = new Configuration();
      flinkConf.setString(FLINK_CRYPTO_RETRIEVER_KEY, CRYPTO_RETRIEVER_CLASS);
      mockedStatic.when(() -> GlobalConfiguration.loadConfiguration(anyString())).thenReturn(flinkConf);
      org.apache.hadoop.conf.Configuration hadoopConf =
          FlinkClientUtil.getHadoopConf();

      assertEquals(CRYPTO_RETRIEVER_CLASS, hadoopConf.get(CRYPTO_RETRIEVER_KEY));
    }
  }

  @Test
  public void testExplicitDirectoryPreservesFlinkHadoopOptions(@TempDir Path dir) throws Exception {
    Files.writeString(dir.resolve("core-site.xml"), "<configuration><property><name>fs.defaultFS</name>"
        + "<value>hdfs://remote-cluster</value></property><property><name>" + CRYPTO_RETRIEVER_KEY
        + "</name><value>site-value</value></property></configuration>");
    try (MockedStatic<GlobalConfiguration> global = Mockito.mockStatic(GlobalConfiguration.class)) {
      Configuration flinkConf = new Configuration();
      flinkConf.setString(FLINK_CRYPTO_RETRIEVER_KEY, CRYPTO_RETRIEVER_CLASS);
      flinkConf.setString(FLINK_HADOOP_CONFIG_PREFIX + "test.marker", "flink-only");
      global.when(() -> GlobalConfiguration.loadConfiguration(anyString())).thenReturn(flinkConf);
      org.apache.hadoop.conf.Configuration hadoopConf = FlinkClientUtil.getHadoopConf(dir.toString());
      assertEquals("hdfs://remote-cluster", hadoopConf.get("fs.defaultFS"));
      assertEquals(CRYPTO_RETRIEVER_CLASS, hadoopConf.get(CRYPTO_RETRIEVER_KEY));
      assertEquals("flink-only", hadoopConf.get("test.marker"));
    }
  }

}
