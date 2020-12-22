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

package org.apache.hudi.util;

import org.apache.hudi.HoodieFlinkStreamer;
import org.apache.hudi.common.config.DFSPropertiesConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.client.common.EngineType;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodiePayloadConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.keygen.SimpleAvroKeyGenerator;
import org.apache.hudi.schema.FilebasedSchemaProvider;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.Properties;

public class StreamerUtil {

  private static Logger LOG = LoggerFactory.getLogger(StreamerUtil.class);

  public static Properties getKafkaProps(HoodieFlinkStreamer.Config cfg) {
    Properties result = new Properties();
    result.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.kafkaBootstrapServers);
    result.put(ConsumerConfig.GROUP_ID_CONFIG, cfg.kafkaGroupId);
    return result;
  }

  public static TypedProperties getProps(HoodieFlinkStreamer.Config cfg) {
    return readConfig(
        FSUtils.getFs(cfg.propsFilePath, getHadoopConf()),
        new Path(cfg.propsFilePath), cfg.configs).getConfig();
  }


  /**
   * Read conig from files.
   */
  public static DFSPropertiesConfiguration readConfig(FileSystem fs, Path cfgPath, List<String> overriddenProps) {
    DFSPropertiesConfiguration conf;
    try {
      conf = new DFSPropertiesConfiguration(cfgPath.getFileSystem(fs.getConf()), cfgPath);
    } catch (Exception e) {
      conf = new DFSPropertiesConfiguration();
      LOG.warn("Unexpected error read props file at :" + cfgPath, e);
    }

    try {
      if (!overriddenProps.isEmpty()) {
        LOG.info("Adding overridden properties to file properties.");
        conf.addProperties(new BufferedReader(new StringReader(String.join("\n", overriddenProps))));
      }
    } catch (IOException ioe) {
      throw new HoodieIOException("Unexpected error adding config overrides", ioe);
    }

    return conf;
  }

  public static Configuration getHadoopConf() {
    return new Configuration();
  }

  public static void checkRequiredProperties(TypedProperties props, List<String> checkPropNames) {
    checkPropNames.forEach(prop -> {
      if (!props.containsKey(prop)) {
        throw new HoodieNotSupportedException("Required property " + prop + " is missing");
      }
    });
  }

  /**
   * Create a key generator class via reflection, passing in any configs needed.
   * <p>
   * If the class name of key generator is configured through the properties file, i.e., {@code props}, use the corresponding key generator class; otherwise, use the default key generator class
   * specified in {@code DataSourceWriteOptions}.
   */
  public static KeyGenerator createKeyGenerator(TypedProperties props) throws IOException {
    String keyGeneratorClass = props.getString("hoodie.datasource.write.keygenerator.class",
        SimpleAvroKeyGenerator.class.getName());
    try {
      return (KeyGenerator) ReflectionUtils.loadClass(keyGeneratorClass, props);
    } catch (Throwable e) {
      throw new IOException("Could not load key generator class " + keyGeneratorClass, e);
    }
  }

  /**
   * Create a payload class via reflection, passing in an ordering/precombine value.
   */
  public static HoodieRecordPayload createPayload(String payloadClass, GenericRecord record, Comparable orderingVal)
      throws IOException {
    try {
      return (HoodieRecordPayload) ReflectionUtils.loadClass(payloadClass,
          new Class<?>[] {GenericRecord.class, Comparable.class}, record, orderingVal);
    } catch (Throwable e) {
      throw new IOException("Could not create payload for class: " + payloadClass, e);
    }
  }

  public static HoodieWriteConfig getHoodieClientConfig(HoodieFlinkStreamer.Config cfg) {
    FileSystem fs = FSUtils.getFs(cfg.targetBasePath, getHadoopConf());
    HoodieWriteConfig.Builder builder =
        HoodieWriteConfig.newBuilder().withEngineType(EngineType.FLINK).withPath(cfg.targetBasePath).combineInput(cfg.filterDupes, true)
            .withCompactionConfig(HoodieCompactionConfig.newBuilder().withPayloadClass(cfg.payloadClassName).build())
            .withPayloadConfig(HoodiePayloadConfig.newBuilder().withPayloadOrderingField(cfg.sourceOrderingField)
                .build())
            .forTable(cfg.targetTableName)
            .withAutoCommit(false)
            .withProps(readConfig(fs, new Path(cfg.propsFilePath), cfg.configs)
                .getConfig());

    builder = builder.withSchema(new FilebasedSchemaProvider(getProps(cfg)).getTargetSchema().toString());
    HoodieWriteConfig config = builder.build();
    return config;
  }

}
