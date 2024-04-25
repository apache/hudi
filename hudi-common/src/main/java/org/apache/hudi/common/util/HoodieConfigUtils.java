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

package org.apache.hudi.common.util;

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodiePayloadProps;
import org.apache.hudi.common.model.RecordPayloadType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.config.HadoopConfigUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;

import static org.apache.hudi.common.config.HoodieReaderConfig.USE_NATIVE_HFILE_READER;
import static org.apache.hudi.common.table.HoodieTableConfig.TABLE_CHECKSUM;

public class HoodieConfigUtils {
  public static final String STREAMER_CONFIG_PREFIX = "hoodie.streamer.";
  @Deprecated
  public static final String DELTA_STREAMER_CONFIG_PREFIX = "hoodie.deltastreamer.";
  public static final String SCHEMAPROVIDER_CONFIG_PREFIX = STREAMER_CONFIG_PREFIX + "schemaprovider.";
  @Deprecated
  public static final String OLD_SCHEMAPROVIDER_CONFIG_PREFIX = DELTA_STREAMER_CONFIG_PREFIX + "schemaprovider.";
  /**
   * Config stored in hive serde properties to tell query engine (spark/flink) to
   * read the table as a read-optimized table when this config is true.
   */
  public static final String IS_QUERY_AS_RO_TABLE = "hoodie.query.as.ro.table";

  /**
   * Config stored in hive serde properties to tell query engine (spark) the
   * location to read.
   */
  public static final String TABLE_SERDE_PATH = "path";

  public static final HoodieConfig DEFAULT_HUDI_CONFIG_FOR_READER = new HoodieConfig();

  private static final Logger LOG = LoggerFactory.getLogger(HoodieConfigUtils.class);

  /**
   * Get ordering field.
   */
  public static String getOrderingField(Properties properties) {
    String orderField = null;
    if (properties.containsKey(HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY)) {
      orderField = properties.getProperty(HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY);
    } else if (properties.containsKey("hoodie.datasource.write.precombine.field")) {
      orderField = properties.getProperty("hoodie.datasource.write.precombine.field");
    } else if (properties.containsKey(HoodieTableConfig.PRECOMBINE_FIELD.key())) {
      orderField = properties.getProperty(HoodieTableConfig.PRECOMBINE_FIELD.key());
    }
    return orderField;
  }

  /**
   * Get payload class.
   */
  public static String getPayloadClass(Properties properties) {
    return RecordPayloadType.getPayloadClassName(new HoodieConfig(properties));
  }

  public static TypedProperties fetchConfigs(
      HoodieStorage storage,
      String metaPath,
      String propertiesFile,
      String propertiesBackupFile,
      int maxReadRetries,
      int maxReadRetryDelayInMs) throws IOException {
    StoragePath cfgPath = new StoragePath(metaPath, propertiesFile);
    StoragePath backupCfgPath = new StoragePath(metaPath, propertiesBackupFile);
    int readRetryCount = 0;
    boolean found = false;

    TypedProperties props = new TypedProperties();
    while (readRetryCount++ < maxReadRetries) {
      for (StoragePath path : Arrays.asList(cfgPath, backupCfgPath)) {
        // Read the properties and validate that it is a valid file
        try (InputStream is = storage.open(path)) {
          props.clear();
          props.load(is);
          found = true;
          if (props.containsKey(TABLE_CHECKSUM.key())) {
            ValidationUtils.checkArgument(HoodieTableConfig.validateChecksum(props));
          }
          return props;
        } catch (IOException e) {
          LOG.warn(String.format("Could not read properties from %s: %s", path, e));
        } catch (IllegalArgumentException e) {
          LOG.warn(String.format("Invalid properties file %s: %s", path, props));
        }
      }

      // Failed to read all files so wait before retrying. This can happen in cases of parallel updates to the properties.
      try {
        Thread.sleep(maxReadRetryDelayInMs);
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while waiting");
      }
    }

    // If we are here then after all retries either no properties file was found or only an invalid file was found.
    if (found) {
      throw new IllegalArgumentException(
          "hoodie.properties file seems invalid. Please check for left over `.updated` files if any, manually copy it to hoodie.properties and retry");
    } else {
      throw new HoodieIOException("Could not load Hoodie properties from " + cfgPath);
    }
  }

  public static HoodieConfig getReaderConfigs(Configuration conf) {
    HoodieConfig config = new HoodieConfig();
    config.setAll(DEFAULT_HUDI_CONFIG_FOR_READER.getProps());
    config.setValue(USE_NATIVE_HFILE_READER,
        Boolean.toString(HadoopConfigUtils.getBooleanWithAltKeys(conf, USE_NATIVE_HFILE_READER)));
    return config;
  }
}
