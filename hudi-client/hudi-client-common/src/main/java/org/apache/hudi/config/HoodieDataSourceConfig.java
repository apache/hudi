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

package org.apache.hudi.config;

import org.apache.hudi.common.config.DefaultHoodieConfig;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.keygen.SimpleAvroKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class HoodieDataSourceConfig extends DefaultHoodieConfig {

  public static final String TABLE_NAME_PROP = HoodieWriteConfig.TABLE_NAME;
  public static final String PRECOMBINE_FIELD_PROP = "hoodie.datasource.write.precombine.field";
  public static final String RECORDKEY_FIELD_PROP = KeyGeneratorOptions.RECORDKEY_FIELD_OPT_KEY;
  public static final String PARTITIONPATH_FIELD_PROP = KeyGeneratorOptions.PARTITIONPATH_FIELD_OPT_KEY;

  public static final String WRITE_PAYLOAD_CLASS = "hoodie.datasource.write.payload.class";
  public static final String DEFAULT_WRITE_PAYLOAD_CLASS = OverwriteWithLatestAvroPayload.class.getName();
  public static final String KEYGENERATOR_CLASS_PROP = "hoodie.datasource.write.keygenerator.class";
  public static final String DEFAULT_KEYGENERATOR_CLASS = SimpleAvroKeyGenerator.class.getName();

  public HoodieDataSourceConfig(Properties props) {
    super(props);
  }

  public static class Builder {

    private final Properties props = new Properties();

    public HoodieDataSourceConfig.Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        this.props.load(reader);
        return this;
      }
    }

    public HoodieDataSourceConfig.Builder fromProperties(Properties props) {
      this.props.putAll(props);
      return this;
    }

    public Builder withTableName(String tableName) {
      props.setProperty(TABLE_NAME_PROP, tableName);
      return this;
    }

    public Builder withPreCombineField(String preCombineField) {
      props.setProperty(PRECOMBINE_FIELD_PROP, preCombineField);
      return this;
    }

    public Builder withWritePayLoad(String payload) {
      props.setProperty(WRITE_PAYLOAD_CLASS, payload);
      return this;
    }

    public Builder withRecordKey(String recordKey) {
      props.setProperty(RECORDKEY_FIELD_PROP, recordKey);
      return this;
    }

    public Builder withPartitionField(String partitionField) {
      props.setProperty(PARTITIONPATH_FIELD_PROP, partitionField);
      return this;
    }

    public Builder withKeyGenerator(String keyGeneratorClass) {
      props.setProperty(KEYGENERATOR_CLASS_PROP, keyGeneratorClass);
      return this;
    }

    public HoodieDataSourceConfig build() {
      HoodieDataSourceConfig config = new HoodieDataSourceConfig(props);
      setDefaultOnCondition(props, !props.containsKey(KEYGENERATOR_CLASS_PROP),
          KEYGENERATOR_CLASS_PROP, DEFAULT_KEYGENERATOR_CLASS);
      setDefaultOnCondition(props, !props.containsKey(WRITE_PAYLOAD_CLASS),
          WRITE_PAYLOAD_CLASS, DEFAULT_WRITE_PAYLOAD_CLASS);
      return config;
    }
  }
}
