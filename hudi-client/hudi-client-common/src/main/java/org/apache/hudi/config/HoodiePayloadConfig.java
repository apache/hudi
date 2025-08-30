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

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.StringUtils;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import static org.apache.hudi.common.model.HoodiePayloadProps.PAYLOAD_EVENT_TIME_FIELD_PROP_KEY;
import static org.apache.hudi.common.model.HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY;

/**
 * Hoodie payload related configs.
 */
@ConfigClassProperty(name = "Payload Configurations",
    groupName = ConfigGroups.Names.RECORD_PAYLOAD,
    description = "Payload related configs, that can be leveraged to "
        + "control merges based on specific business fields in the data.")
public class HoodiePayloadConfig extends HoodieConfig {

  public static final ConfigProperty<String> EVENT_TIME_FIELD = ConfigProperty
      .key(PAYLOAD_EVENT_TIME_FIELD_PROP_KEY)
      .defaultValue("ts")
      .markAdvanced()
      .withDocumentation("Table column/field name to derive timestamp associated with the records. This can"
          + "be useful for e.g, determining the freshness of the table.");

  public static final ConfigProperty<String> PAYLOAD_CLASS_NAME = ConfigProperty
      .key("hoodie.compaction.payload.class")
      .defaultValue(HoodieTableConfig.DEFAULT_PAYLOAD_CLASS_NAME)
      .markAdvanced()
      .withDocumentation("This needs to be same as class used during insert/upserts. Just like writing, compaction also uses "
        + "the record payload class to merge records in the log against each other, merge again with the base file and "
        + "produce the final record to be written after compaction.");

  /** @deprecated Use {@link HoodieWriteConfig#PRECOMBINE_FIELD_NAME} and its methods instead */
  @Deprecated
  public static final ConfigProperty<String> ORDERING_FIELDS = ConfigProperty
      .key(PAYLOAD_ORDERING_FIELD_PROP_KEY)
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("Table column/field name to order records that have the same key, before "
          + "merging and writing to storage.");

  /** @deprecated Use {@link #PAYLOAD_CLASS_NAME} and its methods instead */
  @Deprecated
  public static final String PAYLOAD_CLASS_PROP = PAYLOAD_CLASS_NAME.key();

  private HoodiePayloadConfig() {
    super();
  }

  public static HoodiePayloadConfig.Builder newBuilder() {
    return new HoodiePayloadConfig.Builder();
  }

  public static class Builder {

    private final HoodiePayloadConfig payloadConfig = new HoodiePayloadConfig();

    public Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        this.payloadConfig.getProps().load(reader);
        return this;
      }
    }

    public Builder fromProperties(Properties props) {
      this.payloadConfig.getProps().putAll(props);
      return this;
    }

    public Builder withPayloadOrderingFields(String payloadOrderingFields) {
      if (StringUtils.nonEmpty(payloadOrderingFields)) {
        payloadConfig.setValue(ORDERING_FIELDS, payloadOrderingFields);
      }
      return this;
    }

    public Builder withPayloadEventTimeField(String payloadEventTimeField) {
      payloadConfig.setValue(EVENT_TIME_FIELD, String.valueOf(payloadEventTimeField));
      return this;
    }

    public HoodiePayloadConfig.Builder withPayloadClass(String payloadClassName) {
      payloadConfig.setValue(PAYLOAD_CLASS_NAME, payloadClassName);
      return this;
    }

    public HoodiePayloadConfig build() {
      payloadConfig.setDefaults(HoodiePayloadConfig.class.getName());
      return payloadConfig;
    }
  }

}
