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

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import static org.apache.hudi.common.model.HoodiePayloadProps.PAYLOAD_EVENT_TIME_FIELD_PROP_KEY;
import static org.apache.hudi.common.model.HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY;

/**
 * Hoodie payload related configs.
 */
public class HoodiePayloadConfig extends HoodieConfig {

  public static final ConfigProperty<String> PAYLOAD_ORDERING_FIELD_PROP = ConfigProperty
      .key(PAYLOAD_ORDERING_FIELD_PROP_KEY)
      .defaultValue("ts")
      .withDocumentation("Property to hold the payload ordering field name");

  public static final ConfigProperty<String> PAYLOAD_EVENT_TIME_FIELD_PROP = ConfigProperty
      .key(PAYLOAD_EVENT_TIME_FIELD_PROP_KEY)
      .defaultValue("ts")
      .withDocumentation("Property for payload event time field");

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

    public Builder withPayloadOrderingField(String payloadOrderingField) {
      payloadConfig.setValue(PAYLOAD_ORDERING_FIELD_PROP, String.valueOf(payloadOrderingField));
      return this;
    }

    public Builder withPayloadEventTimeField(String payloadEventTimeField) {
      payloadConfig.setValue(PAYLOAD_EVENT_TIME_FIELD_PROP, String.valueOf(payloadEventTimeField));
      return this;
    }

    public HoodiePayloadConfig build() {
      payloadConfig.setDefaults(HoodiePayloadConfig.class.getName());
      return payloadConfig;
    }
  }

}
