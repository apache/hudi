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

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import static org.apache.hudi.common.model.HoodiePayloadProps.DEFAULT_PAYLOAD_EVENT_TIME_FIELD_VAL;
import static org.apache.hudi.common.model.HoodiePayloadProps.DEFAULT_PAYLOAD_ORDERING_FIELD_VAL;
import static org.apache.hudi.common.model.HoodiePayloadProps.PAYLOAD_EVENT_TIME_FIELD_PROP;
import static org.apache.hudi.common.model.HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP;

/**
 * Hoodie payload related configs.
 */
public class HoodiePayloadConfig extends DefaultHoodieConfig {

  public HoodiePayloadConfig(Properties props) {
    super(props);
  }

  public static HoodiePayloadConfig.Builder newBuilder() {
    return new HoodiePayloadConfig.Builder();
  }

  public static class Builder {

    private final Properties props = new Properties();

    public Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        this.props.load(reader);
        return this;
      }
    }

    public Builder fromProperties(Properties props) {
      this.props.putAll(props);
      return this;
    }

    public Builder withPayloadOrderingField(String payloadOrderingField) {
      props.setProperty(PAYLOAD_ORDERING_FIELD_PROP, String.valueOf(payloadOrderingField));
      return this;
    }

    public Builder withPayloadEventTimeField(String payloadEventTimeField) {
      props.setProperty(PAYLOAD_EVENT_TIME_FIELD_PROP, String.valueOf(payloadEventTimeField));
      return this;
    }

    public HoodiePayloadConfig build() {
      HoodiePayloadConfig config = new HoodiePayloadConfig(props);
      setDefaultOnCondition(props, !props.containsKey(PAYLOAD_ORDERING_FIELD_PROP), PAYLOAD_ORDERING_FIELD_PROP,
          String.valueOf(DEFAULT_PAYLOAD_ORDERING_FIELD_VAL));
      setDefaultOnCondition(props, !props.containsKey(PAYLOAD_EVENT_TIME_FIELD_PROP), PAYLOAD_EVENT_TIME_FIELD_PROP,
              String.valueOf(DEFAULT_PAYLOAD_EVENT_TIME_FIELD_VAL));
      return config;
    }
  }

}
