package org.apache.hudi.config;

import org.apache.hudi.common.config.DefaultHoodieConfig;
import org.apache.hudi.config.HoodieMemoryConfig.Builder;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import static org.apache.hudi.common.model.HoodiePayloadProps.DEFAULT_PAYLOAD_ORDERING_FIELD_VAL;
import static org.apache.hudi.common.model.HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP;

/**
 * Hoodie payload related configs
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

    public HoodiePayloadConfig build() {
      HoodiePayloadConfig config = new HoodiePayloadConfig(props);
      setDefaultOnCondition(props, !props.containsKey(PAYLOAD_ORDERING_FIELD_PROP), DEFAULT_PAYLOAD_ORDERING_FIELD_VAL,
          String.valueOf(DEFAULT_PAYLOAD_ORDERING_FIELD_VAL));
      return config;
    }
  }

}
