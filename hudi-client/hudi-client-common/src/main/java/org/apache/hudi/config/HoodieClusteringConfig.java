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
import org.apache.hudi.table.action.clustering.update.RejectUpdateStrategy;
import org.apache.hudi.table.action.clustering.update.UpdateStrategy;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class HoodieClusteringConfig extends DefaultHoodieConfig {

  public static final String CLUSTERING_UPDATES_STRATEGY_PROP = "hoodie.clustering.updates.strategy";
  public static final String DEFAULT_CLUSTERING_UPDATES_STRATEGY = RejectUpdateStrategy.class.getName();

  public HoodieClusteringConfig(Properties props) {
    super(props);
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private final Properties props = new Properties();

    public Builder fromProperties(Properties props) {
      this.props.putAll(props);
      return this;
    }

    public Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        this.props.load(reader);
        return this;
      }
    }

    public Builder withClusteringUpdatesStrategy(UpdateStrategy updatesStrategy) {
      props.setProperty(CLUSTERING_UPDATES_STRATEGY_PROP, updatesStrategy.getClass().getName());
      return this;
    }

    public HoodieClusteringConfig build() {
      HoodieClusteringConfig config = new HoodieClusteringConfig(props);
      setDefaultOnCondition(props, !props.containsKey(CLUSTERING_UPDATES_STRATEGY_PROP),
          CLUSTERING_UPDATES_STRATEGY_PROP, DEFAULT_CLUSTERING_UPDATES_STRATEGY);
      return config;
    }
  }
}