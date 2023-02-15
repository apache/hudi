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
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.storage.HoodieStorageLayout;

import javax.annotation.concurrent.Immutable;
import java.util.Properties;

/**
 * Storage layout related config.
 */
@Immutable
@ConfigClassProperty(name = "Layout Configs",
    groupName = ConfigGroups.Names.WRITE_CLIENT,
    description = "Configurations that control storage layout and data distribution, "
        + "which defines how the files are organized within a table.")
public class HoodieLayoutConfig extends HoodieConfig {

  public static final ConfigProperty<String> LAYOUT_TYPE = ConfigProperty
      .key("hoodie.storage.layout.type")
      .defaultValue("DEFAULT")
      .withDocumentation("Type of storage layout. Possible options are [DEFAULT | BUCKET]");

  public static final ConfigProperty<String> LAYOUT_PARTITIONER_CLASS_NAME = ConfigProperty
      .key("hoodie.storage.layout.partitioner.class")
      .noDefaultValue()
      .withDocumentation("Partitioner class, it is used to distribute data in a specific way.");

  public static final String SIMPLE_BUCKET_LAYOUT_PARTITIONER_CLASS_NAME =
      "org.apache.hudi.table.action.commit.SparkBucketIndexPartitioner";

  private HoodieLayoutConfig() {
    super();
  }

  public static HoodieLayoutConfig.Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    public HoodieLayoutConfig layoutConfig = new HoodieLayoutConfig();

    public Builder fromProperties(Properties props) {
      this.layoutConfig.getProps().putAll(props);
      return this;
    }

    public Builder withLayoutType(String type) {
      layoutConfig.setValue(LAYOUT_TYPE, type);
      return this;
    }

    public Builder withLayoutPartitioner(String partitionerClass) {
      layoutConfig.setValue(LAYOUT_PARTITIONER_CLASS_NAME, partitionerClass);
      return this;
    }

    public HoodieLayoutConfig build() {
      setDefault();
      return layoutConfig;
    }

    private void setDefault() {
      if (layoutConfig.contains(HoodieIndexConfig.INDEX_TYPE.key())
          && layoutConfig.getString(HoodieIndexConfig.INDEX_TYPE.key()).equals(HoodieIndex.IndexType.BUCKET.name())) {
        layoutConfig.setDefaultValue(LAYOUT_TYPE, HoodieStorageLayout.LayoutType.BUCKET.name());

        // Currently, the partitioner of the SIMPLE bucket index is supported by SparkBucketIndexPartitioner only.
        if ("SIMPLE".equals(layoutConfig.getString(HoodieIndexConfig.BUCKET_INDEX_ENGINE_TYPE))) {
          layoutConfig.setDefaultValue(LAYOUT_PARTITIONER_CLASS_NAME, SIMPLE_BUCKET_LAYOUT_PARTITIONER_CLASS_NAME);
        }
      }
      layoutConfig.setDefaultValue(LAYOUT_TYPE, LAYOUT_TYPE.defaultValue());
    }
  }
}
