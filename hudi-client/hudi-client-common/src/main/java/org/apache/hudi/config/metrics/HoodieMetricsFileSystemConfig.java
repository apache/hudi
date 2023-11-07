/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.config.metrics;

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;

import java.util.Properties;

@ConfigClassProperty(
    name = "Metrics Configurations for file system",
    groupName = ConfigGroups.Names.METRICS,
    description =
        "Enables reporting on Hudi metrics using file system. "
            + " Hudi publishes metrics on every commit, clean, rollback etc.")
public class HoodieMetricsFileSystemConfig extends HoodieConfig {

  public static final String METRICS_FILESYSTEM_PREFIX = "hoodie.metrics.filesystem";

  public static final ConfigProperty<String> FILESYSTEM_METRICS_REPORTER_PATH = ConfigProperty
      .key(METRICS_FILESYSTEM_PREFIX + ".reporter.path")
      .noDefaultValue()
      .sinceVersion("1.0.0")
      .withDocumentation("The path for persisting Hudi storage metrics files.");

  public static final ConfigProperty<String> METRICS_FILE_NAME_PREFIX = ConfigProperty
      .key(METRICS_FILESYSTEM_PREFIX + ".metric.prefix")
      .defaultValue("")
      .sinceVersion("1.0.0")
      .withDocumentation("The prefix for Hudi storage metrics persistence file names.");

  public static final ConfigProperty<Boolean> OVERWRITE_FILE = ConfigProperty
      .key(METRICS_FILESYSTEM_PREFIX + ".overwrite.file")
      .defaultValue(true)
      .sinceVersion("1.0.0")
      // TODO：If set to false, a timestamp can be added to the metrics files.
      .withDocumentation("Whether to override the same metrics file for the same table.");

  public static final ConfigProperty<Boolean> FILESYSTEM_REPORT_SCHEDULE_ENABLE = ConfigProperty
      .key(METRICS_FILESYSTEM_PREFIX + ".schedule.enable")
      .defaultValue(false)
      .sinceVersion("1.0.0")
      .withDocumentation("Whether to enable scheduled output of metrics to the file system, default is off, only need to output the final result to the file system.");

  public static final ConfigProperty<Integer> FILESYSTEM_REPORT_PERIOD_IN_SECONDS = ConfigProperty
      .key(METRICS_FILESYSTEM_PREFIX + ".report.period.seconds")
      .defaultValue(6)
      .sinceVersion("1.0.0")
      .withDocumentation("File system reporting period in seconds. Default to 60.");

  public HoodieMetricsFileSystemConfig() {
    super();
  }

  static Builder newBuilder() {
    return new Builder();
  }

  static class Builder {

    private final HoodieMetricsFileSystemConfig hoodieMetricsFileSystemConfig = new HoodieMetricsFileSystemConfig();

    public HoodieMetricsFileSystemConfig.Builder fromProperties(Properties props) {
      this.hoodieMetricsFileSystemConfig.getProps().putAll(props);
      return this;
    }

    public HoodieMetricsFileSystemConfig build() {
      hoodieMetricsFileSystemConfig.setDefaults(HoodieMetricsFileSystemConfig.class.getName());
      return hoodieMetricsFileSystemConfig;
    }
  }
}
