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

package org.apache.hudi.metrics;

import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.JsonUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;

import com.codahale.metrics.MetricRegistry;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class MetricsFileSystemReporter extends MetricsReporter {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsFileSystemReporter.class);
  private MetricRegistry metricRegistry;
  private SerializableConfiguration hadoopConf;
  private String metricsPath;
  private HoodieWriteConfig config;
  private FileSystem fs;
  private ScheduledExecutorService executor;
  private static final String META_FOLDER_NAME = "/.hoodie";
  private static final String METRICS_FOLDER_NAME = "/metrics";
  private static final String METRICS_FILE_NAME = "_metrics.json";

  public MetricsFileSystemReporter(HoodieWriteConfig config, MetricRegistry metricRegistry, SerializableConfiguration hadoopConf) {
    this.config = config;
    this.metricRegistry = metricRegistry;
    this.hadoopConf = hadoopConf;
    this.metricsPath = config.getMetricsFileSystemReporterPath();
    if (StringUtils.isNullOrEmpty(this.metricsPath)) {
      this.metricsPath = this.config.getBasePath() + META_FOLDER_NAME + METRICS_FOLDER_NAME;
    }
    this.fs = getFs();
    executor = Executors.newScheduledThreadPool(1);
  }

  @Override
  public void start() {
    if (config.getMetricsFileSystemSchedule()) {
      int period = config.getFileSystemReportPeriodSeconds();
      executor.scheduleAtFixedRate(() -> {
        report();
      }, period, period, TimeUnit.SECONDS);
    }
  }

  @Override
  public void report() {
    Path metricsFolder = new Path(metricsPath);
    try {
      if (!fs.exists(metricsFolder)) {
        fs.mkdirs(metricsFolder);
      }
      Path metricsFilePath = new Path(metricsFolder, getFileName(config.getMetricsFileSystemNamePrefix()));
      FSDataOutputStream out = fs.create(metricsFilePath, true);
      Map<String, String> metricsStrMap = metricRegistry.getNames().stream()
          .filter(name -> metricRegistry.getGauges().containsKey(name) || metricRegistry.getCounters().containsKey(name))
          .collect(Collectors.toMap(
              name -> name,
              name -> {
                if (metricRegistry.getGauges().containsKey(name)) {
                  return metricRegistry.getGauges().get(name).getValue().toString();
                } else {
                  return String.valueOf(metricRegistry.getCounters().get(name).getCount());
                }
              }
          ));
      byte[] writeresult = JsonUtils.getObjectMapper().writeValueAsBytes(metricsStrMap);
      out.write(writeresult);
      out.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void stop() {
    try {
      fs.close();
      executor.shutdown();
    } catch (IOException e) {
      LOG.error("Error occurred when close metrics reporter. " + e);
    }
  }

  private String getFileName(String tableName) {
    if (StringUtils.isNullOrEmpty(tableName)) {
      tableName = getTableName(config.getBasePath());
    }
    return tableName + METRICS_FILE_NAME;
  }

  public static String getTableName(String basePath) {
    if (basePath.endsWith("/")) {
      basePath = basePath.substring(0, basePath.length() - 1);
    }
    int lastIndex = basePath.lastIndexOf('/');
    return basePath.substring(lastIndex + 1);
  }

  private FileSystem getFs() {
    return FSUtils.getFs(metricsPath, hadoopConf.newCopy());
  }
}
