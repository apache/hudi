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

package org.apache.hudi.sync;

import ai.onehouse.config.models.configv1.Database;
import ai.onehouse.config.models.configv1.ParserConfig;
import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.sync.common.HoodieSyncTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.hudi.sync.LakeviewSyncConfigHolder.LAKEVIEW_METADATA_EXTRACTOR_LAKE_PATHS;

public class LakeviewSyncTool extends HoodieSyncTool implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(LakeviewSyncTool.class);

  private final List<ParserConfig> parserConfig;

  public LakeviewSyncTool(Properties props, Configuration hadoopConf) {
    super(props, hadoopConf);
    this.parserConfig = getParserConfig();
  }

  private static final Pattern LAKEVIEW_METADATA_EXTRACTOR_LAKE_PATHS_PATTERN = Pattern.compile("([^.]+)\\.databases\\.([^.]+)\\.basePaths");

  @VisibleForTesting
  public List<ParserConfig> getParserConfig() {
    Map<String, ParserConfig> lakeNameToParserConfig = new HashMap<>();
    props.forEach((key, value) -> {
      if (key.toString().startsWith(LAKEVIEW_METADATA_EXTRACTOR_LAKE_PATHS.key())) {
        String currentKey = key.toString();
        currentKey = currentKey.substring(LAKEVIEW_METADATA_EXTRACTOR_LAKE_PATHS.key().length() + 1);
        Matcher matcher = LAKEVIEW_METADATA_EXTRACTOR_LAKE_PATHS_PATTERN.matcher(currentKey);
        if (matcher.find()) {
          String lakeName = matcher.group(1);
          String databaseName = matcher.group(2);
          List<String> tableBasePaths = Arrays.asList(value.toString().split(","));

          ParserConfig currentParserConfig = lakeNameToParserConfig
              .computeIfAbsent(lakeName, lake -> ParserConfig.builder().lake(lake).databases(new ArrayList<>()).build());
          Database database = Database.builder().name(databaseName).basePaths(tableBasePaths).build();
          currentParserConfig.getDatabases().add(database);
        } else {
          LOG.warn("Couldn't parse lakes/databases from {}={}", key, value);
        }
      }
    });
    return new ArrayList<>(lakeNameToParserConfig.values());
  }

  @Override
  public void syncHoodieTable() {
    // TODO: perform sync operation
  }

  @Override
  public void close() throws Exception {
    super.close();
  }
}
