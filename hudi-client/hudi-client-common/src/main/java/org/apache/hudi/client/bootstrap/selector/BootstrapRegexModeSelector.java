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

package org.apache.hudi.client.bootstrap.selector;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.hudi.avro.model.HoodieFileStatus;
import org.apache.hudi.client.bootstrap.BootstrapMode;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class BootstrapRegexModeSelector extends BootstrapModeSelector {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LogManager.getLogger(BootstrapRegexModeSelector.class);

  private final Pattern pattern;
  private final BootstrapMode bootstrapModeOnMatch;
  private final BootstrapMode defaultMode;

  public BootstrapRegexModeSelector(HoodieWriteConfig writeConfig) {
    super(writeConfig);
    this.pattern = Pattern.compile(writeConfig.getBootstrapModeSelectorRegex());
    this.bootstrapModeOnMatch = writeConfig.getBootstrapModeForRegexMatch();
    this.defaultMode = BootstrapMode.FULL_RECORD.equals(bootstrapModeOnMatch)
        ? BootstrapMode.METADATA_ONLY : BootstrapMode.FULL_RECORD;
    LOG.info("Default Mode :" + defaultMode + ", on Match Mode :" + bootstrapModeOnMatch);
  }

  @Override
  public Map<BootstrapMode, List<String>> select(List<Pair<String, List<HoodieFileStatus>>> partitions) {
    return partitions.stream()
        .map(p -> Pair.of(pattern.matcher(p.getKey()).matches() ? bootstrapModeOnMatch : defaultMode, p.getKey()))
        .collect(Collectors.groupingBy(Pair::getKey, Collectors.mapping(Pair::getValue, Collectors.toList())));
  }
}
