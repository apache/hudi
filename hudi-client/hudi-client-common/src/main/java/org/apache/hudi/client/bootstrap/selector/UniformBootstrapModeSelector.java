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

import org.apache.hudi.avro.model.HoodieFileStatus;
import org.apache.hudi.client.bootstrap.BootstrapMode;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A bootstrap selector which employs same bootstrap mode for all partitions.
 */
public abstract class UniformBootstrapModeSelector extends BootstrapModeSelector {

  private final BootstrapMode bootstrapMode;

  public UniformBootstrapModeSelector(HoodieWriteConfig bootstrapConfig, BootstrapMode bootstrapMode) {
    super(bootstrapConfig);
    this.bootstrapMode = bootstrapMode;
  }

  @Override
  public Map<BootstrapMode, List<String>> select(List<Pair<String, List<HoodieFileStatus>>> partitions) {
    return partitions.stream().map(p -> Pair.of(bootstrapMode, p))
        .collect(Collectors.groupingBy(Pair::getKey, Collectors.mapping(x -> x.getValue().getKey(),
            Collectors.toList())));
  }
}