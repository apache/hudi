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

package org.apache.hudi.table.action.compact.strategy;

import org.apache.hudi.config.HoodieWriteConfig;

import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class PartitionRegexBasedCompactionStrategy extends CompactionStrategy {

  @Override
  public List<String> filterPartitionPaths(HoodieWriteConfig writeConfig, List<String> allPartitionPaths) {
    String regex = writeConfig.getCompactionSpecifyPartitionPathRegex();
    Pattern pattern = Pattern.compile(regex);
    return allPartitionPaths.stream().filter(pattern.asPredicate()).collect(Collectors.toList());
  }
}
