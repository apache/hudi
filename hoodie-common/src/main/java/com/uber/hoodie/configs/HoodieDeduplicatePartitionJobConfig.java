/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.configs;

import com.beust.jcommander.Parameter;

public class HoodieDeduplicatePartitionJobConfig extends AbstractCommandConfig {

  @Parameter(names = {"--duplicated-partition-path",
      "-dpp"}, description = "Duplicated partition path for deduplication", required = true)
  public String duplicatedPartitionPath = null;

  @Parameter(names = {"--repaired-output-path",
      "-rop"}, description = "Repaired output path for deduplication", required = true)
  public String repairedOutputPath = null;

  @Parameter(names = {"--base-path",
      "-bp"}, description = "Base path for the hoodie dataset", required = true)
  public String basePath = null;
}
