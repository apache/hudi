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

public class HoodieCompactionAdminToolJobConfig extends AbstractCommandConfig {

  /**
   * Operation Types
   */
  public enum Operation {
    VALIDATE,
    UNSCHEDULE_PLAN,
    UNSCHEDULE_FILE,
    REPAIR
  }

  @Parameter(names = {"--operation", "-op"}, description = "Operation", required = true)
  public Operation operation = Operation.VALIDATE;
  @Parameter(names = {"--base-path", "-bp"}, description = "Base path for the dataset", required = true)
  public String basePath = null;
  @Parameter(names = {"--instant-time", "-in"}, description = "Compaction Instant time", required = false)
  public String compactionInstantTime = null;
  @Parameter(names = {"--partition-path", "-pp"}, description = "Partition Path", required = false)
  public String partitionPath = null;
  @Parameter(names = {"--file-id", "-id"}, description = "File Id", required = false)
  public String fileId = null;
  @Parameter(names = {"--parallelism", "-pl"}, description = "Parallelism for hoodie insert", required = false)
  public int parallelism = 3;
  @Parameter(names = {"--spark-master", "-ms"}, description = "Spark master", required = true)
  public String sparkMaster = null;
  @Parameter(names = {"--spark-memory", "-sm"}, description = "spark memory to use", required = true)
  public String sparkMemory = null;
  @Parameter(names = {"--dry-run", "-dr"}, description = "Dry Run Mode", required = false, arity = 1)
  public boolean dryRun = false;
  @Parameter(names = {"--skip-validation", "-sv"}, description = "Skip Validation", required = false, arity = 1)
  public boolean skipValidation = false;
  @Parameter(names = {"--output-path", "-ot"}, description = "Output Path", required = false, arity = 1)
  public String outputPath = null;
  @Parameter(names = {"--print-output", "-pt"}, description = "Print Output", required = false)
  public boolean printOutput = true;
}
