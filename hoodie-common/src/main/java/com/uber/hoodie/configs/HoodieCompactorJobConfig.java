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

public class HoodieCompactorJobConfig extends AbstractJobConfig {

  @Parameter(names = {"--base-path",
      "-bp"}, description = "Base path for the dataset", required = true)
  public String basePath = null;
  @Parameter(names = {"--table-name", "-tn"}, description = "Table name", required = true)
  public String tableName = null;
  @Parameter(names = {"--instant-time",
      "-sp"}, description = "Compaction Instant time", required = true)
  public String compactionInstantTime = null;
  @Parameter(names = {"--parallelism",
      "-pl"}, description = "Parallelism for hoodie insert", required = true)
  public int parallelism = 1;
  @Parameter(names = {"--schema-file",
      "-sf"}, description = "path for Avro schema file", required = true)
  public String schemaFile = null;
  @Parameter(names = {"--spark-master", "-ms"}, description = "Spark master", required = false)
  public String sparkMaster = null;
  @Parameter(names = {"--spark-memory",
      "-sm"}, description = "spark memory to use", required = true)
  public String sparkMemory = null;
  @Parameter(names = {"--retry", "-rt"}, description = "number of retries", required = false)
  public int retry = 0;
  @Parameter(names = {"--schedule", "-sc"}, description = "Schedule compaction", required = false, arity = 1)
  public Boolean runSchedule = false;
  @Parameter(names = {"--strategy", "-st"}, description = "Stratgey Class", required = false)
  public String strategyClassName = "com.uber.hoodie.io.compact.strategy.UnBoundedCompactionStrategy";
}
