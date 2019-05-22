/*
 * Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.configs;

import com.beust.jcommander.IValueValidator;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import java.util.Arrays;
import java.util.List;

public class HDFSParquetImporterJobConfig extends AbstractCommandConfig {

  public static class FormatValidator implements IValueValidator<String> {

    List<String> validFormats = Arrays.asList("parquet");

    @Override
    public void validate(String name, String value) throws ParameterException {
      if (value == null || !validFormats.contains(value)) {
        throw new ParameterException(String.format(
            "Invalid format type: value:%s: supported formats:%s", value, validFormats));
      }
    }
  }

  @Parameter(names = {"--command", "-c"},
      description = "Write command Valid values are insert(default)/upsert",
      required = false)
  public String command = "INSERT";
  @Parameter(names = {"--src-path",
      "-sp"}, description = "Base path for the input dataset", required = true)
  public String srcPath = null;
  @Parameter(names = {"--target-path",
      "-tp"}, description = "Base path for the target hoodie dataset", required = true)
  public String targetPath = null;
  @Parameter(names = {"--table-name", "-tn"}, description = "Table name", required = true)
  public String tableName = null;
  @Parameter(names = {"--table-type", "-tt"}, description = "Table type", required = true)
  public String tableType = null;
  @Parameter(names = {"--row-key-field",
      "-rk"}, description = "Row key field name", required = true)
  public String rowKey = null;
  @Parameter(names = {"--partition-key-field",
      "-pk"}, description = "Partition key field name", required = true)
  public String partitionKey = null;
  @Parameter(names = {"--parallelism",
      "-pl"}, description = "Parallelism for hoodie insert", required = true)
  public int parallelism = 1;
  @Parameter(names = {"--schema-file",
      "-sf"}, description = "path for Avro schema file", required = true)
  public String schemaFile = null;
  @Parameter(names = {"--format",
      "-f"}, description = "Format for the input data.", required = false, validateValueWith =
      FormatValidator.class)
  public String format = null;
  @Parameter(names = {"--spark-master", "-ms"}, description = "Spark master", required = false)
  public String sparkMaster = null;
  @Parameter(names = {"--spark-memory",
      "-sm"}, description = "spark memory to use", required = true)
  public String sparkMemory = null;
  @Parameter(names = {"--retry", "-rt"}, description = "number of retries", required = false)
  public int retry = 0;
}
