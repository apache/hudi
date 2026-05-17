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

package org.apache.hudi.config;

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;

import javax.annotation.concurrent.Immutable;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * Storage related config.
 */
@Immutable
@ConfigClassProperty(name = "PreCommit Validator Configurations",
    groupName = ConfigGroups.Names.SPARK_DATASOURCE,
    description = "The following set of configurations help validate new data before commits.")
public class HoodiePreCommitValidatorConfig extends HoodieConfig {

  public static final ConfigProperty<String> VALIDATOR_CLASS_NAMES = ConfigProperty
      .key("hoodie.precommit.validators")
      .defaultValue("")
      .markAdvanced()
      .withDocumentation("Comma separated list of class names that can be invoked to validate commit. "
          + "Available streaming offset validators: "
          + "org.apache.hudi.sink.validator.FlinkKafkaOffsetValidator (Flink Kafka), "
          + "org.apache.hudi.utilities.streamer.validator.SparkKafkaOffsetValidator (Spark/HoodieStreamer Kafka)");
  public static final String VALIDATOR_TABLE_VARIABLE = "<TABLE_NAME>";

  public static final ConfigProperty<String> EQUALITY_SQL_QUERIES = ConfigProperty
      .key("hoodie.precommit.validators.equality.sql.queries")
      .defaultValue("")
      .markAdvanced()
      .withDocumentation("Spark SQL queries to run on table before committing new data to validate state before and after commit."
          + " Multiple queries separated by ';' delimiter are supported."
          + " Example: \"select count(*) from \\<TABLE_NAME\\>"
          + " Note \\<TABLE_NAME\\> is replaced by table state before and after commit.");

  public static final ConfigProperty<String> SINGLE_VALUE_SQL_QUERIES = ConfigProperty
      .key("hoodie.precommit.validators.single.value.sql.queries")
      .defaultValue("")
      .markAdvanced()
      .withDocumentation("Spark SQL queries to run on table before committing new data to validate state after commit."
          + "Multiple queries separated by ';' delimiter are supported."
          + "Expected result is included as part of query separated by '#'. Example query: 'query1#result1:query2#result2'"
          + "Note \\<TABLE_NAME\\> variable is expected to be present in query.");

  public static final ConfigProperty<String> STREAMING_OFFSET_TOLERANCE_PERCENTAGE = ConfigProperty
      .key("hoodie.precommit.validators.streaming.offset.tolerance.percentage")
      .defaultValue("0.0")
      .sinceVersion("1.2.0")
      .markAdvanced()
      .withDocumentation("Tolerance percentage for streaming offset validation "
          + "(used by org.apache.hudi.client.validator.StreamingOffsetValidator "
          + "and org.apache.hudi.sink.validator.FlinkKafkaOffsetValidator "
          + "and org.apache.hudi.utilities.streamer.validator.SparkKafkaOffsetValidator). "
          + "The validator compares the offset difference (expected records from source) "
          + "with actual records written. If the deviation exceeds this percentage, "
          + "the commit is rejected or warned depending on the validation failure policy. "
          + "For upsert workloads with deduplication, set a higher tolerance. "
          + "Default is 0.0 (strict mode, exact match required).");

  /**
   * Policy for handling pre-commit validation failures.
   */
  public enum ValidationFailurePolicy {
    /** Validation failures block the commit with an exception. */
    FAIL,
    /** Validation failures emit a warning log but allow the commit to proceed. */
    WARN_LOG
  }

  public static final ConfigProperty<String> VALIDATION_FAILURE_POLICY = ConfigProperty
      .key("hoodie.precommit.validators.failure.policy")
      .defaultValue(ValidationFailurePolicy.FAIL.name())
      .sinceVersion("1.2.0")
      .markAdvanced()
      .withDocumentation("Policy for handling pre-commit validation failures. "
          + "FAIL (default): validation failures block the commit with an exception. "
          + "WARN_LOG: validation failures emit a warning log but allow the commit to proceed. "
          + "Useful for monitoring data quality without impacting write availability.");

  /**
   * Spark SQL queries to run on table before committing new data to validate state before and after commit.
   * Multiple queries separated by ';' delimiter are supported.
   * Example query: 'select count(*) from \<TABLE_NAME\> where col=null'
   * Note \<TABLE_NAME\> variable is expected to be present in query.
   */
  public static final ConfigProperty<String> INEQUALITY_SQL_QUERIES = ConfigProperty
      .key("hoodie.precommit.validators.inequality.sql.queries")
      .defaultValue("")
      .markAdvanced()
      .withDocumentation("Spark SQL queries to run on table before committing new data to validate state before and after commit."
          + "Multiple queries separated by ';' delimiter are supported."
          + "Example query: 'select count(*) from \\<TABLE_NAME\\> where col=null'"
          + "Note \\<TABLE_NAME\\> variable is expected to be present in query.");

  private HoodiePreCommitValidatorConfig() {
    super();
  }

  public static HoodiePreCommitValidatorConfig.Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private final HoodiePreCommitValidatorConfig preCommitValidatorConfig = new HoodiePreCommitValidatorConfig();

    public Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        this.preCommitValidatorConfig.getProps().load(reader);
        return this;
      }
    }

    public Builder fromProperties(Properties props) {
      this.preCommitValidatorConfig.getProps().putAll(props);
      return this;
    }

    public Builder withPreCommitValidator(String preCommitValidators) {
      preCommitValidatorConfig.setValue(VALIDATOR_CLASS_NAMES, preCommitValidators);
      return this;
    }

    public Builder withPrecommitValidatorEqualitySqlQueries(String preCommitValidators) {
      preCommitValidatorConfig.setValue(EQUALITY_SQL_QUERIES, preCommitValidators);
      return this;
    }

    public Builder withPrecommitValidatorSingleResultSqlQueries(String preCommitValidators) {
      preCommitValidatorConfig.setValue(SINGLE_VALUE_SQL_QUERIES, preCommitValidators);
      return this;
    }

    public Builder withPrecommitValidatorInequalitySqlQueries(String preCommitValidators) {
      preCommitValidatorConfig.setValue(INEQUALITY_SQL_QUERIES, preCommitValidators);
      return this;
    }

    public HoodiePreCommitValidatorConfig build() {
      preCommitValidatorConfig.setDefaults(HoodiePreCommitValidatorConfig.class.getName());
      return preCommitValidatorConfig;
    }
  }

}
