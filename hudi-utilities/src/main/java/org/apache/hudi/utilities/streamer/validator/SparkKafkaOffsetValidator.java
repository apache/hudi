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

package org.apache.hudi.utilities.streamer.validator;

import org.apache.hudi.client.validator.StreamingOffsetValidator;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV1;
import org.apache.hudi.common.util.CheckpointUtils.CheckpointFormat;

/**
 * Spark/HoodieStreamer-specific Kafka offset validator.
 *
 * <p>Validates that the number of records written matches the Kafka offset difference
 * between the current and previous HoodieStreamer checkpoints. Uses the Spark Kafka
 * checkpoint format stored with key {@code deltastreamer.checkpoint.key} in extraMetadata.</p>
 *
 * <p>Configuration:
 * <ul>
 *   <li>{@code hoodie.precommit.validators}: Include
 *       {@code org.apache.hudi.utilities.streamer.validator.SparkKafkaOffsetValidator}</li>
 *   <li>{@code hoodie.precommit.validators.streaming.offset.tolerance.percentage}:
 *       Acceptable deviation (default: 0.0 = strict)</li>
 *   <li>{@code hoodie.precommit.validators.failure.policy}:
 *       FAIL (default) or WARN_LOG</li>
 * </ul></p>
 *
 * <p>This validator is primarily intended for append-only ingestion from Kafka via HoodieStreamer.
 * For upsert workloads with deduplication, configure a higher tolerance or use WARN_LOG.</p>
 */
public class SparkKafkaOffsetValidator extends StreamingOffsetValidator {

  /**
   * Create a Spark Kafka offset validator.
   *
   * @param config Validator configuration
   */
  public SparkKafkaOffsetValidator(TypedProperties config) {
    super(config, StreamerCheckpointV1.STREAMER_CHECKPOINT_KEY_V1, CheckpointFormat.SPARK_KAFKA);
  }
}
