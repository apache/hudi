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

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.validator.BasePreCommitValidator;
import org.apache.hudi.client.validator.ValidationContext;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodiePreCommitValidatorConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieValidationException;

import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utility for running pre-commit validators in the HoodieStreamer commit flow.
 *
 * <p>Instantiates and executes validators configured via
 * {@code hoodie.precommit.validators}. Each validator must extend
 * {@link BasePreCommitValidator} and have a constructor that accepts
 * {@link TypedProperties}.</p>
 *
 * <p>Called from {@code StreamSync.writeToSinkAndDoMetaSync()} before
 * the commit is finalized.</p>
 *
 * <p><b>Note on validator compatibility:</b> This utility uses a different instantiation
 * mechanism than {@code SparkValidatorUtils} (used by the Spark table write path).
 * {@code SparkValidatorUtils} expects validators implementing {@code SparkPreCommitValidator}
 * with a {@code (HoodieSparkTable, HoodieEngineContext, HoodieWriteConfig)} constructor.
 * Validators registered here (e.g. {@link SparkKafkaOffsetValidator}) extend
 * {@link BasePreCommitValidator} with a {@code (TypedProperties)} constructor and
 * are NOT compatible with {@code SparkValidatorUtils}. Do not mix them under the same
 * {@code hoodie.precommit.validators} config if both paths are active.</p>
 */
public class SparkStreamerValidatorUtils {

  private static final Logger LOG = LoggerFactory.getLogger(SparkStreamerValidatorUtils.class);

  /**
   * Run all configured pre-commit validators.
   *
   * @param props Configuration properties containing validator class names
   * @param instant Commit instant time
   * @param writeStatusRDD Write statuses from Spark write operations
   * @param checkpointCommitMetadata Extra metadata being committed (contains checkpoint info)
   * @param metaClient Table meta client for timeline access and previous commit lookup
   * @throws HoodieValidationException if any validator fails with FAIL policy
   */
  public static void runValidators(TypedProperties props,
                                   String instant,
                                   JavaRDD<WriteStatus> writeStatusRDD,
                                   Map<String, String> checkpointCommitMetadata,
                                   HoodieTableMetaClient metaClient) {
    String validatorClassNames = props.getString(
        HoodiePreCommitValidatorConfig.VALIDATOR_CLASS_NAMES.key(),
        HoodiePreCommitValidatorConfig.VALIDATOR_CLASS_NAMES.defaultValue());

    if (StringUtils.isNullOrEmpty(validatorClassNames)) {
      return;
    }

    // Cache the RDD to avoid recomputation when collecting write stats (prevents a second DAG evaluation).
    // Always unpersist in finally to prevent executor memory leaks.
    writeStatusRDD.cache();
    try {
      List<WriteStatus> allWriteStatus = writeStatusRDD.collect();
      HoodieCommitMetadata currentMetadata = buildCommitMetadata(allWriteStatus, checkpointCommitMetadata);
      List<HoodieWriteStat> writeStats = allWriteStatus.stream()
          .map(WriteStatus::getStat)
          .collect(Collectors.toList());

      // Load previous commit metadata from timeline
      Option<HoodieCommitMetadata> previousCommitMetadata = loadPreviousCommitMetadata(metaClient);

      ValidationContext context = new SparkValidationContext(
          instant,
          Option.of(currentMetadata),
          Option.of(writeStats),
          previousCommitMetadata,
          metaClient);

      // Instantiate and run each validator
      List<String> classNames = Arrays.stream(validatorClassNames.split(","))
          .map(String::trim)
          .filter(s -> !s.isEmpty())
          .collect(Collectors.toList());

      for (String className : classNames) {
        try {
          BasePreCommitValidator validator = (BasePreCommitValidator)
              ReflectionUtils.loadClass(className, new Class<?>[] {TypedProperties.class}, props);
          LOG.info("Running pre-commit validator: {} for instant: {}", className, instant);
          validator.validateWithMetadata(context);
          LOG.info("Pre-commit validator {} passed for instant: {}", className, instant);
        } catch (HoodieValidationException e) {
          LOG.error("Pre-commit validator {} failed for instant: {}", className, instant, e);
          throw e;
        } catch (Exception e) {
          LOG.error("Failed to instantiate or run validator: {}", className, e);
          throw new HoodieValidationException(
              "Failed to run pre-commit validator: " + className, e);
        }
      }
    } finally {
      writeStatusRDD.unpersist();
    }
  }

  /**
   * Build a pre-commit snapshot of {@link HoodieCommitMetadata} from write statuses and extra metadata.
   *
   * <p>This is intentionally a partial/preview object used only for validation — it contains
   * write stats and checkpoint extra-metadata, but omits fields that are not available before the
   * commit (e.g. schema, operation type). Validators should treat this as a read-only snapshot
   * of what will be committed, not a fully-constructed commit record.</p>
   */
  private static HoodieCommitMetadata buildCommitMetadata(
      List<WriteStatus> writeStatuses, Map<String, String> extraMetadata) {
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();

    // Add write stats
    for (WriteStatus status : writeStatuses) {
      HoodieWriteStat stat = status.getStat();
      if (stat != null) {
        metadata.addWriteStat(stat.getPartitionPath(), stat);
      }
    }

    // Add extra metadata (includes checkpoint info like deltastreamer.checkpoint.key)
    if (extraMetadata != null) {
      extraMetadata.forEach(metadata::addMetadata);
    }

    return metadata;
  }

  /**
   * Load the previous completed commit metadata from the timeline.
   */
  private static Option<HoodieCommitMetadata> loadPreviousCommitMetadata(HoodieTableMetaClient metaClient) {
    try {
      HoodieTimeline completedTimeline = metaClient.reloadActiveTimeline()
          .getWriteTimeline()
          .filterCompletedInstants();
      Option<HoodieInstant> lastInstant = completedTimeline.lastInstant();
      if (lastInstant.isPresent()) {
        return Option.of(completedTimeline.readCommitMetadata(lastInstant.get()));
      }
    } catch (IOException e) {
      throw new HoodieIOException("Failed to load previous commit metadata", e);
    } catch (Exception e) {
      LOG.warn("Failed to load previous commit metadata, skipping previous commit comparison", e);
    }
    return Option.empty();
  }

  private SparkStreamerValidatorUtils() {
    // Utility class
  }
}
