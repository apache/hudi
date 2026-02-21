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

package org.apache.hudi.utilities.sources;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.checkpoint.Checkpoint;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.config.KinesisSourceConfig;
import org.apache.hudi.utilities.ingestion.HoodieIngestionMetrics;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.KinesisOffsetGen;
import org.apache.hudi.utilities.streamer.StreamContext;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

import static org.apache.hudi.common.util.ConfigUtils.getBooleanWithAltKeys;

@Slf4j
public abstract class KinesisSource<T> extends Source<T> {

  protected static final String METRIC_NAME_KINESIS_MESSAGE_IN_COUNT = "kinesisMessageInCount";

  protected final HoodieIngestionMetrics metrics;
  protected final SchemaProvider schemaProvider;
  protected KinesisOffsetGen offsetGen;
  protected final boolean shouldAddOffsets;
  /** Checkpoint data (shardId -> sequenceNumber) collected during toBatch execution. Set by subclasses. */
  protected Map<String, String> lastCheckpointData;

  protected KinesisSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
                          SourceType sourceType, HoodieIngestionMetrics metrics, StreamContext streamContext) {
    super(props, sparkContext, sparkSession, sourceType, streamContext);
    this.schemaProvider = streamContext.getSchemaProvider();
    this.metrics = metrics;
    this.shouldAddOffsets = getBooleanWithAltKeys(props, KinesisSourceConfig.KINESIS_APPEND_OFFSETS);
  }

  @Override
  protected final InputBatch<T> fetchNewData(Option<String> lastCkptStr, long sourceLimit) {
    throw new UnsupportedOperationException("KinesisSource#fetchNewData should not be called");
  }

  @Override
  protected InputBatch<T> readFromCheckpoint(Option<Checkpoint> lastCheckpoint, long sourceLimit) {
    KinesisOffsetGen.KinesisShardRange[] shardRanges = offsetGen.getNextShardRanges(
        lastCheckpoint, sourceLimit, metrics);

    if (shardRanges.length == 0) {
      metrics.updateStreamerSourceNewMessageCount(METRIC_NAME_KINESIS_MESSAGE_IN_COUNT, 0);
      String checkpointStr = lastCheckpoint.isPresent() ? lastCheckpoint.get().getCheckpointKey() : "";
      return new InputBatch<>(Option.empty(), checkpointStr);
    }

    T batch = toBatch(shardRanges);
    String checkpointStr = createCheckpointFromBatch(batch, shardRanges);
    long totalMsgs = getRecordCount(batch);
    metrics.updateStreamerSourceNewMessageCount(METRIC_NAME_KINESIS_MESSAGE_IN_COUNT, totalMsgs);

    log.info("Read {} records from Kinesis stream {} with {} shards, checkpoint: {}",
        totalMsgs, offsetGen.getStreamName(), shardRanges.length, checkpointStr);

    return new InputBatch<>(Option.of(batch), checkpointStr);
  }

  protected abstract T toBatch(KinesisOffsetGen.KinesisShardRange[] shardRanges);

  /**
   * Create checkpoint string from the batch and shard ranges.
   * Subclasses provide checkpoint data (shardId -> sequenceNumber) collected during the read.
   */
  protected abstract String createCheckpointFromBatch(T batch, KinesisOffsetGen.KinesisShardRange[] shardRanges);

  protected abstract long getRecordCount(T batch);
}
