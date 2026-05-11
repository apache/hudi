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
import org.apache.hudi.utilities.exception.HoodieReadFromSourceException;
import org.apache.hudi.utilities.ingestion.HoodieIngestionMetrics;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.KinesisDeaggregator;
import org.apache.hudi.utilities.sources.helpers.KinesisOffsetGen;
import org.apache.hudi.utilities.streamer.StreamContext;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.InvalidArgumentException;
import software.amazon.awssdk.services.kinesis.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.hudi.common.util.ConfigUtils.getBooleanWithAltKeys;

@Slf4j
public abstract class KinesisSource<T> extends Source<T> {

  protected static final String METRIC_NAME_KINESIS_MESSAGE_IN_COUNT = "kinesisMessageInCount";

  protected final HoodieIngestionMetrics metrics;
  protected final SchemaProvider schemaProvider;
  protected KinesisOffsetGen offsetGen;
  protected final boolean shouldAddMetaFields;
  /** Checkpoint data (shardId -> sequenceNumber) collected during toBatch execution. Set by subclasses. */
  protected Map<String, String> lastCheckpointData;

  protected KinesisSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
                          SourceType sourceType, HoodieIngestionMetrics metrics, StreamContext streamContext) {
    super(props, sparkContext, sparkSession, sourceType, streamContext);
    this.schemaProvider = streamContext.getSchemaProvider();
    this.metrics = metrics;
    this.shouldAddMetaFields = getBooleanWithAltKeys(props, KinesisSourceConfig.KINESIS_APPEND_OFFSETS);
  }

  @Override
  protected final InputBatch<T> fetchNewData(Option<String> lastCkptStr, long sourceLimit) {
    throw new UnsupportedOperationException("KinesisSource#fetchNewData should not be called");
  }

  @Override
  protected InputBatch<T> readFromCheckpoint(Option<Checkpoint> lastCheckpoint, long sourceLimit) {
    // STEP 1: Collect all available shards for the stream: open/closed shards.
    KinesisOffsetGen.KinesisShardRange[] allOpenClosedShardRanges = offsetGen.getNextShardRanges(lastCheckpoint, sourceLimit);
    // STEP 2: Filter out shards with no unread records to avoid unnecessary GetRecords calls.
    boolean useLatestStartingPositionStrategy =
        offsetGen.getStartingPositionStrategy() == KinesisSourceConfig.KinesisStartingPositionStrategy.LATEST;
    int numShardsBeforeFilter = allOpenClosedShardRanges.length;
    KinesisOffsetGen.KinesisShardRange[] shardRangesWithUnreadRecords = Arrays.stream(allOpenClosedShardRanges)
        .filter(range -> range.hasUnreadRecords(useLatestStartingPositionStrategy))
        .toArray(KinesisOffsetGen.KinesisShardRange[]::new);
    if (numShardsBeforeFilter > shardRangesWithUnreadRecords.length) {
      log.info("Filtered {} shards with no unread records, {} shards remain",
          numShardsBeforeFilter - shardRangesWithUnreadRecords.length, shardRangesWithUnreadRecords.length);
    }
    // When nothing to read, return empty batch and previous checkpoint if any.
    if (shardRangesWithUnreadRecords.length == 0) {
      metrics.updateStreamerSourceNewMessageCount(METRIC_NAME_KINESIS_MESSAGE_IN_COUNT, 0);
      String checkpointStr = lastCheckpoint.isPresent() ? lastCheckpoint.get().getCheckpointKey() : "";
      return new InputBatch<>(Option.empty(), checkpointStr);
    }
    // STEP 3: Otherwise, do the read.
    T batch = toBatch(shardRangesWithUnreadRecords, sourceLimit);
    // STEP 4: Generate checkpoint.
    // Pass allOpenClosedShardRanges so filtered-out shards are preserved in the checkpoint; otherwise
    // next run would re-read them from TRIM_HORIZON and cause duplicates
    String checkpointStr = createCheckpointFromBatch(batch, shardRangesWithUnreadRecords, allOpenClosedShardRanges);
    // STEP 5: Emit metrics.
    long totalMsgs = getRecordCount(batch);
    metrics.updateStreamerSourceNewMessageCount(METRIC_NAME_KINESIS_MESSAGE_IN_COUNT, totalMsgs);
    log.info("Read {} records from Kinesis stream {} with {} shards, checkpoint: {}",
        totalMsgs, offsetGen.getStreamName(), shardRangesWithUnreadRecords.length, checkpointStr);

    return new InputBatch<>(Option.of(batch), checkpointStr);
  }

  /** Upper bound on consecutive empty GetRecords responses before giving up on a shard. */
  private static final int MAX_EMPTY_RESPONSES_FROM_GET_RECORDS = 100;

  /**
   * Lazy iterator over records from a single Kinesis shard.
   *
   * <p>Records are fetched one GetRecords page at a time; the next page is only requested once all
   * records from the current page have been consumed. This avoids holding the full shard batch in
   * executor memory simultaneously with the caller's output collection.
   *
   * <p>After {@link #hasNext()} returns {@code false} callers must read
   * {@link #getLastSequenceNumber()} and {@link #isReachedEndOfShard()} to obtain checkpoint state.
   *
   * <p><b>lastSequenceNumber correctness invariant:</b> the sequence number is taken from the last
   * <em>raw</em> Kinesis record (pre-deaggregation) of a page and is only committed once all
   * deaggregated records from that page have been yielded. This guarantees the checkpoint never
   * advances past records that have not yet been returned to the caller.
   */
  public static class ShardRecordIterator implements Iterator<Record> {
    private final KinesisClient client;
    private final String shardId;
    private final int maxRecordsPerRequest;
    private final long requestIntervalMs;
    private final long maxTotalRecords;
    private final boolean enableDeaggregation;
    private final long retryInitialIntervalMs;
    private final long retryMaxIntervalMs;
    private final long throttleTimeoutMs;

    /** Current position in the Kinesis shard; null means the shard is exhausted. */
    private String shardIteratorStr;
    /** Records from the most recently fetched page, ready to be yielded. */
    private Iterator<Record> currentPage = Collections.emptyIterator();
    /**
     * Raw lastSeq of the page currently being consumed. Moved to {@link #lastSequenceNumber} only
     * when the page iterator is fully exhausted, ensuring the checkpoint never skips records.
     */
    private String pendingPageLastSeq = null;
    /** Checkpoint-safe lastSeq: reflects only fully-consumed pages. */
    private String lastSequenceNumber = null;
    private boolean reachedEndOfShard = false;
    /** True once no further GetRecords calls should be made. */
    private boolean fetchingDone = false;
    private long totalConsumed = 0;
    private int emptyPageCount = 0;

    /**
     * Dynamically tuned records-per-request limit.
     * Halved on each ProvisionedThroughputExceededException and held there for the rest of the shard read.
     */
    private int currentMaxRecords;
    /** Epoch ms of the last successful GetRecords call; used to enforce {@link #throttleTimeoutMs}. */
    private long lastSuccessTimeMs;

    public ShardRecordIterator(String initialShardIterator, KinesisClient client, String shardId,
                               int maxRecordsPerRequest, long requestIntervalMs, long maxTotalRecords, boolean enableDeaggregation,
                               long retryInitialIntervalMs, long retryMaxIntervalMs, long throttleTimeoutMs) {
      this.shardIteratorStr = initialShardIterator;
      this.client = client;
      this.shardId = shardId;
      this.maxRecordsPerRequest = maxRecordsPerRequest;
      this.requestIntervalMs = requestIntervalMs;
      this.maxTotalRecords = maxTotalRecords;
      this.enableDeaggregation = enableDeaggregation;
      this.retryInitialIntervalMs = retryInitialIntervalMs;
      this.retryMaxIntervalMs = retryMaxIntervalMs;
      this.throttleTimeoutMs = throttleTimeoutMs;
      this.currentMaxRecords = maxRecordsPerRequest;
      this.lastSuccessTimeMs = System.currentTimeMillis();
    }

    @Override
    public boolean hasNext() {
      while (true) {
        if (currentPage.hasNext()) {
          return true;
        }
        // Current page fully consumed: commit its lastSeq before moving on.
        commitPendingPageLastSeq();
        if (fetchingDone) {
          return false;
        }
        fetchNextPage();
        // Loop: if the page was empty, try fetching again (up to MAX_EMPTY_RESPONSES limit).
      }
    }

    @Override
    public Record next() {
      if (!hasNext()) {
        throw new NoSuchElementException("No more records for shard " + shardId);
      }
      totalConsumed++;
      return currentPage.next();
    }

    public Option<String> getLastSequenceNumber() {
      return Option.ofNullable(lastSequenceNumber);
    }

    public boolean isReachedEndOfShard() {
      return reachedEndOfShard;
    }

    private void commitPendingPageLastSeq() {
      if (pendingPageLastSeq != null) {
        lastSequenceNumber = pendingPageLastSeq;
        pendingPageLastSeq = null;
      }
    }

    private void fetchNextPage() {
      if (shardIteratorStr == null || totalConsumed >= maxTotalRecords) {
        fetchingDone = true;
        return;
      }
      GetRecordsResponse response;
      int attempt = 0;
      while (true) {
        try {
          response = client.getRecords(
              GetRecordsRequest.builder()
                  .shardIterator(shardIteratorStr)
                  .limit(Math.min(currentMaxRecords, (int) (maxTotalRecords - totalConsumed)))
                  .build());
          lastSuccessTimeMs = System.currentTimeMillis();
          break;
        } catch (ExpiredIteratorException e) {
          log.warn("Shard iterator expired for {} during GetRecords, stopping read", shardId);
          fetchingDone = true;
          return;
        } catch (ProvisionedThroughputExceededException e) {
          long nowMs = System.currentTimeMillis();
          if (nowMs - lastSuccessTimeMs > throttleTimeoutMs) {
            throw new HoodieReadFromSourceException(
                "Kinesis throughput exceeded for shard " + shardId + ": no successful fetch within "
                    + throttleTimeoutMs + " ms. Last successful fetch, or first fetch was " + (nowMs - lastSuccessTimeMs) + " ms ago.", e);
          }
          // Halve the per-request limit to reduce pressure; floor at 1.
          int prevLimit = currentMaxRecords;
          currentMaxRecords = Math.max(1, currentMaxRecords / 2);
          // Use attempt count only to compute exponential backoff delay, not as a stop condition.
          long waitMs = Math.min(retryInitialIntervalMs * (1L << Math.min(attempt, 30)), retryMaxIntervalMs);
          waitMs += ThreadLocalRandom.current().nextInt(500);
          log.warn("Throughput exceeded for shard {}: halving records/request from {} to {}, retry after {} ms "
              + "(no success for {} ms, will give up after {} ms)",
              shardId, prevLimit, currentMaxRecords, waitMs, nowMs - lastSuccessTimeMs, throttleTimeoutMs);
          try {
            Thread.sleep(waitMs);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new HoodieReadFromSourceException("Interrupted while backing off for shard " + shardId, ie);
          }
          attempt++;
        }
      }

      List<Record> rawRecords = response.records();
      // Update before empty check: null nextShardIterator signals end-of-shard even on a 0-record response.
      shardIteratorStr = response.nextShardIterator();

      if (!rawRecords.isEmpty()) {
        // pendingPageLastSeq is from raw records (pre-deaggregation) per the checkpoint invariant.
        pendingPageLastSeq = rawRecords.get(rawRecords.size() - 1).sequenceNumber();
        List<Record> toYield = enableDeaggregation ? KinesisDeaggregator.deaggregate(rawRecords) : rawRecords;
        currentPage = toYield.iterator();
        emptyPageCount = 0;
      } else {
        if (emptyPageCount++ > MAX_EMPTY_RESPONSES_FROM_GET_RECORDS) {
          fetchingDone = true;
          return;
        }
      }

      // Process records first (done above), then decide whether to stop.
      // millisBehindLatest can be 0 in LocalStack even when the response contained records.
      if (response.millisBehindLatest() == 0) {
        fetchingDone = true;
      }
      if (shardIteratorStr == null) {
        reachedEndOfShard = true;
        fetchingDone = true;
      }

      // Rate-limit only when we will fetch another page.
      if (!fetchingDone && requestIntervalMs > 0) {
        try {
          Thread.sleep(requestIntervalMs);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          fetchingDone = true;
        }
      }
    }
  }

  /**
   * Opens a lazy iterator over records from a single shard.
   * The iterator fetches one GetRecords page at a time as records are consumed, keeping only one
   * page in memory at a time instead of the entire shard batch.
   *
   * @param enableDeaggregation when true, de-aggregates KPL records into individual user records
   * @param retryInitialIntervalMs initial backoff in ms for throughput-exceeded retries
   * @param retryMaxIntervalMs max backoff in ms for throughput-exceeded retries
   * @param throttleTimeoutMs max ms with no successful fetch before giving up on throttling
   */
  public static ShardRecordIterator readShardRecords(KinesisClient client, String streamName,
      KinesisOffsetGen.KinesisShardRange range, KinesisSourceConfig.KinesisStartingPositionStrategy defaultPosition,
      int maxRecordsPerRequest, long intervalMs, long maxTotalRecords,
      boolean enableDeaggregation,
      long retryInitialIntervalMs, long retryMaxIntervalMs, long throttleTimeoutMs) {
    try {
      String initialCursor = getCurrentCursor(client, streamName, range, defaultPosition);
      return new ShardRecordIterator(initialCursor, client, range.getShardId(),
          maxRecordsPerRequest, intervalMs, maxTotalRecords, enableDeaggregation,
          retryInitialIntervalMs, retryMaxIntervalMs, throttleTimeoutMs);
    } catch (InvalidArgumentException e) {
      throw new HoodieReadFromSourceException("Sequence number in checkpoint is expired or invalid for shard "
          + range.getShardId() + ". Reset the checkpoint to recover.", e);
    } catch (ResourceNotFoundException e) {
      throw new HoodieReadFromSourceException("Shard or stream not found: " + range.getShardId(), e);
    }
  }

  private static String getCurrentCursor(KinesisClient client, String streamName,
                                         KinesisOffsetGen.KinesisShardRange range,
                                         KinesisSourceConfig.KinesisStartingPositionStrategy defaultPosition) {
    GetShardIteratorRequest.Builder builder = GetShardIteratorRequest.builder()
        .streamName(streamName)
        .shardId(range.getShardId());

    if (range.getStartingSequenceNumber().isPresent()) {
      builder.shardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER);
      builder.startingSequenceNumber(range.getStartingSequenceNumber().get());
    } else {
      // EARLIEST is normalized to TRIM_HORIZON in constructor
      builder.shardIteratorType(defaultPosition == KinesisSourceConfig.KinesisStartingPositionStrategy.EARLIEST
          ? ShardIteratorType.TRIM_HORIZON : ShardIteratorType.LATEST);
    }
    return client.getShardIterator(builder.build()).shardIterator();
  }

  protected abstract T toBatch(KinesisOffsetGen.KinesisShardRange[] shardRanges, long sourceLimit);

  /**
   * Create checkpoint string from the batch and shard ranges.
   * Subclasses provide checkpoint data (shardId -> sequenceNumber) collected during the read.
   * Must include both read shards (from shardRangesWithUnreadRecords) and filtered shards (from allOpenClosedShardRanges)
   * so the next run does not re-read filtered-out shards from TRIM_HORIZON.
   */
  protected abstract String createCheckpointFromBatch(T batch,
      KinesisOffsetGen.KinesisShardRange[] shardRangesWithUnreadRecords,
      KinesisOffsetGen.KinesisShardRange[] allOpenClosedShardRanges);

  protected abstract long getRecordCount(T batch);
}
