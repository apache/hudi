/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.utilities.sources.helpers;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.config.DFSPathSelectorConfig;
import org.apache.hudi.utilities.config.S3SourceConfig;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;
import software.amazon.awssdk.services.sqs.model.BatchResultErrorEntry;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.ConfigUtils.checkRequiredConfigProperties;
import static org.apache.hudi.common.util.ConfigUtils.getIntWithAltKeys;
import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;

/**
 * This class has methods for processing cloud objects.
 * It currently supports only AWS S3 objects and AWS SQS queue.
 */
public class CloudObjectsSelector {
  public static final List<String> ALLOWED_S3_EVENT_PREFIX =
      Collections.singletonList("ObjectCreated");
  public static final String S3_PREFIX = "s3://";
  public static volatile Logger log = LoggerFactory.getLogger(CloudObjectsSelector.class);
  public static final String SQS_ATTR_APPROX_MESSAGES = "ApproximateNumberOfMessages";
  // Live queue-depth counters fetched every batch alongside SQS_ATTR_APPROX_MESSAGES. These three
  // are disjoint: visible (available) + not-visible (in-flight) + delayed = true queue depth.
  public static final String SQS_ATTR_APPROX_MESSAGES_NOT_VISIBLE = "ApproximateNumberOfMessagesNotVisible";
  public static final String SQS_ATTR_APPROX_MESSAGES_DELAYED = "ApproximateNumberOfMessagesDelayed";
  // Static queue-config attributes logged once per selector for diagnostics (not per batch).
  // NOTE: FIFO-only attributes (FifoQueue, ContentBasedDeduplication, DeduplicationScope,
  // FifoThroughputLimit) must never be named in a GetQueueAttributes request - SQS rejects the
  // whole call with InvalidAttributeNameException on a standard queue. FIFO-ness is derived from
  // the queue-name suffix instead, per the AWS API docs.
  static final String FIFO_QUEUE_URL_SUFFIX = ".fifo";
  static final String SQS_ATTR_MESSAGE_RETENTION_PERIOD = "MessageRetentionPeriod";
  static final String SQS_ATTR_VISIBILITY_TIMEOUT = "VisibilityTimeout";
  static final String SQS_ATTR_RECEIVE_MESSAGE_WAIT_TIME = "ReceiveMessageWaitTimeSeconds";
  static final String SQS_ATTR_DELAY_SECONDS = "DelaySeconds";
  static final String SQS_ATTR_REDRIVE_POLICY = "RedrivePolicy";
  // Emit a receive-progress DEBUG line every time cumulative fetched messages cross this many, to
  // keep the loop's logging bounded (~a handful of lines/batch) instead of one line per receive call.
  private static final int MESSAGE_PROGRESS_LOG_INTERVAL = 25000;
  // SQS caps ReceiveMessage/DeleteMessageBatch at 10 entries per call (AWS hard limit), so draining a
  // backlog is dominated by round-trip latency; the receive/delete phases fan calls out across a fixed
  // pool. Thread names are prefixed so the workers are identifiable in stack dumps.
  private static final String RECEIVE_THREAD_PREFIX = "sqs-receive-";
  private static final String DELETE_THREAD_PREFIX = "sqs-delete-";
  // The shared SqsClient's HTTP connection pool must be at least the worker count or workers block on
  // connection acquisition (connectionAcquisitionTimeout=10s). This is the AWS SDK v2 default sync
  // maxConnections: at or below it the default client already has enough connections for every worker
  // plus headroom for the serial calls, so createAmazonSqsClient leaves the client untouched and only
  // resizes the pool above this value. See createAmazonSqsClient for why that matters.
  private static final int SQS_CLIENT_MIN_MAX_CONNECTIONS = 50;
  // Bound how long we wait for a receive/delete pool to drain on shutdown. On the normal paths every task
  // has already been joined by the time we shut down, so this only guards against a stuck AWS call on the
  // paths that abandon the phase without draining (a worker Error, or an interrupt).
  private static final int POOL_SHUTDOWN_TIMEOUT_SECS = 60;
  // How many empty ReceiveMessage responses confirm the queue is drained when short polling is in
  // effect (longPollWait <= 0). A short poll "queries a subset of servers (based on a weighted random
  // distribution) ... and sends an immediate response, even if no messages are found" (AWS), so a single
  // empty response proves almost nothing - but it is also nearly free, which is why the count can afford
  // to be high. See emptyReceivesToConfirmDrain for the long-polling case.
  static final int MAX_EMPTY_RECEIVES_SHORT_POLL = 5;
  // Budget, in seconds, for proving the queue is drained. Under long polling an empty response is only
  // sent once the poll window expires, so an empty response costs a full longPollWait - which makes the
  // number of empties we can afford a function of longPollWait rather than a fixed count. Sizing the
  // budget at the AWS maximum long-poll wait (20s) keeps drain confirmation to roughly one poll window
  // at every setting: 1 empty at longPollWait=20, 2 at 10, 4 at 5, 20 at 1. That is a wall-clock bound
  // achieved structurally, with no timer and nothing to abort mid-flight.
  static final int DRAIN_CONFIRMATION_WINDOW_SECS = 20;
  // A transient ReceiveMessage failure (throttling, a 5xx, a dropped connection) is tolerated rather
  // than aborting the batch: everything already received is in-flight and would be stranded until the
  // visibility timeout expires. Only a run of them indicates a systemic problem worth stopping for.
  // Counted consecutively in completion order on the master thread and reset by any successful call -
  // including one that returns no messages, which still proves the endpoint and credentials are healthy.
  // Kept small because the AWS SDK has already exhausted its own standard retry strategy (3 attempts
  // with exponential backoff) before the exception ever surfaces here, so this is a second layer.
  // NOTE: this counts consecutive *completions*, not serial calls, so the tolerance it delivers is
  // tighter at high parallelism than at 1 - a throttling episode tends to fail several in-flight calls
  // at once, and 5 such completions in a row is far likelier than 5 serial calls failing in a row.
  // Tightest on a queue that is not yielding: there the only calls that could reset the run are 20s
  // empty polls, and a burst of ~100ms failures lands well ahead of them. The direction is intended - a
  // fan-out-wide failure is stronger evidence of a systemic problem than one isolated failure - and it
  // is safe because in-flight calls are drained rather than cancelled, so their healthy responses are
  // still folded in before the empty-result check in getMessagesToProcess decides whether to throw.
  static final int MAX_CONSECUTIVE_TRANSIENT_FAILURES = 5;
  // ReceiveMessage returns OverLimit "if the maximum number of in flight messages is reached" (~120,000
  // for a standard queue, 20,000 for FIFO). That is backpressure, not an error: received messages stay
  // in-flight until onCommit deletes them, so the fix is to stop receiving, hand back what was fetched
  // and let the delete phase free the quota. Never retried (it would fail identically) and never fatal
  // (failing the job over normal saturation would take ingestion down).
  static final String SQS_OVER_LIMIT_ERROR_CODE = "OverLimit";
  // Only probe queue counters on a drain that fell materially short of what the queue claimed was
  // available (less than this fraction of it). A drain that lands close to the target is ordinary
  // eventual consistency in ApproximateNumberOfMessages and does not justify a WARN plus an extra
  // GetQueueAttributes call on every batch.
  private static final int DRAIN_SHORTFALL_WARN_DIVISOR = 2;
  // The receive loop is target-driven (keep fetching until numMessagesToProcess is reached) rather than
  // fixed-iteration, because SQS routinely returns fewer than maxMessagesPerRequest messages per call
  // even with a deep backlog. Target-driven alone is unbounded though: a queue with slow ingress keeps
  // resetting the empty counter and can hold the loop for hours. Cap the total calls at this multiple of
  // the ideal count (plannedReceiveCalls), which tolerates an average per-call yield as low as
  // 1/RECEIVE_CALL_BUDGET_FACTOR of the maximum before the batch gives up and lets the next ingestion
  // cycle continue from where it left off. Counted in calls rather than seconds on purpose: the budget
  // then encodes a claim about queue yield alone and is latency-independent by construction, so slow
  // round-trips make a batch take longer without making it smaller.
  static final int RECEIVE_CALL_BUDGET_FACTOR = 2;
  // SQS caps DeleteMessageBatch at 10 entries per call (AWS hard limit); a larger batch is rejected
  // outright with TooManyEntriesInBatchRequest.
  static final int SQS_BATCH_MAX_ENTRIES = 10;
  // DeleteMessageBatch can return partial failures even on an HTTP 200, and a call can also throw
  // outright (throttling, a dropped connection); both leave the batch undeleted and are retried up to
  // this many times before the residual failures are logged with their SQS reason. The original code did
  // not retry at all. Also a constant rather than a config for the same reason.
  static final int DELETE_MAX_RETRIES = 3;
  // Retries only ever target transient, server-side failures (SQS InternalError, throttling, a dropped
  // connection); retrying microseconds later would almost certainly hit the same condition, so each pass
  // backs off exponentially from this base: 100ms, 200ms, 400ms. Bounded by DELETE_MAX_RETRIES, so the
  // delete phase adds at most 700ms of sleep on the driver even in the worst case.
  static final long DELETE_RETRY_BASE_BACKOFF_MS = 100;
  // How many failed message ids to name in the residual-failure WARN. Enough for on-call to grep the
  // queue or a DLQ for a concrete message without dumping an unbounded list into the log.
  private static final int DELETE_FAILURE_SAMPLE_SIZE = 10;
  // Synthetic reason code for a DeleteMessageBatch call that threw, so a thrown call and an
  // SQS-reported entry failure can flow through one retry path and one summary. Not an SQS error code.
  static final String DELETE_CALL_FAILED_CODE = "DeleteMessageBatchCallFailed";
  // Transient SQS/AWS error codes that the SDK's own throttling predicate does not recognise, so they
  // would otherwise fall through to the status-code rule and be misjudged. KmsThrottled in particular is
  // an HTTP 400 on an SSE-KMS encrypted queue and is purely transient, but is absent from
  // AwsErrorCode.THROTTLING_ERROR_CODES. The rest mirror AwsErrorCode.RETRYABLE_ERROR_CODES.
  private static final Set<String> EXTRA_TRANSIENT_ERROR_CODES = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
      "KmsThrottled", "InternalError", "RequestTimeout", "RequestTimeoutException", "PriorRequestNotComplete")));
  // Placeholders so the residual-failure WARN never renders a bare "null" for fields SQS may omit.
  private static final String UNKNOWN_ERROR_CODE = "UNKNOWN";
  private static final String NO_ERROR_MESSAGE = "<none provided by SQS>";
  static final String SQS_MODEL_MESSAGE = "Message";
  static final String SQS_MODEL_EVENT_RECORDS = "Records";
  static final String SQS_MODEL_EVENT_NAME = "eventName";
  static final String S3_MODEL_EVENT_TIME = "eventTime";
  static final String S3_FILE_SIZE = "fileSize";
  static final String S3_FILE_PATH = "filePath";
  public final String queueUrl;
  public final int longPollWait;
  public final int maxMessagePerBatch;
  public final int maxMessagesPerRequest;
  public final int visibilityTimeout;
  public final int processingParallelism;
  public final TypedProperties props;
  public final String fsName;
  private final String regionName;
  // Guards the one-time queue-config INFO line so static config is not re-logged every batch.
  private volatile boolean queueConfigLogged = false;

  /**
   * Cloud Objects Selector Class. {@link CloudObjectsSelector}
   */
  public CloudObjectsSelector(TypedProperties props) {
    checkRequiredConfigProperties(props, Arrays.asList(
        S3SourceConfig.S3_SOURCE_QUEUE_URL, S3SourceConfig.S3_SOURCE_QUEUE_REGION));
    this.props = props;
    this.queueUrl = getStringWithAltKeys(props, S3SourceConfig.S3_SOURCE_QUEUE_URL);
    this.regionName = getStringWithAltKeys(props, S3SourceConfig.S3_SOURCE_QUEUE_REGION);
    this.fsName = getStringWithAltKeys(props, S3SourceConfig.S3_SOURCE_QUEUE_FS, true);
    this.longPollWait = getIntWithAltKeys(props, S3SourceConfig.S3_QUEUE_LONG_POLL_WAIT);
    this.maxMessagePerBatch = getIntWithAltKeys(props, S3SourceConfig.S3_SOURCE_QUEUE_MAX_MESSAGES_PER_BATCH);
    this.maxMessagesPerRequest = getIntWithAltKeys(props, S3SourceConfig.S3_SOURCE_QUEUE_MAX_MESSAGES_PER_REQUEST);
    this.visibilityTimeout = getIntWithAltKeys(props, S3SourceConfig.S3_SOURCE_QUEUE_VISIBILITY_TIMEOUT);
    this.processingParallelism =
        Math.max(1, getIntWithAltKeys(props, S3SourceConfig.S3_SOURCE_QUEUE_PROCESSING_PARALLELISM));
  }

  /**
   * Get SQS queue attributes.
   *
   * @param sqsClient AWSClient for sqsClient
   * @param queueUrl  queue full url
   * @return map of attributes needed
   */
  protected Map<String, String> getSqsQueueAttributes(SqsClient sqsClient, String queueUrl) {
    // Request ALL rather than an explicit attribute list. FIFO-only attribute names (FifoQueue,
    // ContentBasedDeduplication, ...) are rejected outright on a standard queue with
    // InvalidAttributeNameException, which would fail the entire GetQueueAttributes call - and this
    // call is load-bearing for the receive loop, so that takes ingestion down. ALL returns whatever
    // applies to this queue type; callers use attrOrUnknown() for anything that may be absent.
    GetQueueAttributesResponse queueAttributesResult = sqsClient.getQueueAttributes(
            GetQueueAttributesRequest.builder()
                    .queueUrl(queueUrl)
                    .attributeNames(QueueAttributeName.ALL)
                    .build()
    );
    return queueAttributesResult.attributesAsStrings();
  }

  /**
   * Returns the attribute value for {@code key}, or "unknown" when SQS did not return it (for
   * example a standard queue omits {@code FifoQueue}, or a mock in tests only stubs a subset).
   */
  private static String attrOrUnknown(Map<String, String> attributes, String key) {
    String value = attributes.get(key);
    return value != null ? value : "unknown";
  }

  /**
   * Logs the queue's static configuration exactly once per selector instance. Whether the queue is
   * FIFO determines which in-flight ceiling applies (standard vs FIFO), and
   * {@code MessageRetentionPeriod} bounds how long a backlog can sit before messages expire.
   *
   * <p>FIFO-ness is derived from the {@code .fifo} queue-name suffix rather than the
   * {@code FifoQueue} attribute: that attribute exists only on FIFO queues, so naming it in a
   * GetQueueAttributes request is rejected outright on a standard queue. Attributes that may be
   * absent (e.g. {@code RedrivePolicy} without a dead-letter queue) go through
   * {@link #attrOrUnknown}.
   *
   * <p>FIFO-ness is diagnostic only and deliberately does not gate the receive fan-out, which would
   * otherwise be the obvious thing to check: concurrent receives on a FIFO queue would break the
   * per-message-group ordering such a queue exists to provide. It is not reachable here. This selector
   * is driven by S3 event notifications, and S3 cannot publish to a FIFO queue - neither directly nor
   * through SNS, since a standard SNS topic cannot deliver to a FIFO queue. If this class is ever
   * repointed at an arbitrary queue, that assumption is what has to be re-checked.
   *
   * @param queueUrl        the queue url, for correlation
   * @param queueAttributes attributes already fetched for this batch (no extra SQS call is made)
   */
  private void logQueueConfigOnce(String queueUrl, Map<String, String> queueAttributes) {
    if (queueConfigLogged) {
      return;
    }
    queueConfigLogged = true;
    // Diagnostics must never be able to fail a batch: this runs inline on the receive path, so any
    // future change here (parsing an attribute, deriving a new field) stays contained.
    try {
      boolean fifoQueue = queueUrl != null && queueUrl.endsWith(FIFO_QUEUE_URL_SUFFIX);
      log.info("SQS queue config [{}]: fifoQueue={}, messageRetentionPeriod={}s, visibilityTimeout(queue default)={}s, "
              + "receiveMessageWaitTime(queue default)={}s, delaySeconds={}, redrivePolicy={}",
          queueUrl,
          fifoQueue,
          attrOrUnknown(queueAttributes, SQS_ATTR_MESSAGE_RETENTION_PERIOD),
          attrOrUnknown(queueAttributes, SQS_ATTR_VISIBILITY_TIMEOUT),
          attrOrUnknown(queueAttributes, SQS_ATTR_RECEIVE_MESSAGE_WAIT_TIME),
          attrOrUnknown(queueAttributes, SQS_ATTR_DELAY_SECONDS),
          attrOrUnknown(queueAttributes, SQS_ATTR_REDRIVE_POLICY));
    } catch (Exception e) {
      log.warn("Failed to log SQS queue config for {}; continuing.", queueUrl, e);
    }
  }

  /**
   * Get the file attributes filePath, eventTime and size from JSONObject record.
   *
   * @param record of object event
   * @return map of file attribute
   */
  protected Map<String, Object> getFileAttributesFromRecord(JSONObject record) throws UnsupportedEncodingException {
    Map<String, Object> fileRecord = new HashMap<>();
    String eventTimeStr = record.getString(S3_MODEL_EVENT_TIME);
    long eventTime =
        Date.from(Instant.from(DateTimeFormatter.ISO_INSTANT.parse(eventTimeStr))).getTime();
    JSONObject s3Object = record.getJSONObject("s3").getJSONObject("object");
    String bucket = URLDecoder.decode(record.getJSONObject("s3").getJSONObject("bucket").getString("name"), "UTF-8");
    String key = URLDecoder.decode(s3Object.getString("key"), "UTF-8");
    String filePath = this.fsName + "://" + bucket + "/" + key;
    fileRecord.put(S3_MODEL_EVENT_TIME, eventTime);
    fileRecord.put(S3_FILE_SIZE, s3Object.getLong("size"));
    fileRecord.put(S3_FILE_PATH, filePath);
    return fileRecord;
  }

  /**
   * Amazon SQS Client Builder. One thread-safe client is shared across the receive/delete worker
   * pools (AWS SDK for Java 2.x clients are thread-safe and meant to be shared).
   *
   * <p>Concurrent ReceiveMessage/DeleteMessageBatch calls must never block on HTTP connection
   * acquisition, so the connection pool has to be at least {@link #processingParallelism}. The SDK's
   * default sync pool is already {@value #SQS_CLIENT_MIN_MAX_CONNECTIONS} connections, which covers
   * every parallelism up to that value with headroom for the serial calls (getQueueAttributes) - so
   * the pool is only resized when the configured parallelism actually exceeds the default, and the
   * client is otherwise built exactly as it was before the fan-out was introduced.
   *
   * <p>Resizing means naming a concrete HTTP client implementation, which turns
   * {@code software.amazon.awssdk:apache-client} from "whatever sync implementation the runtime
   * happens to provide" into a hard requirement of this source. Keeping the default path untouched
   * confines that requirement to deployments that explicitly opt into a parallelism above
   * {@value #SQS_CLIENT_MIN_MAX_CONNECTIONS}, rather than imposing it on every S3 source.
   */
  public SqsClient createAmazonSqsClient() {
    SqsClientBuilder builder = SqsClient.builder().region(Region.of(regionName));
    if (requiresLargerConnectionPool(processingParallelism)) {
      builder.httpClientBuilder(ApacheHttpClient.builder().maxConnections(processingParallelism));
    }
    return builder.build();
  }

  /**
   * Whether the configured parallelism outgrows the SDK's default sync connection pool and therefore
   * needs {@link #createAmazonSqsClient} to name a concrete HTTP client to resize it.
   */
  static boolean requiresLargerConnectionPool(int parallelism) {
    return parallelism > SQS_CLIENT_MIN_MAX_CONNECTIONS;
  }

  /**
   * Receives up to {@code min(approxAvailable, maxMessagePerBatch)} messages from the queue. Calls are
   * dispatched continuously to a fixed pool of {@link #processingParallelism} workers: the master thread
   * consumes each completion the moment it arrives and immediately refills the freed slot, so a worker
   * never waits on a sibling. Each ReceiveMessage returns at most {@code maxMessagesPerRequest} messages
   * (SQS caps this at 10), so throughput is bound by round-trip latency and concurrency is the only
   * lever. The shared {@code sqsClient} is thread-safe.
   *
   * <p>All counters and every termination decision stay on this master thread, so no termination state
   * is shared with the workers; {@code busyNanos} remains the only concurrently mutated value.
   *
   * <p>The batch stops dispatching on the first of: the target message count is reached; the queue is
   * confirmed drained (see {@link #emptyReceivesToConfirmDrain}); SQS reports the in-flight ceiling
   * ({@value #SQS_OVER_LIMIT_ERROR_CODE}); a non-retryable failure occurs; {@value
   * #MAX_CONSECUTIVE_TRANSIENT_FAILURES} transient failures occur in a row; or the receive-call budget
   * ({@value #RECEIVE_CALL_BUDGET_FACTOR}x {@code plannedReceiveCalls}) is spent.
   *
   * <p>Three of those bounds overlap and are deliberately not merged - each answers a different question,
   * in the unit natural to it. {@code maxMessagePerBatch} bounds how much work to take, in messages.
   * {@link #emptyReceivesToConfirmDrain} decides whether the queue is empty, using the explicit empty
   * response SQS returns rather than inferring emptiness from the absence of a signal.
   * {@code receiveCallBudget} bounds runaway, in calls - deliberately not in seconds, so that slow
   * round-trips make a batch take longer without making it smaller. A time-based budget would conflate
   * "the queue is yielding poorly" with "the network is slow right now", which are different problems
   * with different fixes.
   *
   * <p>Whenever a <em>stop condition</em> ends dispatch, the calls already in flight are drained and their
   * messages kept - never cancelled. A ReceiveMessage that succeeded server-side has already made its
   * messages invisible, so discarding the response would strand them for the full visibility timeout. For
   * the same reason an individual failure does not abort the batch. Ending early never drops messages:
   * whatever is not received stays in the queue for the next batch.
   *
   * <p>The two paths that abandon the phase outright - a worker {@link Error}, and an interrupt - are the
   * deliberate exceptions: both discard whatever has been collected, which does leave those messages
   * in-flight until the visibility timeout expires. That is accepted because in both cases the batch has
   * no future (a broken JVM or classpath, or a shutdown with nobody left to commit), so returning a
   * partial result would only feed a checkpoint that is never written. It is a delay, never message loss.
   *
   * <p>If nothing at all was fetched and no call even proved the queue reachable, the failure is rethrown
   * - an empty result would otherwise be indistinguishable from a drained queue to the caller and to
   * on-call. A batch that did get a real answer from SQS (an empty response, or an in-flight-limit
   * rejection) returns normally even if some sibling call failed, since the queue is demonstrably healthy.
   *
   * @return the received messages in <em>completion</em> order, not call-issue order. Callers must not
   *     depend on the ordering: {@code S3EventsMetaSelector} sorts on the S3 event time and derives its
   *     checkpoint from the maximum, so the source already treats receive order as unordered input.
   */
  protected List<Message> getMessagesToProcess(
      SqsClient sqsClient,
      String queueUrl,
      int longPollWait,
      int visibilityTimeout,
      int maxMessagePerBatch,
      int maxMessagesPerRequest) {
    ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
            .queueUrl(queueUrl)
            .waitTimeSeconds(longPollWait)
            .visibilityTimeout(visibilityTimeout)
            .maxNumberOfMessages(maxMessagesPerRequest)
            .build();
    // Get count for available messages
    Map<String, String> queueAttributesResult = getSqsQueueAttributes(sqsClient, queueUrl);
    logQueueConfigOnce(queueUrl, queueAttributesResult);
    long approxMessagesAvailable = Long.parseLong(queueAttributesResult.get(SQS_ATTR_APPROX_MESSAGES));
    long numMessagesToProcess = Math.min(approxMessagesAvailable, maxMessagePerBatch);
    // Integer ceiling division rather than Math.ceil on a double: numMessagesToProcess is a long, and
    // routing a message count through a 53-bit mantissa to divide it is both lossy above 2^53 and harder
    // to read than the exact arithmetic. The divisor is floored at 1 so a misconfigured non-positive
    // maxMessagesPerRequest cannot divide by zero here - SQS rejects such a request on its own.
    long callSize = Math.max(1, maxMessagesPerRequest);
    long plannedReceiveCalls = (numMessagesToProcess + callSize - 1) / callSize;
    int emptiesToConfirmDrain = emptyReceivesToConfirmDrain(longPollWait);
    log.info("Approximately {} messages available in queue.", approxMessagesAvailable);
    log.info("SQS receive plan for queue {}: approxAvailable={}, approxInFlight={}, approxDelayed={}, "
            + "maxMessagePerBatch={}, maxMessagesPerRequest={}, numMessagesToProcess={}, plannedReceiveCalls={}, "
            + "processingParallelism={}, emptiesToConfirmDrain={}, longPollWait={}s, visibilityTimeout={}s",
        queueUrl, approxMessagesAvailable,
        attrOrUnknown(queueAttributesResult, SQS_ATTR_APPROX_MESSAGES_NOT_VISIBLE),
        attrOrUnknown(queueAttributesResult, SQS_ATTR_APPROX_MESSAGES_DELAYED),
        maxMessagePerBatch, maxMessagesPerRequest, numMessagesToProcess, plannedReceiveCalls,
        processingParallelism, emptiesToConfirmDrain, longPollWait, visibilityTimeout);

    if (numMessagesToProcess <= 0) {
      log.info("SQS receive summary for queue {}: fetched 0 messages across 0 receive calls, exitReason={}.",
          queueUrl, ReceiveExitReason.NO_MESSAGES);
      return Collections.emptyList();
    }

    // Bound the total number of receive calls: plannedReceiveCalls is the ideal count (every call
    // returning a full maxMessagesPerRequest), and the budget allows for short responses without letting
    // a trickling queue hold this loop indefinitely. Every call count is a long, since they all derive
    // from numMessagesToProcess and an int would silently truncate the moment maxMessagePerBatch widened.
    // The floor keeps the budget from cutting a tiny batch off before either termination heuristic has
    // had enough calls to reach its threshold.
    long budgetFloor = Math.max(emptiesToConfirmDrain, MAX_CONSECUTIVE_TRANSIENT_FAILURES);
    long receiveCallBudget = Math.max(plannedReceiveCalls * RECEIVE_CALL_BUDGET_FACTOR, budgetFloor);
    // The pool size is the one count that stays an int: it is bounded by processingParallelism, an int
    // config, and Executors.newFixedThreadPool takes an int.
    int workers = (int) Math.max(1, Math.min(processingParallelism, plannedReceiveCalls));
    ReceivePlan plan = new ReceivePlan(numMessagesToProcess, maxMessagesPerRequest, workers,
        receiveCallBudget, emptiesToConfirmDrain, plannedReceiveCalls);

    long wallStartMs = System.currentTimeMillis();
    // busyNanos is the only shared state: worker tasks add their SQS-call time concurrently.
    AtomicLong busyNanos = new AtomicLong();
    Callable<List<Message>> receiveTask = () -> {
      long callStartNanos = System.nanoTime();
      try {
        return sqsClient.receiveMessage(receiveMessageRequest).messages();
      } finally {
        busyNanos.addAndGet(System.nanoTime() - callStartNanos);
      }
    };

    ReceiveState state = new ReceiveState();
    long receiveWallMs;
    ExecutorService pool = newFixedThreadPool(RECEIVE_THREAD_PREFIX, plan.workers);
    try {
      runReceiveLoop(pool, receiveTask, state, plan, queueUrl);
      // Captured before pool shutdown so the perf line below measures only the fanned-out receive calls.
      receiveWallMs = System.currentTimeMillis() - wallStartMs;
    } finally {
      shutdownThreadPool(pool);
    }

    if (state.exitReason == null) {
      state.exitReason = state.callsIssued >= plan.receiveCallBudget && state.messages.size() < plan.numMessagesToProcess
          ? ReceiveExitReason.CALL_BUDGET_EXHAUSTED : ReceiveExitReason.TARGET_REACHED;
    }
    // Probe only on a drain that fell materially short of what the queue claimed: that is the case worth
    // diagnosing (typically the in-flight ceiling), whereas landing near the target is ordinary
    // ApproximateNumberOfMessages drift and must not cost a WARN plus an extra API call every batch.
    // Multiplied rather than divided so a target of 1 still qualifies: integer division would floor the
    // threshold to 0 and silence the probe exactly when the whole batch came back empty.
    if (state.exitReason == ReceiveExitReason.DRAINED
        && (long) DRAIN_SHORTFALL_WARN_DIVISOR * state.messages.size() < plan.numMessagesToProcess) {
      logReceiveBreakProbe(sqsClient, queueUrl, visibilityTimeout, state.emptyReceives,
          state.messages.size(), state.callsCompleted, plan.numMessagesToProcess);
    }
    logReceiveOutcome(queueUrl, state, plan, receiveWallMs, busyNanos.get());

    if (state.messages.isEmpty()) {
      // Nothing fetched: an empty return would look indistinguishable from a drained queue to the caller
      // and to on-call, leaving a stalled pipeline looking healthy.
      if (state.fatalFailure != null) {
        throw new HoodieException(String.format(
            "Non-retryable failure receiving messages from SQS queue %s", queueUrl), state.fatalFailure);
      }
      // Only when nothing proved the queue reachable. A call that answered - an empty response, or an
      // OverLimit that SQS itself returned - is positive evidence that the endpoint, the credentials and
      // the queue are healthy, which is the very evidence used above to reset the transient-failure run.
      // Without this guard a genuinely drained queue that also happened to throttle one of its concurrent
      // polls would take ingestion down, and so would a batch that hit the in-flight ceiling on most calls
      // and was throttled on one - the outcome the backpressure classification exists to prevent.
      if (state.failedCalls > 0 && state.healthyResponses == 0) {
        throw new HoodieException(String.format(
            "Failed to receive any messages from SQS queue %s: %d of %d ReceiveMessage calls failed and none "
                + "succeeded", queueUrl, state.failedCalls, state.callsCompleted), state.firstFailure);
      }
    }
    return state.messages;
  }

  /**
   * How many empty ReceiveMessage responses are needed before the queue is treated as drained.
   *
   * <p>Under long polling AWS documents that ReceiveMessage "queries all servers for messages, sending a
   * response once at least one message is available ... An empty response is sent only if the polling
   * wait time expires". So an empty response is both strong evidence and expensive - it always costs the
   * full {@code longPollWait}. AWS scopes the residual risk to exactly the cheap case: "In rare cases,
   * you might receive empty responses even when a queue still contains messages, especially if you
   * specified a low value for Receive message wait time."
   *
   * <p>The count is therefore however many empties fit in {@value #DRAIN_CONFIRMATION_WINDOW_SECS}
   * seconds, which holds drain confirmation to roughly one poll window at every setting rather than
   * multiplying it by a fixed count. Short polling ({@code longPollWait <= 0}) samples only a subset of
   * servers and returns immediately, so its empties are both weak evidence and nearly free, and it keeps
   * the higher fixed count.
   *
   * <p>Empties are always counted as a batch total rather than consecutively, in both modes: consecutive
   * counting is what lets a queue with slow ingress reset the counter indefinitely and hold the phase for
   * the whole call budget. The trade is on the short-poll path, where an empty response on a queue that
   * still holds messages is expected rather than exceptional, so scattered empties can end a batch before
   * its target is reached. That costs throughput on a non-default setting, not correctness: confirming a
   * drain early only defers the remainder to the next ingestion cycle, it never drops a message.
   *
   * <p>Deliberately not scaled by the worker count, even though at the default this returns 1 while
   * {@code processingParallelism} defaults to 16. Concurrent polls are not independent draws: they
   * observe the same queue over the same overlapping window, and a long-poll empty reports queue state
   * ("nothing was visible on any server for the full wait") rather than a per-call sample, so a shared
   * cause does not compound across workers. The sampling that does produce false empties belongs to
   * short polling, which queries only a subset of servers - which is why AWS scopes the residual risk to
   * low wait times, and why {@code longPollWait} is already the right variable to key on. A worker-count
   * term would also be inert precisely where it is needed: at {@code longPollWait=1} the poll-cost term
   * already demands 20 empties, while at the default it would raise 1 to 8 in the regime where a single
   * empty is strongest. It would additionally make the threshold depend on batch size, since the worker
   * count is {@code min(processingParallelism, plannedReceiveCalls)}.
   */
  static int emptyReceivesToConfirmDrain(int longPollWait) {
    if (longPollWait <= 0) {
      return MAX_EMPTY_RECEIVES_SHORT_POLL;
    }
    return Math.max(1, DRAIN_CONFIRMATION_WINDOW_SECS / longPollWait);
  }

  /**
   * Drives the receive phase: prime the pool, then consume one completion at a time and refill the freed
   * slot immediately. {@link CompletionService#take()} blocks on <em>any</em> completion, never on all of
   * them, which is what removes the wave barrier - a worker that returns in 40ms starts its next call
   * without waiting for a sibling that is long-polling for 20s.
   *
   * <p>The loop runs until nothing is outstanding. Once a stop condition is set, no further calls are
   * dispatched but the calls already in flight are still drained and their messages kept: cancelling them
   * would discard responses for calls that already succeeded server-side and strand those messages for
   * the full visibility timeout.
   */
  private void runReceiveLoop(ExecutorService pool, Callable<List<Message>> receiveTask,
                              ReceiveState state, ReceivePlan plan, String queueUrl) {
    CompletionService<List<Message>> completionService = new ExecutorCompletionService<>(pool);
    try {
      dispatchReceiveCalls(completionService, receiveTask, state, plan);
      while (state.outstanding > 0) {
        Future<List<Message>> completed = completionService.take();
        state.outstanding--;
        state.callsCompleted++;
        foldReceiveResult(completed, state, plan, queueUrl);
        dispatchReceiveCalls(completionService, receiveTask, state, plan);
      }
    } catch (InterruptedException e) {
      // Only take() on this master thread throws this, i.e. the driver thread was interrupted, which in
      // practice only happens while the ingestion service is being torn down. This path discards the
      // messages already collected, and since their receipt handles go with them they stay in-flight
      // until the visibility timeout expires before another batch can see them. That is accepted because
      // the batch is being abandoned anyway - there is nobody left to commit it, so returning a partial
      // result would only push the same messages into a checkpoint that will never be written. It is a
      // delay, never message loss.
      Thread.currentThread().interrupt();
      throw new HoodieException("Interrupted while receiving messages from SQS queue " + queueUrl, e);
    }
  }

  /**
   * Fills every free worker slot with a new ReceiveMessage call, subject to {@link #canDispatchMore}.
   * Called once to prime the pool and again after each completion, so the pool stays saturated while
   * work remains.
   */
  private static void dispatchReceiveCalls(CompletionService<List<Message>> completionService,
                                           Callable<List<Message>> receiveTask,
                                           ReceiveState state, ReceivePlan plan) {
    while (state.outstanding < plan.workers && canDispatchMore(state, plan)) {
      completionService.submit(receiveTask);
      state.outstanding++;
      state.callsIssued++;
    }
  }

  /**
   * Whether another ReceiveMessage call is worth dispatching. The in-flight term optimistically credits
   * every outstanding call with a full {@code maxMessagesPerRequest}, so a call is never dispatched that
   * the messages already in flight would make redundant. That bounds the residual overshoot of
   * {@code maxMessagePerBatch} to {@code maxMessagesPerRequest - 1}.
   */
  static boolean canDispatchMore(ReceiveState state, ReceivePlan plan) {
    return !state.stopped()
        && state.callsIssued < plan.receiveCallBudget
        && state.messages.size() + (long) state.outstanding * plan.maxMessagesPerRequest < plan.numMessagesToProcess;
  }

  /**
   * Folds one completed ReceiveMessage call into the loop state on the master thread: merge its messages,
   * or classify and account for its failure.
   */
  private void foldReceiveResult(Future<List<Message>> completed, ReceiveState state,
                                 ReceivePlan plan, String queueUrl) throws InterruptedException {
    List<Message> messages;
    try {
      messages = completed.get();
    } catch (ExecutionException e) {
      // An Error (OutOfMemoryError, a LinkageError from a missing SDK class) says nothing about the
      // health of the queue and is not something to absorb: the JVM or the classpath is broken, and
      // counting it as one more tolerated failure could return a "successful" partial batch after a
      // fatal condition. Propagate it unchanged - the caller's finally block still shuts the pool down.
      if (e.getCause() instanceof Error) {
        throw (Error) e.getCause();
      }
      handleReceiveFailure(e.getCause(), state, queueUrl);
      return;
    }
    // Any successful call - including an empty one - proves the endpoint and credentials are healthy.
    state.consecutiveTransientFailures = 0;
    state.healthyResponses++;
    if (messages.isEmpty()) {
      // Empties are a batch total that never resets. Resetting on a stray message is what let a
      // slow-ingress queue hold the phase for its entire call budget, and a total is what bounds the
      // drain-confirmation cost to one poll window.
      if (++state.emptyReceives >= plan.emptiesToConfirmDrain) {
        state.stop(ReceiveExitReason.DRAINED);
      }
      return;
    }
    long before = state.messages.size();
    state.messages.addAll(messages);
    maybeLogReceiveProgress(queueUrl, before, state.messages.size(), state.callsCompleted);
  }

  /**
   * Applies the classification in {@link #classifySqsFailure} to one failed call. Backpressure and
   * non-retryable failures stop dispatch at once; transient ones are tolerated until
   * {@value #MAX_CONSECUTIVE_TRANSIENT_FAILURES} occur in a row.
   */
  private void handleReceiveFailure(Throwable cause, ReceiveState state, String queueUrl) {
    switch (classifySqsFailure(cause)) {
      case BACKPRESSURE:
        // Deliberately not counted as a failed call: it is normal saturation, and counting it would make
        // an OverLimit-only batch throw below as though the queue were broken. It does count as a healthy
        // response - SQS answered, so the endpoint and credentials demonstrably work.
        state.healthyResponses++;
        state.stop(ReceiveExitReason.IN_FLIGHT_LIMIT);
        log.warn("SQS reported the in-flight message limit ({}) for queue {} after fetching {} messages; ending the "
                + "receive phase. Received messages stay in-flight until onCommit deletes them, which frees the quota, "
                + "so the remaining backlog is picked up by the next ingestion cycle.",
            SQS_OVER_LIMIT_ERROR_CODE, queueUrl, state.messages.size(), cause);
        break;
      case FATAL:
        state.recordFailure(cause);
        // First one wins, matching stop() and recordFailure(): when several concurrent calls fail fatally
        // in the same drain, the exception thrown must carry the same cause the summary reports.
        if (state.fatalFailure == null) {
          state.fatalFailure = cause;
        }
        state.stop(ReceiveExitReason.FATAL_ERROR);
        log.error("Non-retryable SQS ReceiveMessage failure for queue {} after fetching {} messages; ending the receive "
                + "phase immediately rather than retrying a condition no retry can fix. Check the queue url, its "
                + "region, and the credentials/permissions of this job.",
            queueUrl, state.messages.size(), cause);
        break;
      default:
        state.recordFailure(cause);
        if (++state.consecutiveTransientFailures >= MAX_CONSECUTIVE_TRANSIENT_FAILURES) {
          state.stop(ReceiveExitReason.CONSECUTIVE_FAILURES);
        }
        break;
    }
  }

  /**
   * Emits the receive phase's summary, any condition-specific warnings, and the parallelism perf line.
   */
  private void logReceiveOutcome(String queueUrl, ReceiveState state, ReceivePlan plan, long wallMs, long busyNanos) {
    log.info("SQS receive summary for queue {}: fetched {} messages across {} receive calls "
            + "({} failed), exitReason={}.",
        queueUrl, state.messages.size(), state.callsCompleted, state.failedCalls, state.exitReason);
    if (state.failedCalls > 0) {
      log.warn("{} of {} SQS ReceiveMessage calls failed for queue {}; continued with the {} messages that "
              + "were received. Undelivered messages stay in the queue and are picked up by the next batch.",
          state.failedCalls, state.callsCompleted, queueUrl, state.messages.size(), state.firstFailure);
    }
    if (state.exitReason == ReceiveExitReason.CALL_BUDGET_EXHAUSTED) {
      log.warn("SQS receive call budget exhausted for queue {}: {} calls yielded {} of {} targeted messages "
              + "(plannedReceiveCalls={}, budgetFactor={}). The queue is returning short responses; the "
              + "remaining backlog is left for the next ingestion cycle.",
          queueUrl, state.callsCompleted, state.messages.size(), plan.numMessagesToProcess, plan.plannedReceiveCalls,
          RECEIVE_CALL_BUDGET_FACTOR);
    }
    logParallelPerf("receive", queueUrl, plan.workers, state.callsCompleted, wallMs, busyNanos);
  }

  /**
   * Emits a bounded receive-progress DEBUG line only when the cumulative fetched count crosses a
   * {@link #MESSAGE_PROGRESS_LOG_INTERVAL} boundary, so logging stays at a handful of lines per batch
   * regardless of parallelism.
   */
  private void maybeLogReceiveProgress(String queueUrl, long before, long after, long receiveCalls) {
    if (before / MESSAGE_PROGRESS_LOG_INTERVAL != after / MESSAGE_PROGRESS_LOG_INTERVAL) {
      log.debug("SQS receive progress for queue {}: {} messages fetched across {} receive calls.",
          queueUrl, after, receiveCalls);
    }
  }

  /**
   * Probes queue counters when the batch ends on empty responses, to diagnose early termination
   * while a backlog remains: received messages stay in-flight until onCommit deletes them, so an SQS
   * in-flight ceiling can cap the batch far below approxAvailable. Purely diagnostic - never lets the
   * probe fail the batch.
   */
  private void logReceiveBreakProbe(SqsClient sqsClient, String queueUrl, int visibilityTimeout, long emptyReceives,
                                    long fetched, long receiveCalls, long numMessagesToProcess) {
    Map<String, String> attributesAtBreak = Collections.emptyMap();
    try {
      attributesAtBreak = getSqsQueueAttributes(sqsClient, queueUrl);
    } catch (Exception e) {
      log.warn("Failed to probe SQS queue attributes at receive-loop break for queue {}; "
          + "continuing without in-flight counters.", queueUrl, e);
    }
    log.warn("SQS ReceiveMessage returned {} empty responses for queue {} after fetching {} messages "
            + "across {} receive calls (numMessagesToProcess={}); ending batch early. approxAvailable(now)={}, "
            + "approxInFlight(now)={}, approxDelayed(now)={}, visibilityTimeout={}s. Received messages remain "
            + "in-flight until onCommit delete, so an in-flight ceiling can cap the batch below the backlog.",
        emptyReceives, queueUrl, fetched, receiveCalls, numMessagesToProcess,
        attrOrUnknown(attributesAtBreak, SQS_ATTR_APPROX_MESSAGES),
        attrOrUnknown(attributesAtBreak, SQS_ATTR_APPROX_MESSAGES_NOT_VISIBLE),
        attrOrUnknown(attributesAtBreak, SQS_ATTR_APPROX_MESSAGES_DELAYED),
        visibilityTimeout);
  }

  /**
   * Create partitions of list using specific batch size. we can't use third party API for this
   * functionality, due to https://github.com/apache/hudi/blob/master/style/checkstyle.xml#L270
   */
  protected List<List<MessageTracker>> createListPartitions(List<MessageTracker> singleList, int eachBatchSize) {
    List<List<MessageTracker>> listPartitions = new ArrayList<>();
    if (singleList.size() == 0 || eachBatchSize < 1) {
      return listPartitions;
    }
    for (int start = 0; start < singleList.size(); start += eachBatchSize) {
      int end = Math.min(start + eachBatchSize, singleList.size());
      if (start > end) {
        throw new IndexOutOfBoundsException(
            "Index " + start + " is out of the list range <0," + (singleList.size() - 1) + ">");
      }
      listPartitions.add(new ArrayList<>(singleList.subList(start, end)));
    }
    return listPartitions;
  }

  /**
   * Deletes one batch (at most {@value #SQS_BATCH_MAX_ENTRIES} entries, the SQS API cap) of messages
   * and returns the trackers SQS reported as failed, so the caller can retry them. DeleteMessageBatch
   * can return partial failures even on an HTTP 200, and undeleted messages stay in-flight and are
   * redelivered once the visibility timeout expires, so failures must be surfaced rather than only
   * counted.
   *
   * <p>A synthetic per-entry id (the batch-local index) is used instead of the message id: the id
   * only has to be unique within the batch, and under at-least-once delivery the same message id can
   * appear twice, which would otherwise collide and make SQS reject the whole batch.
   */
  protected List<FailedDelete> deleteBatchOfMessages(SqsClient sqs, String queueUrl, List<MessageTracker> messagesToBeDeleted) {
    if (messagesToBeDeleted.isEmpty()) {
      return Collections.emptyList();
    }
    ValidationUtils.checkArgument(messagesToBeDeleted.size() <= SQS_BATCH_MAX_ENTRIES,
        "DeleteMessageBatch accepts at most " + SQS_BATCH_MAX_ENTRIES + " entries per call, got "
            + messagesToBeDeleted.size());
    List<DeleteMessageBatchRequestEntry> deleteEntries = new ArrayList<>(messagesToBeDeleted.size());
    for (int i = 0; i < messagesToBeDeleted.size(); i++) {
      deleteEntries.add(
          DeleteMessageBatchRequestEntry.builder()
                  .id(String.valueOf(i))
                  .receiptHandle(messagesToBeDeleted.get(i).receiptHandle)
                  .build());
    }
    DeleteMessageBatchResponse deleteResponse = sqs.deleteMessageBatch(
        DeleteMessageBatchRequest.builder().queueUrl(queueUrl).entries(deleteEntries).build());
    if (deleteResponse.failed().isEmpty()) {
      log.debug("Successfully deleted {} messages from queue.", deleteEntries.size());
      return Collections.emptyList();
    }
    // Keep the SQS-provided reason (code + senderFault) with each failed tracker so the caller can log
    // why deletion failed once at the end rather than per batch.
    List<FailedDelete> failed = new ArrayList<>(deleteResponse.failed().size());
    for (BatchResultErrorEntry error : deleteResponse.failed()) {
      MessageTracker tracker = trackerForEntryId(messagesToBeDeleted, error.id());
      if (tracker == null) {
        // SQS echoed an entry id that was never sent (ids are batch-local indices assigned just above, so
        // this is a protocol violation rather than something that happens in practice). The failure cannot
        // be attributed to a message: there is no receipt handle to retry with, and it cannot be added to
        // the residual set either, since that set is counted against processedMessages to derive the
        // deleted total and a phantom entry would corrupt it. So it is reported here and nowhere else -
        // meaning the summary's deleted count is an upper bound whenever this WARN fires. The affected
        // message stays in-flight and is redelivered after the visibility timeout regardless.
        log.warn("SQS reported a delete failure for unknown entry id \"{}\" on queue {} (code={}); it cannot be "
            + "mapped back to a message, so that message's deletion status is unknown and the deleted count "
            + "below may over-count by one.",
            error.id(), queueUrl, error.code());
        continue;
      }
      failed.add(FailedDelete.reported(tracker, error));
    }
    // Per-batch detail at DEBUG only; residual failures are summarized once at WARN in
    // deleteProcessedMessages so a transient blip across many batches does not flood the logs.
    log.debug("Failed to delete {} messages out of {} from queue {}.", failed.size(), deleteEntries.size(), queueUrl);
    return failed;
  }

  /**
   * Resolves an entry id assigned by {@link #deleteBatchOfMessages} back to its message. The id is the
   * batch-local index, so no lookup map is needed. Returns {@code null} when SQS echoes an id that was
   * never sent (non-numeric or out of range), which the caller reports rather than ignores.
   */
  private static MessageTracker trackerForEntryId(List<MessageTracker> batch, String entryId) {
    try {
      int index = Integer.parseInt(entryId);
      return index >= 0 && index < batch.size() ? batch.get(index) : null;
    } catch (NumberFormatException e) {
      return null;
    }
  }

  /**
   * Delete Queue Messages after hudi commit. This method will be invoked by source.onCommit.
   * Deletes run concurrently across a fixed pool of up to {@link #processingParallelism} threads;
   * batches that fail - whether SQS reported the entry as failed on an HTTP 200 or the call threw
   * outright - are collected across all batches and retried up to {@value #DELETE_MAX_RETRIES} times
   * with exponential backoff, since undeleted messages stay in-flight, consume the in-flight quota, and
   * are redelivered once the visibility timeout expires. Any residual failures after the retries are
   * logged with their reason (see {@link #logDeleteFailures}).
   *
   * <p>A failed delete is deliberately not fatal. This runs from {@code Source.onCommit}, which
   * StreamSync calls <em>after</em> the Hudi commit has already landed, and HoodieStreamer turns any
   * exception out of the sync round into a job shutdown - so aborting here on one throttled call would
   * take ingestion down over a condition SQS itself recovers from by redelivering. The one exception is
   * a systemic failure: if calls were throwing and not a single message could be deleted, that is a
   * queue/credential/network problem rather than stale individual entries, and it is surfaced.
   */
  public void deleteProcessedMessages(SqsClient sqs, String queueUrl, List<MessageTracker> processedMessages) {
    if (processedMessages.isEmpty()) {
      return;
    }
    long startMs = System.currentTimeMillis();
    // create batch for deletion, DeleteMessageBatchRequest only accepts max SQS_BATCH_MAX_ENTRIES entries
    List<List<MessageTracker>> deleteBatches = createListPartitions(processedMessages, SQS_BATCH_MAX_ENTRIES);
    int totalBatches = deleteBatches.size();
    int workers = Math.max(1, Math.min(processingParallelism, totalBatches));
    AtomicLong busyNanos = new AtomicLong();
    ExecutorService pool = newFixedThreadPool(DELETE_THREAD_PREFIX, workers);
    // Failures SQS attributes to this caller (senderFault=true, e.g. ReceiptHandleIsInvalid once the
    // visibility timeout has expired) cannot succeed on a retry, so they are set aside by the pass that
    // reported them instead of consuming the retry budget - and are still reported at the end.
    List<FailedDelete> permanentFailures = new ArrayList<>();
    DeleteStats stats = new DeleteStats();
    int retries = 0;
    try {
      DeletePass pass = deleteBatchesConcurrently(pool, sqs, queueUrl, deleteBatches, busyNanos);
      stats.add(pass);
      List<FailedDelete> retryable = splitOffPermanentFailures(pass, permanentFailures);
      while (!retryable.isEmpty() && retries < DELETE_MAX_RETRIES) {
        retries++;
        long backoffMs = sleepBeforeDeleteRetry(queueUrl, retryable.size(), retries);
        List<MessageTracker> retryTrackers =
            retryable.stream().map(failedDelete -> failedDelete.message).collect(Collectors.toList());
        pass = deleteBatchesConcurrently(pool, sqs, queueUrl,
            createListPartitions(retryTrackers, SQS_BATCH_MAX_ENTRIES), busyNanos);
        stats.add(pass);
        retryable = splitOffPermanentFailures(pass, permanentFailures);
        log.debug("Delete retry {} for queue {} (backoff {} ms) left {} messages still failing.",
            retries, queueUrl, backoffMs, retryable.size());
      }
      List<FailedDelete> residual = new ArrayList<>(permanentFailures);
      residual.addAll(retryable);
      long wallMs = System.currentTimeMillis() - startMs;
      int deleted = processedMessages.size() - residual.size();
      log.info("Deleted {} of {} processed messages from queue {} across {} delete batches and {} SQS calls "
              + "({} calls failed, {} retry passes) in {} ms ({} messages failed to delete).",
          deleted, processedMessages.size(), queueUrl, totalBatches, stats.calls, stats.failedCalls,
          retries, wallMs, residual.size());
      if (stats.failedCalls > 0) {
        // The residual WARN below groups by reason but carries no stack trace; log the first cause once
        // here so the SDK-level reason (throttling, connection acquisition timeout, ...) is recoverable.
        log.warn("{} of {} SQS DeleteMessageBatch calls failed for queue {}; their batches were retried and "
                + "{} messages remain undeleted. Undeleted messages are redelivered after the visibility "
                + "timeout, so ingestion may see them again rather than losing them.",
            stats.failedCalls, stats.calls, queueUrl, residual.size(), stats.firstThrown);
      }
      if (stats.fatalThrown != null) {
        // A non-retryable failure that struck only part of the phase (a session token expiring mid-run,
        // say) leaves deleted > 0, so the systemic throw below stays silent by design. Without this it
        // would be reported only as an indistinguishable residual-failure WARN, so name the condition
        // explicitly: no retry can clear it, and it will recur on every commit until an operator acts.
        log.error("Non-retryable SQS DeleteMessageBatch failure for queue {}; stopped dispatching further "
                + "delete batches ({} batches skipped) and did not retry, since no retry can succeed. {} of {} "
                + "messages remain undeleted and will be redelivered after the visibility timeout. Check the "
                + "queue url, its region, and the credentials/permissions of this job.",
            queueUrl, stats.skippedBatches, residual.size(), processedMessages.size(), stats.fatalThrown);
      }
      if (!residual.isEmpty()) {
        logDeleteFailures(queueUrl, residual, retries);
      }
      logParallelPerf("delete", queueUrl, workers, stats.calls, wallMs, busyNanos.get());
      // Nothing at all could be deleted and calls were throwing: systemic (bad credentials, queue gone,
      // network down) rather than individual stale entries, and the caller is about to clear its tracked
      // messages as if they had been handled, so surface it. A purely senderFault residue is deliberately
      // not fatal - every receipt handle being stale is a real queue state that no retry or job failure
      // can fix, and the messages are redelivered regardless.
      //
      // NOTE: this deliberately does not fire when every entry failed server-side on an HTTP 200
      // (senderFault=false, e.g. InternalError) with no call ever throwing. That is an equally total
      // failure to delete, but SQS reports genuinely broken plumbing - bad credentials, a deleted queue -
      // by throwing, so firstThrown is the sharper signal, and escalating a persistent per-entry
      // condition into a job shutdown would trade at-least-once redelivery for an ingestion outage.
      // Making that case visible belongs to the residual-failure metric tracked as follow-up, not here.
      // The extra answeredCalls() guard: a DeleteMessageBatch that returned HTTP 200 proves the queue is
      // reachable and the credentials work, even if it reported every entry as failed. Without it, the
      // deliberately-non-fatal all-senderFault case above (every receipt handle stale after a long commit)
      // turns into a job shutdown as soon as one unrelated call is also throttled - and fanning out makes
      // such a throttle more likely, not less.
      if (deleted == 0 && stats.firstThrown != null && stats.answeredCalls() == 0) {
        throw new HoodieException("Failed to delete any of the " + processedMessages.size()
            + " processed messages from SQS queue " + queueUrl + " (" + stats.failedCalls + " of "
            + stats.calls + " DeleteMessageBatch calls threw, " + residual.size()
            + " messages still undeleted after " + retries + " retries)", stats.firstThrown);
      }
    } finally {
      shutdownThreadPool(pool);
    }
  }

  /**
   * Deletes the given batches concurrently on {@code pool} and returns the outcome of the pass: every
   * message that was not deleted, plus the call stats. Each batch's SQS call time is accumulated into
   * {@code busyNanos} so the caller can log the serialized-equivalent time and prove the deletes
   * actually overlapped.
   */
  private DeletePass deleteBatchesConcurrently(ExecutorService pool, SqsClient sqs, String queueUrl,
                                               List<List<MessageTracker>> deleteBatches, AtomicLong busyNanos) {
    int workers = Math.max(1, Math.min(processingParallelism, deleteBatches.size()));
    CompletionService<BatchOutcome> completionService = new ExecutorCompletionService<>(pool);
    Iterator<List<MessageTracker>> pending = deleteBatches.iterator();
    DeletePass pass = new DeletePass();
    int outstanding = 0;
    try {
      // Dispatch incrementally rather than queueing every batch up front. The pool stays saturated either
      // way, but holding the untried batches back is what makes fail-fast possible: on a non-retryable
      // failure the remaining batches are simply never submitted. With a deleted queue or revoked
      // credentials that is the difference between ceil(M/10) doomed calls and roughly one per worker.
      outstanding += dispatchDeleteBatches(completionService, pending, sqs, queueUrl, busyNanos,
          workers - outstanding);
      while (outstanding > 0) {
        Future<BatchOutcome> completed = completionService.take();
        outstanding--;
        pass.calls++;
        BatchOutcome outcome = outcomeOf(completed);
        if (outcome.thrown == null) {
          pass.failures.addAll(outcome.failures);
        } else {
          // A thrown call leaves its whole batch undeleted, exactly like an SQS-reported entry failure, so
          // it goes through the same path. Aborting the pass outright would discard the failures already
          // collected from the other batches, skip the retries entirely, and propagate out of onCommit
          // into a job shutdown (see deleteProcessedMessages). Fanning out makes a transient throttle or
          // connection-acquisition timeout more likely, not less, so it must be survivable.
          pass.failedCalls++;
          if (pass.firstThrown == null) {
            pass.firstThrown = outcome.thrown;
          }
          if (classifySqsFailure(outcome.thrown) == SqsFailureKind.FATAL) {
            // Nothing that follows can succeed, so stop handing out work. The batches already in flight
            // still drain below - their messages are recorded as failures either way, and cancelling them
            // would only lose the outcome of calls that may well have succeeded.
            if (pass.fatalThrown == null) {
              pass.fatalThrown = outcome.thrown;
            }
          }
          for (MessageTracker message : outcome.batch) {
            pass.failures.add(FailedDelete.thrown(message, outcome.thrown));
          }
        }
        if (pass.fatalThrown == null) {
          outstanding += dispatchDeleteBatches(completionService, pending, sqs, queueUrl, busyNanos,
              workers - outstanding);
        }
      }
      if (pass.fatalThrown != null) {
        // Everything never dispatched is undeleted too, and must be accounted for or the summary would
        // report those messages as deleted. They are recorded against the fatal cause, which
        // FailedDelete.thrown classifies as permanent, so they are reported rather than retried.
        while (pending.hasNext()) {
          for (MessageTracker message : pending.next()) {
            pass.failures.add(FailedDelete.thrown(message, pass.fatalThrown));
          }
          pass.skippedBatches++;
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new HoodieException("Interrupted while deleting messages from SQS queue " + queueUrl, e);
    }
    return pass;
  }

  /**
   * Submits up to {@code slots} more delete batches, returning how many were actually dispatched.
   */
  private int dispatchDeleteBatches(CompletionService<BatchOutcome> completionService,
                                    Iterator<List<MessageTracker>> pending, SqsClient sqs, String queueUrl,
                                    AtomicLong busyNanos, int slots) {
    int dispatched = 0;
    while (dispatched < slots && pending.hasNext()) {
      List<MessageTracker> deleteBatch = pending.next();
      completionService.submit(() -> {
        long callStartNanos = System.nanoTime();
        try {
          return new BatchOutcome(deleteBatch, deleteBatchOfMessages(sqs, queueUrl, deleteBatch), null);
        } catch (Exception e) {
          // Returned rather than thrown so the outcome carries its own batch: a CompletionService future
          // cannot be mapped back to its task. An Error is deliberately not caught here and still
          // surfaces through ExecutionException - see outcomeOf.
          return new BatchOutcome(deleteBatch, Collections.emptyList(), e);
        } finally {
          busyNanos.addAndGet(System.nanoTime() - callStartNanos);
        }
      });
      dispatched++;
    }
    return dispatched;
  }

  /**
   * Unwraps a completed delete task. Only an {@link Error} can surface as an {@link ExecutionException}
   * here, since the task itself converts every {@code Exception} into a {@link BatchOutcome}. An Error
   * (OutOfMemoryError, a LinkageError from a missing SDK class) is not a delete failure and must not be
   * funnelled into the retry path: the JVM or the classpath is broken, and retrying the batch would only
   * hide a fatal condition behind a residual-failure log line. Propagate it, mirroring the receive loop.
   */
  private static BatchOutcome outcomeOf(Future<BatchOutcome> completed) throws InterruptedException {
    try {
      return completed.get();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof Error) {
        throw (Error) e.getCause();
      }
      throw new HoodieException("Unexpected failure completing an SQS delete batch", e.getCause());
    }
  }

  /**
   * Moves the pass's permanent failures - the ones no retry can make succeed - into
   * {@code permanentSink} and returns the ones worth retrying. See {@link FailedDelete#permanent}.
   */
  private static List<FailedDelete> splitOffPermanentFailures(DeletePass pass, List<FailedDelete> permanentSink) {
    List<FailedDelete> retryable = new ArrayList<>();
    for (FailedDelete failedDelete : pass.failures) {
      if (failedDelete.permanent) {
        permanentSink.add(failedDelete);
      } else {
        retryable.add(failedDelete);
      }
    }
    return retryable;
  }

  /**
   * Classifies a failed SQS call. Shared by the receive and delete phases, because SQS reports the same
   * conditions the same way on both.
   *
   * <p>SQS status codes do not track severity - {@code RequestThrottled} and {@code OverLimit} are HTTP
   * 400, {@code ThrottlingException} is HTTP 403, while {@code MalformedQueryString} is HTTP 404 - so the
   * error code decides and the status code is only the fallback for codes we do not recognise.
   *
   * <p>{@code AwsServiceException.isThrottlingException()} matches on error code rather than status, but
   * its set omits {@code KmsThrottled}; {@link #EXTRA_TRANSIENT_ERROR_CODES} covers that gap. The
   * fallback degrades safely in both directions: an unrecognised 5xx is transient, an unrecognised 4xx is
   * non-retryable, and an exception carrying no usable status at all is treated as transient rather than
   * failing the operation outright.
   */
  static SqsFailureKind classifySqsFailure(Throwable cause) {
    if (cause instanceof AwsServiceException) {
      AwsServiceException serviceException = (AwsServiceException) cause;
      String errorCode = serviceException.awsErrorDetails() != null
          ? serviceException.awsErrorDetails().errorCode() : null;
      if (SQS_OVER_LIMIT_ERROR_CODE.equals(errorCode)) {
        return SqsFailureKind.BACKPRESSURE;
      }
      if (serviceException.isThrottlingException() || EXTRA_TRANSIENT_ERROR_CODES.contains(errorCode)) {
        return SqsFailureKind.TRANSIENT;
      }
      int statusCode = serviceException.statusCode();
      if (statusCode >= 500) {
        return SqsFailureKind.TRANSIENT;
      }
      return statusCode >= 400 ? SqsFailureKind.FATAL : SqsFailureKind.TRANSIENT;
    }
    // SdkClientException and friends are transport-level (connection reset, socket timeout, connection
    // acquisition timeout) and therefore transient by nature.
    return cause instanceof SdkException ? SqsFailureKind.TRANSIENT : SqsFailureKind.FATAL;
  }

  /**
   * Logs the upcoming retry and sleeps for its exponential backoff, returning the backoff applied. The
   * first retry logs at INFO - one transient partial failure is routine and self-healing - and later
   * attempts at WARN, since a failure that survives a retry is worth an operator's attention.
   *
   * @return the backoff in millis that was slept
   */
  private static long sleepBeforeDeleteRetry(String queueUrl, int failedCount, int attempt) {
    long backoffMs = DELETE_RETRY_BASE_BACKOFF_MS << (attempt - 1);
    if (attempt == 1) {
      log.info("Retrying delete of {} failed SQS messages (attempt {}/{}) for queue {} after {} ms backoff.",
          failedCount, attempt, DELETE_MAX_RETRIES, queueUrl, backoffMs);
    } else {
      log.warn("Retrying delete of {} failed SQS messages (attempt {}/{}) for queue {} after {} ms backoff.",
          failedCount, attempt, DELETE_MAX_RETRIES, queueUrl, backoffMs);
    }
    try {
      Thread.sleep(backoffMs);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new HoodieException("Interrupted while backing off before an SQS delete retry for queue " + queueUrl, e);
    }
    return backoffMs;
  }

  /**
   * Logs why the residual messages could not be deleted after all retries, grouped by reason (SQS error
   * code + senderFault, or {@value #DELETE_CALL_FAILED_CODE} for a call that threw) and with a sample of
   * the affected message ids. Undeleted messages stay in-flight until the visibility timeout expires and
   * are then redelivered, so the reason - a transient server error vs. a permanent client fault such as
   * an expired receipt handle (senderFault=true) - is what on-call needs to decide whether to act, and
   * the message ids are what makes the queue or DLQ greppable.
   */
  private void logDeleteFailures(String queueUrl, List<FailedDelete> failed, int retries) {
    Map<String, Long> failuresByReason = failed.stream()
        .collect(Collectors.groupingBy(FailedDelete::reason, Collectors.counting()));
    List<String> sampleMessageIds = failed.stream()
        .limit(DELETE_FAILURE_SAMPLE_SIZE)
        .map(failedDelete -> failedDelete.message.messageId)
        .collect(Collectors.toList());
    log.warn("Failed to delete {} SQS messages from queue {} after {} retries; failures by reason: {}; "
            + "sample messageIds (up to {} of {}): {}; sample error: \"{}\". Undeleted messages remain "
            + "in-flight until the visibility timeout expires and are then redelivered - and redrive to the "
            + "DLQ once maxReceiveCount is reached, if a redrive policy is configured on the queue.",
        failed.size(), queueUrl, retries, failuresByReason, DELETE_FAILURE_SAMPLE_SIZE, failed.size(),
        sampleMessageIds, failed.get(0).errorMessage);
  }

  /**
   * Creates a fixed-size pool of daemon threads named with {@code namePrefix} for identifiability in
   * stack dumps. Daemon threads so a leaked pool can never block JVM shutdown.
   */
  private static ExecutorService newFixedThreadPool(String namePrefix, int size) {
    AtomicInteger threadIndex = new AtomicInteger();
    return Executors.newFixedThreadPool(size, runnable -> {
      Thread thread = new Thread(runnable, namePrefix + threadIndex.getAndIncrement());
      thread.setDaemon(true);
      return thread;
    });
  }

  /**
   * Shuts a receive/delete pool down. On the normal paths every task has already been joined by the time
   * this runs, so it returns promptly; the timeout only guards against a stuck AWS call on the paths that
   * abandon the phase with work still outstanding (a worker {@link Error}, or an interrupt). Restores the
   * interrupt flag on interruption per the standard contract.
   */
  private static void shutdownThreadPool(ExecutorService pool) {
    pool.shutdown();
    try {
      if (!pool.awaitTermination(POOL_SHUTDOWN_TIMEOUT_SECS, TimeUnit.SECONDS)) {
        pool.shutdownNow();
      }
    } catch (InterruptedException e) {
      pool.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Logs whether a fanned-out phase actually ran concurrently. {@code serialEquivMs} is the summed
   * wall time of every individual SQS call; {@code speedup = serialEquivMs / wallMs}. A speedup near
   * 1 means the calls ran serialized (a regression); a speedup near {@code parallelism} means they
   * overlapped as intended.
   */
  private void logParallelPerf(String phase, String queueUrl, int parallelism, long calls, long wallMs, long busyNanos) {
    long serialEquivMs = busyNanos / 1_000_000L;
    double speedup = wallMs > 0 ? (double) serialEquivMs / wallMs : 1.0;
    log.info("SQS {} perf for queue {}: parallelism={}, calls={}, wallMs={}, serialEquivMs={}, speedup={}x.",
        phase, queueUrl, parallelism, calls, wallMs, serialEquivMs,
        String.format(Locale.ROOT, "%.1f", speedup));
  }

  /**
   * Configs supported.
   */
  public static class Config {
    /**
     * {@link  #S3_SOURCE_QUEUE_URL} is the queue url for cloud object events.
     */
    @Deprecated
    public static final String S3_SOURCE_QUEUE_URL = S3SourceConfig.S3_SOURCE_QUEUE_URL.key();

    /**
     * {@link  #S3_SOURCE_QUEUE_REGION} is the case-sensitive region name of the cloud provider for the queue. For example, "us-east-1".
     */
    @Deprecated
    public static final String S3_SOURCE_QUEUE_REGION = S3SourceConfig.S3_SOURCE_QUEUE_REGION.key();

    /**
     * {@link  #S3_SOURCE_QUEUE_FS} is file system corresponding to queue. For example, for AWS SQS it is s3/s3a.
     */
    @Deprecated
    public static final String S3_SOURCE_QUEUE_FS = S3SourceConfig.S3_SOURCE_QUEUE_FS.key();

    /**
     * {@link  #S3_QUEUE_LONG_POLL_WAIT} is the long poll wait time in seconds If set as 0 then
     * client will fetch on short poll basis.
     */
    @Deprecated
    public static final String S3_QUEUE_LONG_POLL_WAIT = S3SourceConfig.S3_QUEUE_LONG_POLL_WAIT.key();

    /**
     * {@link  #S3_SOURCE_QUEUE_MAX_MESSAGES_PER_BATCH} is max messages for each batch of Hudi Streamer
     * run. Source will process these maximum number of message at a time.
     */
    @Deprecated
    public static final String S3_SOURCE_QUEUE_MAX_MESSAGES_PER_BATCH = S3SourceConfig.S3_SOURCE_QUEUE_MAX_MESSAGES_PER_BATCH.key();

    /**
     * {@link  #S3_SOURCE_QUEUE_MAX_MESSAGES_PER_REQUEST} is max messages for each request.
     */
    @Deprecated
    public static final String S3_SOURCE_QUEUE_MAX_MESSAGES_PER_REQUEST = S3SourceConfig.S3_SOURCE_QUEUE_MAX_MESSAGES_PER_REQUEST.key();

    /**
     * {@link  #S3_SOURCE_QUEUE_VISIBILITY_TIMEOUT} is visibility timeout for messages in queue. After we
     * consume the message, queue will move the consumed messages to in-flight state, these messages
     * can't be consumed again by source for this timeout period.
     */
    @Deprecated
    public static final String S3_SOURCE_QUEUE_VISIBILITY_TIMEOUT = S3SourceConfig.S3_SOURCE_QUEUE_VISIBILITY_TIMEOUT.key();

    /**
     * {@link  #SOURCE_INPUT_SELECTOR} source input selector.
     */
    @Deprecated
    public static final String SOURCE_INPUT_SELECTOR = DFSPathSelectorConfig.SOURCE_INPUT_SELECTOR.key();
  }

  public static class MessageTracker {
    private final String messageId;
    private final String receiptHandle;

    MessageTracker(Message message) {
      this.messageId = message.messageId();
      this.receiptHandle = message.receiptHandle();
    }
  }

  /**
   * A message that could not be deleted, paired with the reason. Covers both ways a delete fails: SQS
   * reported the entry as failed on an otherwise successful DeleteMessageBatch, or the call threw and
   * the whole batch is undeleted. Normalising both into one type lets a single retry path and a single
   * summary handle them - see {@link #deleteProcessedMessages} and {@link #logDeleteFailures}.
   *
   * <p>{@link #permanent} is the retry decision. For an entry SQS reported, it is {@code senderFault}:
   * SQS sets that when the fault is the caller's (an expired receipt handle, say), which no retry can
   * fix. For a call that threw, it is whether {@link #classifySqsFailure} judged the cause non-retryable
   * - a deleted queue or revoked credentials is not made retryable by the fact that it surfaced as a
   * thrown call rather than a per-entry error, and retrying it would burn the entire retry budget on
   * calls that cannot succeed.
   */
  static final class FailedDelete {
    private final MessageTracker message;
    private final String code;
    private final boolean senderFault;
    private final boolean permanent;
    private final String errorMessage;

    private FailedDelete(MessageTracker message, String code, boolean senderFault, boolean permanent,
                         String errorMessage) {
      this.message = message;
      this.code = code;
      this.senderFault = senderFault;
      this.permanent = permanent;
      this.errorMessage = errorMessage;
    }

    /** SQS reported this entry as failed on an otherwise successful DeleteMessageBatch call. */
    static FailedDelete reported(MessageTracker message, BatchResultErrorEntry error) {
      boolean senderFault = Boolean.TRUE.equals(error.senderFault());
      return new FailedDelete(message,
          error.code() != null ? error.code() : UNKNOWN_ERROR_CODE,
          senderFault, senderFault,
          error.message() != null ? error.message() : NO_ERROR_MESSAGE);
    }

    /** The DeleteMessageBatch call threw, so SQS never reported per-entry status for this message. */
    static FailedDelete thrown(MessageTracker message, Throwable cause) {
      return new FailedDelete(message, DELETE_CALL_FAILED_CODE, false,
          classifySqsFailure(cause) == SqsFailureKind.FATAL,
          cause != null ? cause.toString() : NO_ERROR_MESSAGE);
    }

    /** Grouping key for the residual-failure summary. */
    String reason() {
      return code + " (senderFault=" + senderFault + ")";
    }
  }

  /**
   * Why the receive phase stopped dispatching, reported in the summary line for on-call.
   */
  enum ReceiveExitReason {
    /** The queue reported no available messages, so no receive call was made at all. */
    NO_MESSAGES,
    /** {@code numMessagesToProcess} was reached. */
    TARGET_REACHED,
    /** Enough empty responses accumulated to confirm the queue is drained. */
    DRAINED,
    /** SQS reported {@value #SQS_OVER_LIMIT_ERROR_CODE}: the in-flight quota is exhausted. */
    IN_FLIGHT_LIMIT,
    /** A non-retryable failure occurred. */
    FATAL_ERROR,
    /** {@value #MAX_CONSECUTIVE_TRANSIENT_FAILURES} transient failures occurred in a row. */
    CONSECUTIVE_FAILURES,
    /** The receive-call budget was spent while the queue kept returning short responses. */
    CALL_BUDGET_EXHAUSTED
  }

  /**
   * The immutable plan for one receive phase: what to fetch and the limits that bound the attempt.
   * Separated from {@link ReceiveState} so the loop's inputs cannot be confused with its running totals.
   */
  static final class ReceivePlan {
    private final long numMessagesToProcess;
    private final int maxMessagesPerRequest;
    private final int workers;
    private final long receiveCallBudget;
    private final int emptiesToConfirmDrain;
    private final long plannedReceiveCalls;

    ReceivePlan(long numMessagesToProcess, int maxMessagesPerRequest, int workers, long receiveCallBudget,
                int emptiesToConfirmDrain, long plannedReceiveCalls) {
      this.numMessagesToProcess = numMessagesToProcess;
      this.maxMessagesPerRequest = maxMessagesPerRequest;
      this.workers = workers;
      this.receiveCallBudget = receiveCallBudget;
      this.emptiesToConfirmDrain = emptiesToConfirmDrain;
      this.plannedReceiveCalls = plannedReceiveCalls;
    }
  }

  /**
   * Running state of one receive phase. Every field is touched only by the master thread, so all of it is
   * plain non-atomic state - the workers never see it.
   */
  static final class ReceiveState {
    private final List<Message> messages = new ArrayList<>();
    /** Calls submitted to the pool but not yet folded back in. */
    private int outstanding;
    private long callsIssued;
    private long callsCompleted;
    /** Excludes {@link SqsFailureKind#BACKPRESSURE}, which is saturation rather than a failure. */
    private long failedCalls;
    /**
     * Calls that proved the queue reachable: any successful response, plus an OverLimit, which SQS itself
     * returned and which therefore also demonstrates working credentials and a live queue. Used to decide
     * whether an empty result with some failures is a broken pipeline or a healthy one that got unlucky.
     */
    private long healthyResponses;
    private long emptyReceives;
    private int consecutiveTransientFailures;
    private ReceiveExitReason exitReason;
    private Throwable firstFailure;
    private Throwable fatalFailure;

    boolean stopped() {
      return exitReason != null;
    }

    /** Records the first stop condition to fire; later ones cannot mask why dispatch actually ended. */
    void stop(ReceiveExitReason reason) {
      if (exitReason == null) {
        exitReason = reason;
      }
    }

    void recordFailure(Throwable cause) {
      failedCalls++;
      if (firstFailure == null) {
        firstFailure = cause;
      }
    }
  }

  /**
   * Outcome of one concurrent pass of delete batches: every message the pass failed to delete, and the
   * call stats needed for the perf line and for telling a systemic failure from individual bad entries.
   */
  private static final class DeletePass {
    private final List<FailedDelete> failures = new ArrayList<>();
    private int calls;
    private int failedCalls;
    /** Batches never dispatched because a non-retryable failure stopped the pass. */
    private int skippedBatches;
    private Throwable firstThrown;
    /** Set when a call failed non-retryably, which stops this pass dispatching any more batches. */
    private Throwable fatalThrown;
  }

  /** Running totals across all delete passes (initial + retries), for the summary and perf lines. */
  private static final class DeleteStats {
    private int calls;
    private int failedCalls;
    private int skippedBatches;
    private Throwable firstThrown;
    private Throwable fatalThrown;

    void add(DeletePass pass) {
      calls += pass.calls;
      failedCalls += pass.failedCalls;
      skippedBatches += pass.skippedBatches;
      if (firstThrown == null) {
        firstThrown = pass.firstThrown;
      }
      if (fatalThrown == null) {
        fatalThrown = pass.fatalThrown;
      }
    }

    /**
     * Calls that got an answer from SQS. A DeleteMessageBatch that returned HTTP 200 proves the endpoint
     * resolved, the credentials authenticated and the queue exists, even when it reported every entry as
     * failed - which is exactly the evidence needed to tell a broken pipeline from a stale-handle batch.
     */
    int answeredCalls() {
      return calls - failedCalls;
    }
  }

  /**
   * Outcome of one DeleteMessageBatch task, carrying its own batch because a {@link CompletionService}
   * future cannot be mapped back to the task that produced it.
   */
  private static final class BatchOutcome {
    private final List<MessageTracker> batch;
    private final List<FailedDelete> failures;
    private final Throwable thrown;

    BatchOutcome(List<MessageTracker> batch, List<FailedDelete> failures, Throwable thrown) {
      this.batch = batch;
      this.failures = failures;
      this.thrown = thrown;
    }
  }

  /**
   * How a failed SQS call should be handled. See {@link #classifySqsFailure}.
   */
  enum SqsFailureKind {
    /** Retrying may succeed: throttling, a 5xx, a dropped connection. */
    TRANSIENT,
    /** A queue-level ceiling was hit. Stop issuing calls, but this is saturation, not a failure. */
    BACKPRESSURE,
    /** No retry can succeed: queue gone, bad credentials, malformed request. */
    FATAL
  }
}
