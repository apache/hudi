/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.event;

import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.sink.utils.CoordinationResponseSerDe;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.util.SerializedValue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Correspondent between a write task with the coordinator.
 */
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
public class Correspondent {

  /**
   * Initial backoff between two instant-time polls.
   */
  private static final long POLL_BASE_MS = 50L;

  /**
   * Upper bound of the backoff between two instant-time polls.
   */
  private static final long POLL_CAP_MS = 1000L;

  /**
   * Status of an instant-time request served by the coordinator.
   */
  public enum Status {
    /**
     * The instant time is ready to use.
     */
    READY,
    /**
     * The instant is still being created, the requester should poll again.
     */
    PENDING
  }

  private final OperatorID operatorID;
  private final TaskOperatorEventGateway gateway;

  @VisibleForTesting
  protected Correspondent() {
    this.operatorID = null;
    this.gateway = null;
  }

  /**
   * Creates a coordinator correspondent.
   *
   * @param operatorID The operator ID
   * @param gateway    The gateway
   *
   * @return an instance of {@code Correspondent}.
   */
  public static Correspondent getInstance(OperatorID operatorID, TaskOperatorEventGateway gateway) {
    return new Correspondent(operatorID, gateway);
  }

  /**
   * Requests the instant time for the given checkpoint from the coordinator.
   *
   * <p>The coordinator answers each request in O(1) with a {@link Status}: the requester polls with a
   * capped exponential backoff (plus jitter) under a single {@code pollBudgetMs} deadline until the
   * instant is {@code READY}, and retries transient transport errors within the same budget. A
   * {@code PENDING} reply never extends the deadline. Instant creation failures fail the job through
   * the coordinator's normal asynchronous failure path.
   *
   * @param checkpointId The checkpoint id (or -1 for bulk insert)
   * @param pollBudgetMs The overall budget to wait for an instant, in milliseconds
   *
   * @return the instant time to write with
   */
  public String requestInstantTime(long checkpointId, long pollBudgetMs) {
    final long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(pollBudgetMs);
    long backoffMs = POLL_BASE_MS;
    while (true) {
      InstantTimeResponse response;
      try {
        response = fetchInstantTimeResponse(checkpointId);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new HoodieException("Interrupted while requesting the instant time from the coordinator", e);
      } catch (Exception e) {
        // transient transport/coordinator error: retry within the budget, reusing the same checkpoint identity.
        if (System.nanoTime() >= deadlineNanos) {
          throw new HoodieException("Timeout requesting the instant time from the coordinator for checkpoint " + checkpointId, e);
        }
        backoffMs = sleepAndGrow(backoffMs);
        continue;
      }
      if (response.getStatus() == Status.READY) {
        return response.getInstant();
      }
      // PENDING: keep polling, but never reset the deadline.
      if (System.nanoTime() >= deadlineNanos) {
        throw new HoodieException("Timeout waiting for the instant time from the coordinator for checkpoint " + checkpointId);
      }
      backoffMs = sleepAndGrow(backoffMs);
    }
  }

  /**
   * Sends a single instant-time request to the coordinator and returns its response.
   *
   * <p>Isolated so tests can stub the transport while reusing the poll loop in {@link #requestInstantTime}.
   */
  protected InstantTimeResponse fetchInstantTimeResponse(long checkpointId) throws Exception {
    return CoordinationResponseSerDe.unwrap(this.gateway.sendRequestToCoordinator(this.operatorID,
        new SerializedValue<>(InstantTimeRequest.getInstance(checkpointId))).get());
  }

  private static long sleepAndGrow(long backoffMs) {
    long capped = Math.min(backoffMs, POLL_CAP_MS);
    // full jitter in [capped/2, capped] to avoid a thundering herd of polls landing together.
    long sleepMs = capped / 2 + ThreadLocalRandom.current().nextLong(capped / 2 + 1);
    try {
      Thread.sleep(sleepMs);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new HoodieException("Interrupted while backing off between instant time polls", e);
    }
    return Math.min(backoffMs << 1, POLL_CAP_MS);
  }

  /**
   * Sends a writing metadata event to the coordinator.
   */
  public void sendWriteMetadataEvent(WriteMetadataEvent writeMetadataEvent) {
    try {
      this.gateway.sendOperatorEventToCoordinator(this.operatorID, new SerializedValue<>(writeMetadataEvent));
    } catch (IOException e) {
      throw new HoodieException("Error sending write metadata event to the coordinator", e);
    }
  }

  /**
   * Sends a request to the coordinator to fetch the inflight instants.
   */
  public Map<Long, String> requestInflightInstants() {
    try {
      InflightInstantsResponse response = CoordinationResponseSerDe.unwrap(this.gateway.sendRequestToCoordinator(this.operatorID,
          new SerializedValue<>(InflightInstantsRequest.getInstance())).get());
      return response.getInflightInstants();
    } catch (Exception e) {
      throw new HoodieException("Error requesting the instant time from the coordinator", e);
    }
  }

  /**
   * A request for instant time with a given checkpoint id.
   */
  @AllArgsConstructor(access = AccessLevel.PRIVATE)
  @Getter
  public static class InstantTimeRequest implements CoordinationRequest {

    private final long checkpointId;

    public static InstantTimeRequest getInstance(long checkpointId) {
      return new InstantTimeRequest(checkpointId);
    }
  }

  /**
   * A response with instant time.
   */
  @AllArgsConstructor(access = AccessLevel.PRIVATE)
  @Getter
  public static class InstantTimeResponse implements CoordinationResponse {

    private final Status status;
    private final String instant;

    /**
     * The instant is ready to use.
     */
    public static InstantTimeResponse ready(String instant) {
      return new InstantTimeResponse(Status.READY, instant);
    }

    /**
     * The instant is still being created, the requester should poll again.
     */
    public static InstantTimeResponse pending() {
      return new InstantTimeResponse(Status.PENDING, null);
    }
  }

  /**
   * A request for the current inflight instants in the coordinator.
   */
  @AllArgsConstructor(access = AccessLevel.PRIVATE)
  @Getter
  public static class InflightInstantsRequest implements CoordinationRequest {

    public static InflightInstantsRequest getInstance() {
      return new InflightInstantsRequest();
    }
  }

  /**
   * A response with instant time.
   */
  @AllArgsConstructor(access = AccessLevel.PRIVATE)
  @Getter
  public static class InflightInstantsResponse implements CoordinationResponse {

    private final HashMap<Long, String> inflightInstants;

    public static InflightInstantsResponse getInstance(HashMap<Long, String> inflightInstants) {
      return new InflightInstantsResponse(inflightInstants);
    }
  }
}
