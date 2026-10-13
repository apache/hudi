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
import java.util.concurrent.TimeUnit;

/**
 * Correspondent between a write task with the coordinator.
 */
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
public class Correspondent {

  /**
   * Initial backoff in milliseconds between two instant-time polls, allowing for instant creation latency.
   */
  private static final long POLL_BASE_MS = 200L;

  /**
   * Upper bound in milliseconds of the backoff between two instant-time polls.
   */
  private static final long POLL_CAP_MS = 1000L;

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
   * <p>Polls with capped exponential backoff until the instant is non-null or the timeout expires.
   * Request failures are propagated immediately.
   *
   * @param checkpointId The checkpoint id (or -1 for bulk insert)
   * @param pollBudgetMs The overall budget to wait for an instant, in milliseconds
   *
   * @return the instant time to write with
   */
  public String requestInstantTime(long checkpointId, long pollBudgetMs) {
    final long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(pollBudgetMs);
    long backoffMs = POLL_BASE_MS;
    try {
      do {
        String instant = fetchInstantTimeResponse(checkpointId).getInstant();
        if (instant != null) {
          return instant;
        }
        long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos <= 0) {
          break;
        }
        TimeUnit.NANOSECONDS.sleep(Math.min(remainingNanos, TimeUnit.MILLISECONDS.toNanos(backoffMs)));
        backoffMs = Math.min(backoffMs * 2, POLL_CAP_MS);
      } while (System.nanoTime() < deadlineNanos);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new HoodieException("Interrupted while requesting the instant time from the coordinator", e);
    } catch (Exception e) {
      throw new HoodieException(
          "Error requesting the instant time from the coordinator for checkpoint " + checkpointId, e);
    }
    throw new HoodieException("Timeout waiting for the instant time from the coordinator for checkpoint " + checkpointId);
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

    /**
     * The instant time, or null while the instant is still being created.
     */
    private final String instant;

    public static InstantTimeResponse getInstance(String instant) {
      return new InstantTimeResponse(instant);
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
