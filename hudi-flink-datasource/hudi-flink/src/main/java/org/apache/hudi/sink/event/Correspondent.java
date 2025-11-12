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

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.util.SerializedValue;

/**
 * Correspondent between a write task with the coordinator.
 */
public class Correspondent {
  private final OperatorID operatorID;
  private final TaskOperatorEventGateway gateway;

  private Correspondent(OperatorID operatorID, TaskOperatorEventGateway gateway) {
    this.operatorID = operatorID;
    this.gateway = gateway;
  }

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
   * Sends a request to the coordinator to fetch the instant time.
   */
  public String requestInstantTime(long checkpointId) {
    try {
      InstantTimeResponse response = CoordinationResponseSerDe.unwrap(this.gateway.sendRequestToCoordinator(this.operatorID,
          new SerializedValue<>(InstantTimeRequest.getInstance(checkpointId))).get());
      return response.getInstant();
    } catch (Exception e) {
      throw new HoodieException("Error requesting the instant time from the coordinator", e);
    }
  }

  public OperatorID getOperatorID() {
    return operatorID;
  }

  public TaskOperatorEventGateway getGateway() {
    return gateway;
  }

  /**
   * A request for instant time with a given checkpoint id.
   */
  public static class InstantTimeRequest implements CoordinationRequest {
    private final long checkpointId;

    private InstantTimeRequest(long checkpointId) {
      this.checkpointId = checkpointId;
    }

    public static InstantTimeRequest getInstance(long checkpointId) {
      return new InstantTimeRequest(checkpointId);
    }

    public long getCheckpointId() {
      return checkpointId;
    }
  }

  /**
   * A response with instant time.
   */
  public static class InstantTimeResponse implements CoordinationResponse {
    private final String instant;

    private InstantTimeResponse(String instant) {
      this.instant = instant;
    }

    public static InstantTimeResponse getInstance(String instant) {
      return new InstantTimeResponse(instant);
    }

    public String getInstant() {
      return instant;
    }
  }
}
