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

package org.apache.hudi.sink.checkpoint;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.sink.muttley.AthenaIngestionGateway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Athena-specific implementation of {@link CheckpointService}.
 *
 * <p>This implementation uses Athena Ingestion Gateway (via Muttley RPC) to fetch
 * checkpoint information. This is an internal Uber-specific implementation and serves
 * as an example for custom checkpoint service implementations.
 *
 * <p><b>Note:</b> This implementation requires access to internal Uber services
 * and will not work in standard Apache Hudi deployments. It is included here as a
 * reference implementation for enterprises that want to integrate their own checkpoint
 * tracking systems.
 */
public class AthenaCheckpointService implements CheckpointService {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(AthenaCheckpointService.class);

  private final AthenaIngestionGateway athenaGateway;

  /**
   * Constructor with default timeout.
   *
   * @param rpcCaller Name of the calling service for RPC headers
   * @param rpcCallee Name of the Athena service to call
   */
  public AthenaCheckpointService(String rpcCaller, String rpcCallee) {
    this.athenaGateway = new AthenaIngestionGateway(rpcCaller, rpcCallee);
  }

  /**
   * Constructor that accepts a custom AthenaIngestionGateway (useful for testing).
   *
   * @param athenaGateway The AthenaIngestionGateway instance to use
   */
  public AthenaCheckpointService(AthenaIngestionGateway athenaGateway) {
    this.athenaGateway = athenaGateway;
  }

  @Override
  public Option<CheckpointInfo> getCheckpointInfo(CheckpointRequest request) throws IOException {
    LOG.info("AthenaCheckpointService called with request: {}", request);
    LOG.debug("Request details - DC: {}, Env: {}, CheckpointId: {}, JobName: {}, HadoopUser: {}, "
            + "SourceCluster: {}, TargetCluster: {}, CheckpointLookback: {}, TopicOperatorIds: {}, "
            + "ServiceTier: {}, ServiceName: {}",
        request.getDc(), request.getEnv(), request.getCheckpointId(), request.getJobName(),
        request.getHadoopUser(), request.getSourceCluster(), request.getTargetCluster(),
        request.getCheckpointLookback(), request.getTopicOperatorIds(), request.getServiceTier(),
        request.getServiceName());

    try {
      LOG.info("Calling athenaGateway.getKafkaCheckpointsInfo...");
      Option<AthenaIngestionGateway.CheckpointKafkaOffsetInfo> athenaResult =
          athenaGateway.getKafkaCheckpointsInfo(
              request.getDc(),
              request.getEnv(),
              request.getCheckpointId(),
              request.getJobName(),
              request.getHadoopUser(),
              request.getSourceCluster(),
              request.getTargetCluster(),
              request.getCheckpointLookback(),
              request.getTopicOperatorIds(),
              request.getServiceTier(),
              request.getServiceName()
          );

      LOG.info("Call to athenaGateway completed. Result present: {}", athenaResult.isPresent());

      if (athenaResult.isPresent()) {
        // Convert Athena-specific response to generic CheckpointInfo
        AthenaIngestionGateway.CheckpointKafkaOffsetInfo athenaInfo = athenaResult.get();
        Map<String, String> kafkaOffsets = new HashMap<>();

        // Convert the structured Athena format to a simple map
        if (athenaInfo.getKafkaOffsetsInfo() != null) {
          for (AthenaIngestionGateway.CheckpointKafkaOffsetInfo.KafkaOffsetsInfo offsetInfo :
              athenaInfo.getKafkaOffsetsInfo()) {
            if (offsetInfo.getOffsets() != null && offsetInfo.getOffsets().getOffsets() != null) {
              offsetInfo.getOffsets().getOffsets().forEach((partition, offset) -> {
                String key = String.format("%s:%s:%d",
                    offsetInfo.getTopicName(), offsetInfo.getClusterName(), partition);
                kafkaOffsets.put(key, String.valueOf(offset));
              });
            }
          }
        }

        long checkpointId = athenaInfo.getCheckpointId() != null
            ? Long.parseLong(athenaInfo.getCheckpointId())
            : request.getCheckpointId();

        CheckpointInfo checkpointInfo = new CheckpointInfo(kafkaOffsets, checkpointId);
        LOG.info("Successfully converted Athena response to CheckpointInfo: {}", checkpointInfo);
        return Option.of(checkpointInfo);
      }

      return Option.empty();
    } catch (IOException e) {
      LOG.error("IOException in getCheckpointInfo: {}", e.getMessage(), e);
      throw e;
    } catch (Exception e) {
      LOG.error("Unexpected exception in getCheckpointInfo: {}", e.getMessage(), e);
      throw new IOException("Unexpected error getting checkpoint info", e);
    }
  }
}
