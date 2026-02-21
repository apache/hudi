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

package org.apache.hudi.utilities.sources.helpers;

import org.apache.hudi.utilities.config.KinesisSourceConfig;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Serializable;

/**
 * Serializable configuration for Kinesis reads, used in Spark closures to avoid
 * capturing non-serializable KinesisOffsetGen.
 */
@AllArgsConstructor
@Getter
public class KinesisReadConfig implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String streamName;
  private final String region;
  private final String endpointUrl; // null if not set
  private final String accessKey; // null if not set
  private final String secretKey; // null if not set
  private final KinesisSourceConfig.KinesisStartingPosition startingPosition;
  private final boolean shouldAddOffsets;
  private final boolean enableDeaggregation;
  private final int maxRecordsPerRequest;
  private final long intervalMs;
  private final long maxRecordsPerShard;
}
