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

package org.apache.hudi.utilities.config;

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;

import javax.annotation.concurrent.Immutable;

/**
 * S3 Source Configs
 */
@Immutable
@ConfigClassProperty(name = "S3 Source Configs",
    groupName = ConfigGroups.Names.HUDI_STREAMER,
    subGroupName = ConfigGroups.SubGroupNames.DELTA_STREAMER_SOURCE,
    description = "Configurations controlling the behavior of S3 source in Deltastreamer.")
public class S3SourceConfig extends HoodieConfig {

  private static final String S3_SOURCE_PREFIX = "hoodie.deltastreamer.s3.source.";

  public static final ConfigProperty<String> S3_SOURCE_QUEUE_URL = ConfigProperty
      .key(S3_SOURCE_PREFIX + "queue.url")
      .noDefaultValue()
      .withDocumentation("Queue url for cloud object events");

  public static final ConfigProperty<String> S3_SOURCE_QUEUE_REGION = ConfigProperty
      .key(S3_SOURCE_PREFIX + "queue.region")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("Case-sensitive region name of the cloud provider for the queue. For example, \"us-east-1\".");

  public static final ConfigProperty<String> S3_SOURCE_QUEUE_FS = ConfigProperty
      .key(S3_SOURCE_PREFIX + "queue.fs")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("File system corresponding to queue. For example, for AWS SQS it is s3/s3a.");

  public static final ConfigProperty<String> S3_QUEUE_LONG_POLL_WAIT = ConfigProperty
      .key(S3_SOURCE_PREFIX + "queue.long.poll.wait")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("Long poll wait time in seconds, If set as 0 then client will fetch on short poll basis.");

  public static final ConfigProperty<String> S3_SOURCE_QUEUE_MAX_MESSAGES_PER_BATCH = ConfigProperty
      .key(S3_SOURCE_PREFIX + "queue.max.messages.per.batch")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("Max messages for each batch of Hudi Streamer run. Source will process these maximum number of message at a time.");

  public static final ConfigProperty<String> S3_SOURCE_QUEUE_MAX_MESSAGES_PER_REQUEST = ConfigProperty
      .key(S3_SOURCE_PREFIX + "queue.max.messages.per.request")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("Max messages for each request");

  public static final ConfigProperty<String> S3_SOURCE_QUEUE_VISIBILITY_TIMEOUT = ConfigProperty
      .key(S3_SOURCE_PREFIX + "queue.visibility.timeout")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("Visibility timeout for messages in queue. After we consume the message, queue will move the consumed "
          + "messages to in-flight state, these messages can't be consumed again by source for this timeout period.");
}
