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

package org.apache.hudi.utilities.sources.helpers.gcs;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;

/**
 * Whether a message should be processed or not, and an optional description about the message.
 */
public class MessageValidity {

  private final ProcessingDecision processingDecision;
  private final Option<String> description;

  public static final MessageValidity DEFAULT_VALID_MESSAGE = new MessageValidity(ProcessingDecision.DO_PROCESS,
          "Valid message");

  MessageValidity(ProcessingDecision processingDecision, String description) {
    this.processingDecision = processingDecision;
    this.description = StringUtils.isNullOrEmpty(description) ? Option.empty() : Option.of(description);
  }

  public ProcessingDecision getDecision() {
    return processingDecision;
  }

  public Option<String> getDescription() {
    return description;
  }

  /**
   * A decision whether to process the message or not
   * */
  public enum ProcessingDecision {
    DO_PROCESS,
    DO_SKIP
  }
}
