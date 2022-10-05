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

import java.util.List;

/**
 * A batch of messages fetched from Google Cloud Pubsub within the metadata fetcher of
 * Incremental GCS ingestion module.
 */
public class MessageBatch {
  private final List<String> messages;

  public MessageBatch(List<String> messages) {
    this.messages = messages;
  }

  public List<String> getMessages() {
    return messages;
  }

  public boolean isEmpty() {
    return messages.isEmpty();
  }
}
