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

package org.apache.hudi.sink.compact;

import org.apache.hudi.client.WriteStatus;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;

/**
 * Represents a commit event from the compaction task {@link CompactOperator}.
 */
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class CompactionCommitEvent implements Serializable {
  private static final long serialVersionUID = 1L;

  /**
   * The compaction commit instant time.
   */
  private String instant;

  /**
   * The file ID.
   */
  private String fileId;

  /**
   * The write statuses.
   */
  private List<WriteStatus> writeStatuses;
  /**
   * The compaction task identifier.
   */
  private int taskID;

  /**
   * An event with NULL write statuses that represents a failed compaction.
   */
  public CompactionCommitEvent(String instant, String fileId, int taskID) {
    this(instant, fileId, null, taskID);
  }

  public boolean isFailed() {
    return this.writeStatuses == null;
  }
}
