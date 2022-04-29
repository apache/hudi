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

package org.apache.hudi.connect.transaction;

import org.apache.hudi.connect.writers.ConnectWriter;

/**
 * Stores all the state for the current Transaction within a
 * {@link TransactionParticipant}.
 * @param <T> The type of status returned by the underlying writer.
 */
public class TransactionInfo<T> {

  private final String commitTime;
  private final ConnectWriter<T> writer;
  private long expectedKafkaOffset;
  private boolean commitInitiated;

  public TransactionInfo(String commitTime, ConnectWriter<T> writer) {
    this.commitTime = commitTime;
    this.writer = writer;
    this.expectedKafkaOffset = 0;
    this.commitInitiated = false;
  }

  public String getCommitTime() {
    return commitTime;
  }

  public ConnectWriter<T> getWriter() {
    return writer;
  }

  public long getExpectedKafkaOffset() {
    return expectedKafkaOffset;
  }

  public boolean isCommitInitiated() {
    return commitInitiated;
  }

  public void setExpectedKafkaOffset(long expectedKafkaOffset) {
    this.expectedKafkaOffset = expectedKafkaOffset;
  }

  public void commitInitiated() {
    this.commitInitiated = true;
  }
}
