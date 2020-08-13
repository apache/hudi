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

package org.apache.hudi.internal;

import java.util.ArrayList;
import java.util.List;
import org.apache.hudi.client.HoodieInternalWriteStatus;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;

/**
 * Hoodie's {@link WriterCommitMessage} used in datasource implementation.
 */
public class HoodieWriterCommitMessage implements WriterCommitMessage {

  private List<HoodieInternalWriteStatus> writeStatuses = new ArrayList<>();

  public HoodieWriterCommitMessage(List<HoodieInternalWriteStatus> writeStatuses) {
    this.writeStatuses = writeStatuses;
  }

  public List<HoodieInternalWriteStatus> getWriteStatuses() {
    return writeStatuses;
  }

  @Override
  public String toString() {
    return "HoodieWriterCommitMessage{" + "writeStatuses=" + writeStatuses + '}';
  }
}
