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

import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.hudi.client.WriteStatus;

import java.util.*;

/**
 * An operator event to init or restore coordinator.
 */
public class InitWriterEvent implements OperatorEvent {
    private static final long serialVersionUID = 1L;

    private final List<WriteStatus> writeStatuses;
    private final String instant;
    private final int taskID;

    private InitWriterEvent(
            int taskID,
            String instant,
            List<WriteStatus> writeStatuses) {
        this.taskID = taskID;
        this.instant = instant;
        this.writeStatuses = new ArrayList<>(writeStatuses);
    }

    /**
     * Returns the builder for {@link InitWriterEvent}.
     */
    public static Builder builder() {
        return new Builder();
    }

    public List<WriteStatus> getWriteStatuses() {
        return writeStatuses;
    }

    public String getInstant() {
        return instant;
    }

    public int getTaskID() {
        return taskID;
    }

    // -------------------------------------------------------------------------
    //  Builder
    // -------------------------------------------------------------------------

    /**
     * Builder for {@link InitWriterEvent}.
     */
    public static class Builder {
        private List<WriteStatus> writeStatuses;
        private String instant;
        private Integer taskID;

        public InitWriterEvent build() {
            Objects.requireNonNull(taskID);
            Objects.requireNonNull(instant);
            Objects.requireNonNull(writeStatuses);
            return new InitWriterEvent(taskID, instant, writeStatuses);
        }

        public Builder taskID(int taskID) {
            this.taskID = taskID;
            return this;
        }

        public Builder instant(String instant) {
            this.instant = instant;
            return this;
        }

        public Builder writeStatus(List<WriteStatus> writeStatuses) {
            this.writeStatuses = writeStatuses;
            return this;
        }
    }
}
