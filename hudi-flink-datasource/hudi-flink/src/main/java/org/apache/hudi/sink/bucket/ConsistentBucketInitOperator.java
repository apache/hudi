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

package org.apache.hudi.sink.bucket;

import org.apache.hudi.common.model.HoodieRecord;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Functions for initializing consistent hashing metadata. It should be single parallelism operator,
 * to ensure no race condition in creating the metadata.
 */
public class ConsistentBucketInitOperator
    extends ProcessOperator<HoodieRecord, HoodieRecord> {

  public static class InitFunction extends ProcessFunction<HoodieRecord, HoodieRecord> {

    /**
     * Gateway to send operator events to the operator coordinator.
     */
    protected transient OperatorEventGateway eventGateway;

    private Map<String, Boolean> partitionsSeen;

    @Override
    public void open(Configuration parameters) throws Exception {
      partitionsSeen = Collections.synchronizedMap(new LinkedHashMap<String, Boolean>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry eldest) {
          return size() > 65536;
        }
      });
    }

    public void setOperatorEventGateway(OperatorEventGateway operatorEventGateway) {
      this.eventGateway = operatorEventGateway;
    }

    @Override
    public void processElement(HoodieRecord value, ProcessFunction.Context ctx, Collector out) throws Exception {
      String p = value.getPartitionPath();

      if (!partitionsSeen.containsKey(p)) {
        OperatorEvent event = (OperatorEvent) new ConsistentBucketInitCoordinator.ConsistentBucketInitializationEvent(p);
        this.eventGateway.sendEventToCoordinator(event);
      }

      out.collect(value);
    }
  }

  public ConsistentBucketInitOperator() {
    super(new InitFunction());
  }

  public void setOperatorEventGateway(OperatorEventGateway operatorEventGateway) {
    ((InitFunction) getUserFunction()).setOperatorEventGateway(operatorEventGateway);
  }
}
