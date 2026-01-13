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

package org.apache.hudi.sink.partitioner.index;

import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.utils.RuntimeContextUtils;
import org.apache.hudi.utils.StateTtlConfigUtils;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;

import java.util.stream.StreamSupport;

/**
 * Factory to create an {@link IndexBackend} based on the configured index type.
 */
public class IndexDelegatorFactory {
  public static IndexBackend create(Configuration conf, FunctionInitializationContext context, RuntimeContext runtimeContext) throws Exception {
    HoodieIndex.IndexType indexType = OptionsResolver.getIndexType(conf);
    switch (indexType) {
      case FLINK_STATE:
        ValueStateDescriptor<HoodieRecordGlobalLocation> indexStateDesc =
            new ValueStateDescriptor<>(
                "indexState",
                TypeInformation.of(HoodieRecordGlobalLocation.class));
        double ttl = conf.get(FlinkOptions.INDEX_STATE_TTL) * 24 * 60 * 60 * 1000;
        if (ttl > 0) {
          indexStateDesc.enableTimeToLive(StateTtlConfigUtils.createTtlConfig((long) ttl));
        }
        ValueState<HoodieRecordGlobalLocation> indexState = context.getKeyedStateStore().getState(indexStateDesc);
        ValidationUtils.checkArgument(indexState != null, "indexState should not be null when using FLINK_STATE index!");
        return new FlinkStateIndexBackend(indexState);
      case GLOBAL_RECORD_LEVEL_INDEX:
        ListState<JobID> jobIdState = context.getOperatorStateStore().getListState(
            new ListStateDescriptor<>(
                "bucket-assign-job-id-state",
                TypeInformation.of(JobID.class)
            ));
        long initCheckpointId = -1;
        if (context.isRestored()) {
          int attemptId = RuntimeContextUtils.getAttemptNumber(runtimeContext);
          initCheckpointId = initCheckpointId(attemptId, jobIdState, context.getRestoredCheckpointId().orElse(-1L), runtimeContext);
        }
        // set the jobId state with current job id.
        jobIdState.clear();
        jobIdState.add(RuntimeContextUtils.getJobId(runtimeContext));
        return new RecordLevelIndexBackend(conf, initCheckpointId);
      default:
        throw new UnsupportedOperationException("Index type " + indexType + " is not supported for bucket assigning yet.");
    }
  }

  private static long initCheckpointId(int attemptId, ListState<JobID> jobIdState, long restoredCheckpointId, RuntimeContext runtimeContext) throws Exception {
    if (attemptId <= 0) {
      // returns early if the job/task is initially started.
      return -1;
    }
    JobID currentJobId = RuntimeContextUtils.getJobId(runtimeContext);
    if (StreamSupport.stream(jobIdState.get().spliterator(), false)
        .noneMatch(currentJobId::equals)) {
      // do not set up the checkpoint id if the state comes from the old job.
      return -1;
    }
    // sets up the known checkpoint id as the last successful checkpoint id for purposes of cache cleaning.
    return restoredCheckpointId;
  }
}
