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

package org.apache.hudi.sink.bootstrap.batch;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.sink.bootstrap.BootstrapOperator;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.HashSet;
import java.util.Set;

/**
 * The operator to load index from existing hoodieTable.
 *
 * <p>This function should only be used for bounded source.
 *
 * <p>When a record comes in, the function firstly checks whether the partition path of the record is already loaded,
 * if the partition is not loaded yet, loads the entire partition and sends the index records to downstream operators
 * before it sends the input record; if the partition is loaded already, sends the input record directly.
 *
 * <p>The input records should shuffle by the partition path to avoid repeated loading.
 */
public class BatchBootstrapOperator<I, O extends HoodieRecord<?>>
    extends BootstrapOperator<I, O> {

  private Set<String> partitionPathSet;
  private boolean haveSuccessfulCommits;

  public BatchBootstrapOperator(Configuration conf) {
    super(conf);
  }

  @Override
  public void open() throws Exception {
    super.open();
    this.partitionPathSet = new HashSet<>();
    this.haveSuccessfulCommits = StreamerUtil.haveSuccessfulCommits(hoodieTable.getMetaClient());
  }

  @Override
  protected void preLoadIndexRecords() {
    // no operation
  }

  @Override
  @SuppressWarnings("unchecked")
  public void processElement(StreamRecord<I> element) throws Exception {
    final HoodieRecord<?> record = (HoodieRecord<?>) element.getValue();
    final String partitionPath = record.getKey().getPartitionPath();

    if (haveSuccessfulCommits && !partitionPathSet.contains(partitionPath)) {
      loadRecords(partitionPath);
      partitionPathSet.add(partitionPath);
    }

    // send the trigger record
    output.collect((StreamRecord<O>) element);
  }

  @Override
  protected boolean shouldLoadFile(String fileId, int maxParallelism, int parallelism, int taskID) {
    // load all the file groups in the partition
    return true;
  }
}
