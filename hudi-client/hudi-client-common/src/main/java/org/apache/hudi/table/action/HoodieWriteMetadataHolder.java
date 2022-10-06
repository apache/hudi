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

package org.apache.hudi.table.action;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class HoodieWriteMetadataHolder<O> extends HoodieWriteMetadata<O> {

  private HoodieWriteMetadata<O> delegate;
  private AtomicBoolean getWriteStatusesInvoked = new AtomicBoolean(false);

  public HoodieWriteMetadataHolder(HoodieWriteMetadata<O> writeMetadata) {
    this.delegate = writeMetadata;
  }

  public O getWriteStatuses() {
    //if (!getWriteStatusesInvoked.getAndSet(true)) {
    return delegate.getWriteStatuses();
    /*} else {
      throw new HoodieIOException(("Repeated calling of getWriteStatuses is not supported. "));
    }*/
  }

  public Option<HoodieCommitMetadata> getCommitMetadata() {
    return delegate.getCommitMetadata();
  }

  public Option<Duration> getFinalizeDuration() {
    return delegate.getFinalizeDuration();
  }

  public Option<Duration> getIndexUpdateDuration() {
    return delegate.getIndexUpdateDuration();
  }

  public boolean isCommitted() {
    return delegate.isCommitted();
  }

  public Option<List<HoodieWriteStat>> getWriteStats() {
    return delegate.getWriteStats();
  }

  public Option<Duration> getIndexLookupDuration() {
    return delegate.getIndexLookupDuration();
  }

  public Map<String, List<String>> getPartitionToReplaceFileIds() {
    return delegate.getPartitionToReplaceFileIds();
  }

}
