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

package org.apache.hudi.execution;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.queue.BoundedInMemoryQueueConsumer;
import org.apache.hudi.io.HoodieWriteHandle;

import java.util.ArrayList;
import java.util.List;

/**
 * Consumes stream of hoodie records from in-memory queue and writes to one explicit create handle.
 */
public class ExplicitWriteHandler<T>
    extends BoundedInMemoryQueueConsumer<HoodieLazyInsertIterable.HoodieInsertValueGenResult<HoodieRecord>, List<WriteStatus>> {

  private final List<WriteStatus> statuses = new ArrayList<>();

  private HoodieWriteHandle handle;

  public ExplicitWriteHandler(HoodieWriteHandle handle) {
    this.handle = handle;
  }

  @Override
  public void consumeOneRecord(HoodieLazyInsertIterable.HoodieInsertValueGenResult<HoodieRecord> genResult) {
    final HoodieRecord insertPayload = genResult.getResult();
    handle.write(insertPayload, genResult.schema, new TypedProperties(genResult.props));
  }

  @Override
  public void finish() {
    closeOpenHandle();
    assert statuses.size() > 0;
  }

  @Override
  public List<WriteStatus> getResult() {
    return statuses;
  }

  private void closeOpenHandle() {
    statuses.addAll(handle.close());
  }
}

