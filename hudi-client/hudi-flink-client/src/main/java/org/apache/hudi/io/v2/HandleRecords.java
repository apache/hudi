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

package org.apache.hudi.io.v2;

import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;

import java.util.Collections;
import java.util.Iterator;

/**
 * {@code HandleRecords} is a holder containing records iterator for {@code HoodieDataBlock}
 * and delete records iterator for {@code HoodieDeleteBlock}.
 *
 * <p>Insert records and delete records are separated using two iterators for more efficient
 * memory utilization, for example, the data bytes in the iterator are reused based Flink managed
 * memory pool, and the RowData wrapper is also a singleton reusable object to minimize on-heap
 * memory costs, thus being more GC friendly for massive data scenarios.
 */
public class HandleRecords {
  private final Iterator<HoodieRecord> recordItr;
  private final Option<Iterator<DeleteRecord>> deleteRecordItr;

  public HandleRecords(Iterator<HoodieRecord> recordItr, Iterator<DeleteRecord> deleteItr) {
    this.recordItr = recordItr;
    this.deleteRecordItr = Option.ofNullable(deleteItr);
  }

  public Iterator<HoodieRecord> getRecordItr() {
    return this.recordItr;
  }

  public Iterator<DeleteRecord> getDeleteRecordItr() {
    return this.deleteRecordItr.orElse(Collections.emptyIterator());
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private Iterator<HoodieRecord> recordItr;
    private Iterator<DeleteRecord> deleteRecordItr;

    public Builder() {
    }

    public Builder withRecordItr(Iterator<HoodieRecord> recordItr) {
      this.recordItr = recordItr;
      return this;
    }

    public Builder withDeleteRecordItr(Iterator<DeleteRecord> deleteRecordItr) {
      this.deleteRecordItr = deleteRecordItr;
      return this;
    }

    public HandleRecords build() {
      return new HandleRecords(recordItr, deleteRecordItr);
    }
  }
}
