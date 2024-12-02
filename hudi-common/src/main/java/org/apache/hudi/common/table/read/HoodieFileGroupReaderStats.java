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

package org.apache.hudi.common.table.read;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class HoodieFileGroupReaderStats {
  private long numInserts = 0L;
  private long numUpdates = 0L;
  private long numDeletes = 0L;

  public HoodieFileGroupReaderStats() {
  }

  public HoodieFileGroupReaderStats(long numInserts, long numUpdates, long numDeletes) {
    this.numInserts = numInserts;
    this.numUpdates = numUpdates;
    this.numDeletes = numDeletes;
  }

  public long getNumInserts() {
    return numInserts;
  }

  public long getNumUpdates() {
    return numUpdates;
  }

  public long getNumDeletes() {
    return numDeletes;
  }

  public void incrementNumInserts() {
    numInserts++;
  }

  public void incrementNumUpdates() {
    numUpdates++;
  }

  public void incrementNumDeletes() {
    numDeletes++;
  }
}
