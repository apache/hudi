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

package org.apache.hudi.index.hbase;

import java.io.Serializable;

/**
 * <code>HBaseIndexQPSResourceAllocator</code> defines methods to manage resource allocation for HBase index operations.
 */
public interface HBaseIndexQPSResourceAllocator extends Serializable {

  /**
   * This method returns the QPS Fraction value that needs to be acquired such that the respective HBase index operation
   * can be completed in desiredPutsTime.
   *
   * @param numPuts Number of inserts to be written to HBase index
   * @param desiredPutsTimeInSecs Total expected time for the HBase inserts operation
   * @return QPS fraction that needs to be acquired.
   */
  float calculateQPSFractionForPutsTime(final long numPuts, final int desiredPutsTimeInSecs);

  /**
   * This method acquires the requested QPS Fraction against HBase cluster for index operation.
   *
   * @param desiredQPSFraction QPS fraction that needs to be requested and acquired
   * @param numPuts Number of inserts to be written to HBase index
   * @return value of the acquired QPS Fraction.
   */
  float acquireQPSResources(final float desiredQPSFraction, final long numPuts);

  /**
   * This method releases the acquired QPS Fraction.
   */
  void releaseQPSResources();
}
