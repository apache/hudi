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

package org.apache.hudi.client;

import java.io.IOException;
import java.io.Serializable;

/**
 * Client will run one round of clustering.
 */
public abstract class BaseClusterer<T, I, K, O> implements Serializable {

  private static final long serialVersionUID = 1L;

  protected transient BaseHoodieWriteClient<T, I, K, O> clusteringClient;

  public BaseClusterer(BaseHoodieWriteClient<T, I, K, O> clusteringClient) {
    this.clusteringClient = clusteringClient;
  }

  /**
   * Run clustering for the instant.
   * @param instantTime
   * @throws IOException
   */
  public abstract void cluster(String instantTime) throws IOException;

  /**
   * Update the write client used by async clustering.
   * @param writeClient
   */
  public void updateWriteClient(BaseHoodieWriteClient<T, I, K, O> writeClient) {
    this.clusteringClient = writeClient;
  }
}
