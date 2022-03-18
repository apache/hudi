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

package org.apache.hudi.metaserver.service;

import org.apache.hudi.metaserver.store.MetadataStore;
import org.apache.hudi.metaserver.util.TableUtil;
import org.apache.thrift.TException;

import java.io.Serializable;
import java.util.List;

/**
 * Handle all partition related requests.
 */
public class PartitionService implements Serializable {

  private MetadataStore store;

  public PartitionService(MetadataStore metadataStore) {
    this.store = metadataStore;
  }

  public List<String> listAllPartitions(String db, String tb) throws TException {
    long tableId = TableUtil.getTableId(db, tb, store);
    return store.getAllPartitions(tableId);
  }
}
