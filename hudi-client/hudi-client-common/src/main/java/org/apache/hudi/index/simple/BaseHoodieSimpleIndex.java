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

package org.apache.hudi.index.simple;

import org.apache.hudi.common.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.BaseHoodieTable;

/**
 * A simple index which reads interested fields(record key and partition path) from base files and
 * joins with incoming records to find the tagged location.
 *
 * @param <T>
 */
public abstract class BaseHoodieSimpleIndex<T extends HoodieRecordPayload, I, K, O, P> extends HoodieIndex<T, I, K, O, P> {
  protected BaseHoodieSimpleIndex(HoodieWriteConfig config) {
    super(config);
  }

  @Override
  public O updateLocation(O writeStatusRDD, HoodieEngineContext context, BaseHoodieTable<T, I, K, O, P> hoodieTable) throws HoodieIndexException {
    return writeStatusRDD;
  }

  @Override
  public boolean rollbackCommit(String commitTime) {
    return true;
  }

  @Override
  public boolean isGlobal() {
    return false;
  }

  @Override
  public boolean canIndexLogFiles() {
    return false;
  }

  @Override
  public boolean isImplicitWithStorage() {
    return true;
  }

}
