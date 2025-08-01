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

package org.apache.hudi.common.engine;

import org.apache.hudi.avro.HoodieAvroReaderContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;

import org.apache.avro.generic.IndexedRecord;

/**
 * Factory that provides the {@link IndexedRecord} based {@link HoodieReaderContext} for reading data into the avro format.
 */
public class AvroReaderContextFactory implements ReaderContextFactory<IndexedRecord> {
  private final HoodieTableMetaClient metaClient;
  private final String payloadClassName;

  public AvroReaderContextFactory(HoodieTableMetaClient metaClient) {
    this(metaClient, metaClient.getTableConfig().getPayloadClass());
  }

  public AvroReaderContextFactory(HoodieTableMetaClient metaClient, String payloadClassName) {
    this.metaClient = metaClient;
    this.payloadClassName = payloadClassName;
  }

  @Override
  public HoodieReaderContext<IndexedRecord> getContext() {
    return new HoodieAvroReaderContext(metaClient.getStorageConf(), metaClient.getTableConfig(), Option.empty(), Option.empty());
  }
}
