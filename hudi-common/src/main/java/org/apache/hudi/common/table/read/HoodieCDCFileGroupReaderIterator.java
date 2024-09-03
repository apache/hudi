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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.storage.HoodieStorage;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class HoodieCDCFileGroupReaderIterator<T> implements ClosableIterator<T> {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieCDCFileGroupReaderIterator.class);

  public HoodieCDCFileGroupReaderIterator(HoodieReaderContext<T> readerContext,
                                          HoodieStorage storage,
                                          TypedProperties props,
                                          String tablePath,
                                          String latestCommitTime,
                                          List<FileSlice> fileSlices,
                                          Schema dataSchema,
                                          Schema requestedSchema,
                                          Option<InternalSchema> internalSchemaOpt,
                                          HoodieTableMetaClient hoodieTableMetaClient,
                                          boolean shouldUseRecordPosition) {

  }

  @Override
  public boolean hasNext() {
    return false;
  }

  @Override
  public T next() {
    return null;
  }

  public void close() {
  }
}
