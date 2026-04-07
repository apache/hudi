/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.compact.handler;

import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.configuration.Configuration;

/**
 * Composite handler for async clean services of the data table and metadata table.
 */
public class CompositeCleanHandler extends CompositeTableServiceHandler<CleanHandler> {

  CompositeCleanHandler(Option<CleanHandler> dataTableHandler, Option<CleanHandler> metadataTableHandler) {
    super(dataTableHandler, metadataTableHandler);
  }

  public static CompositeCleanHandler create(Configuration conf, HoodieFlinkWriteClient writeClient) {
    if (!conf.get(FlinkOptions.CLEAN_ASYNC_ENABLED)) {
      return new CompositeCleanHandler(Option.empty(), Option.empty());
    }
    return new CompositeCleanHandler(
        Option.of(new CleanHandler(writeClient)),
        OptionsResolver.isStreamingIndexWriteEnabled(conf)
            ? Option.of(new CleanHandler(StreamerUtil.createMetadataWriteClient(writeClient)))
            : Option.empty());
  }

  public void clean() {
    forEachHandler(CleanHandler::clean);
  }

  public void waitForCleaningFinish() {
    forEachHandler(CleanHandler::waitForCleaningFinish);
  }

  public void startAsyncCleaning() {
    forEachHandler(CleanHandler::startAsyncCleaning);
  }

  @Override
  public void close() {
    forEachHandler(CleanHandler::close);
  }
}
