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

/**
 * Composite handler for async clean services of the data table and metadata table.
 */
public class CompositeCleanHandler extends CompositeTableServiceHandler<CleanHandler> implements CleanHandler {

  CompositeCleanHandler(CleanHandler dataTableHandler, CleanHandler metadataTableHandler) {
    super(dataTableHandler, metadataTableHandler);
  }

  @Override
  public void clean() {
    forEachHandler(CleanHandler::clean);
  }

  @Override
  public void waitForCleaningFinish() {
    forEachHandler(CleanHandler::waitForCleaningFinish);
  }

  @Override
  public void startAsyncCleaning() {
    forEachHandler(CleanHandler::startAsyncCleaning);
  }

  @Override
  public void close() {
    forEachHandler(CleanHandler::close);
  }
}
