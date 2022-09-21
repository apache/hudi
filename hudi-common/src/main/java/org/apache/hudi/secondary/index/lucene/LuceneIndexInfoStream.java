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

package org.apache.hudi.secondary.index.lucene;

import org.apache.hudi.common.config.HoodieBuildTaskConfig;

import org.apache.lucene.util.InfoStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class LuceneIndexInfoStream extends InfoStream {
  private static final Logger LOG = LoggerFactory.getLogger(LuceneIndexInfoStream.class);

  private final String uniqueKey;
  private final boolean logEnable;

  public LuceneIndexInfoStream(HoodieBuildTaskConfig secondaryIndexConfig, String uniqueKey) {
    this.uniqueKey = uniqueKey;
    this.logEnable = secondaryIndexConfig.isLuceneIndexLogEnabled();
  }

  @Override
  public void message(final String component, final String message) {
    LOG.info("Lucene index info, uniqueKey:{}, component:{}, message:{}",
        uniqueKey, component, message);
  }

  @Override
  public boolean isEnabled(final String component) {
    return logEnable;
  }

  @Override
  public void close() throws IOException {

  }
}
