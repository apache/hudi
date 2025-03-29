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

package org.apache.hudi.table.marker;

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.IOType;

import java.io.IOException;
import java.util.Set;

/**
 * Provides APIs to handle {@link IOType#APPEND} markers only present in table version 6 and below.
 */
public interface AppendMarkerHandler {
  /**
   * Fetches markers for log files w/ Append IOType ysed only for table version 6.
   *
   * @param context     {@code HoodieEngineContext} instance
   * @param parallelism parallelism for reading the marker files in the directory
   * @return all the log file paths of write IO type "APPEND"
   * @throws IOException
   */
  Set<String> getAppendedLogPaths(HoodieEngineContext context, int parallelism) throws IOException;
}
