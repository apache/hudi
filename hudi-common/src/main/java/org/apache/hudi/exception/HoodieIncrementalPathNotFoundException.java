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

package org.apache.hudi.exception;

/**
 * Thrown when path is not found in incremental query.
 * see [HUDI-2711] for more details on the issue
 */
public class HoodieIncrementalPathNotFoundException extends HoodieException {

  public HoodieIncrementalPathNotFoundException(Throwable e) {
    super("Path not found during incremental query. It is likely that the underlying file has been "
        + "moved or deleted by the cleaner. Consider setting "
        + "hoodie.datasource.read.incr.fallback.fulltablescan.enable to true.", e);
  }
}
