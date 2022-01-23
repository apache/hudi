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

package org.apache.hudi.hbase.regionserver;

import java.io.IOException;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * This interface denotes a scanner as one which can ship cells. Scan operation do many RPC requests
 * to server and fetch N rows/RPC. These are then shipped to client. At the end of every such batch
 * {@link #shipped()} will get called.
 */
@InterfaceAudience.Private
public interface Shipper {

  /**
   * Called after a batch of rows scanned and set to be returned to client. Any in between cleanup
   * can be done here.
   */
  void shipped() throws IOException;
}
