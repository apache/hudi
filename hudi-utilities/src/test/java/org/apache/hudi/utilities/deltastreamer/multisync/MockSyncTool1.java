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

package org.apache.hudi.utilities.deltastreamer.multisync;

import org.apache.hudi.sync.common.HoodieSyncTool;

import org.apache.hadoop.conf.Configuration;

import java.util.Properties;

public class MockSyncTool1 extends HoodieSyncTool {

  public static boolean syncSuccess;

  public MockSyncTool1(Properties props, Configuration hadoopConf) {
    super(props, hadoopConf);
  }

  @Override
  public void syncHoodieTable() {
    syncSuccess = true;
  }
}
