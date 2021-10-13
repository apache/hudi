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

package org.apache.hudi.sink.partitioner.profile;

import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.action.commit.SmallFile;

import java.util.Collections;
import java.util.List;

/**
 * WriteProfile that always return empty small files.
 *
 * <p>This write profile is used for INSERT OVERWRITE and INSERT OVERWRITE TABLE operations,
 * the existing small files are ignored because of the 'OVERWRITE' semantics.
 *
 * <p>Note: assumes the index can always index log files for Flink write.
 */
public class EmptyWriteProfile extends WriteProfile {
  public EmptyWriteProfile(HoodieWriteConfig config, HoodieFlinkEngineContext context) {
    super(config, context);
  }

  @Override
  protected List<SmallFile> smallFilesProfile(String partitionPath) {
    return Collections.emptyList();
  }
}
