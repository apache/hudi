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

package org.apache.hudi.hadoop.hive;

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.apache.hudi.hadoop.hive.HoodieCombineHiveInputFormat.CombineHiveInputSplit;

/**
 * Represents a CombineHiveInputSplit for realtime tables.
 */
public class HoodieCombineRealtimeHiveSplit extends CombineHiveInputSplit {

  public HoodieCombineRealtimeHiveSplit() throws IOException {
    super(ShimLoader.getHadoopShims().getCombineFileInputFormat().getInputSplitShim());
  }

  public HoodieCombineRealtimeHiveSplit(JobConf jobConf, CombineFileSplit
      combineFileSplit, Map<Path, PartitionDesc> map)
      throws IOException {
    super(jobConf, combineFileSplit, map);
  }
}
